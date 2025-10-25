library(tidyverse)
library(purrr)

## FROM JEAN-OLIVIER IRISSON, see https://github.com/jiho/castr/blob/master/R/slide.R
#' Apply a function in a sliding window along a vector
#'
#' Allows to compute a moving average, moving median, or even moving standard deviation, etc. in a generic way.
#'
#' @param x input numeric vector.
#' @param k order of the window; the window size is 2k+1.
#' @param fun function to apply in the moving window.
#' @param n number of times to pass the function over the data.
#' @param ... arguments passed to `fun`. A usual one is `na.rm=TRUE` to avoid getting `NA`s at the extremities of `x`.
#'
#' @details A window of size `2k+1` is centred on element `i` of `x`. All elements from index `i-k` to index `i+k` are sent to function `fun`. The returned value is associated with index `i` in the result. The window is moved to element `i+1` and so on.
#'
#' For such sliding window computation to make sense, the data must be recorded on a regular coordinate (i.e. at regular intervals). Otherwise, data points that are far from each other may end up in the same window.
#'
#' The extremeties of the input vector are padded with `NA` to be able to center the sliding window from the first to the last elements. This means that, to avoid getting `k` missing values at the beginning and at the end of the result, `na.rm=TRUE` should be passed to `fun`.
#'
#' @return The data passed through `fun`, `n` times.
#' @export
#'
#' @seealso [cweights()] to compute weights centered in the middle of the window.
#'
#' @examples
#' # create some data and add random noise
#' xs <- sin(seq(0, 4*pi, length=100))
#' x <- xs + rnorm(length(xs), sd=0.25)
#' plot(x)
#' lines(xs)
#' # filter the data in various ways
#' # moving average
#' mav   <- slide(x, 3, mean, na.rm=TRUE)
#' # running moving average
#' rmav  <- slide(x, 3, mean, na.rm=TRUE, n=4)
#' # weighted moving average
#' wmav  <- slide(x, 3, weighted.mean, na.rm=TRUE, w=cweights(3))
#' # weighted running moving average
#' wrmav <- slide(x, 3, weighted.mean, na.rm=TRUE, w=cweights(3), n=4)
#' # moving median
#' mmed  <- slide(x, 3, median, na.rm=TRUE)
#' lines(mav, col="red")
#' lines(rmav, col="red", lty="dashed")
#' lines(wmav, col="orange")
#' lines(wrmav, col="orange", lty="dashed")
#' lines(mmed, col="blue")
#' # inspect variability around filtered data
#' plot(slide(x-rmav, 7, sd))
#' plot(slide(x-mmed, 7, mad))
slide <- function(x, k, fun, n=1, ...) {
  # make sure to get a function as the `fun` argument (i.e. avoid name masking)
  if (!is.function(fun)) {
    fun <- get(as.character(substitute(fun)), mode="function")
  }

  if (n>=1) {
    # repeat n times
    for (t in 1:n) {
      # pad the extremities of data to be able to compute over the whole vector
      x <- c(rep(NA, times=k), x, rep(NA, times=k))

      # apply the rolling function (and remove padding at the extremities)
      x <- sapply((k+1):(length(x)-k), function(i) {
        fun(x[(i-k):(i+k)], ...)
      })
    }
  }

  return(x)
}

# extract transmissometer data
extract_cp_data <- function(wmo, path_to_data){

  ncfile <- paste0(path_to_data, wmo ,'/', wmo, '_Rtraj.nc')

  # open netcdf
  nc_data <- ncdf4::nc_open(ncfile) # Rtraj files should ALWAYS exist for this application so I am not checking if it is...

  # extract data from NetCDF
  value <- try(ncdf4::ncvar_get(nc_data, 'CP660'))
  depth <- ncdf4::ncvar_get(nc_data, 'PRES') 
  mc <- ncdf4::ncvar_get(nc_data, 'MEASUREMENT_CODE')
  juld <- ncdf4::ncvar_get(nc_data, 'JULD')
  juld <- oce::argoJuldToTime(juld)
  cycle <- ncdf4::ncvar_get(nc_data, 'CYCLE_NUMBER')
  # extract wmo (and remove leading and trailing whitespaces)
  wmo <- stringr::str_trim(ncdf4::ncvar_get(nc_data, 'PLATFORM_NUMBER'))

  # check state of parameter
  if(class(value)[1] == 'try-error'){ # parameter does not exist, return empty tibble
    return(tibble::tibble(juld = NA, depth = NA, mc = NA, value = NA, qc = NA))
  }else{
    # check for adjusted values for that parameter
    value_adjusted <- try(ncdf4::ncvar_get(nc_data, 'CP660_ADJUSTED'))
    if(class(value_adjusted)[1] == 'try-error'){ # ADJUSTED field does not exist in NetCDF file for now
      qc <- ncdf4::ncvar_get(nc_data, 'CP660_QC')
      qc <- as.numeric(unlist(strsplit(qc,split="")))
    }else if(all(is.na(value_adjusted)) == TRUE){ # if TRUE, there are no adjusted values
      qc <- ncdf4::ncvar_get(nc_data, 'CP660_QC')
      qc <- as.numeric(unlist(strsplit(qc,split="")))
    }else{ # there are adjusted values
      qc <- ncdf4::ncvar_get(nc_data, 'CP660_ADJUSTED_QC')
      qc <- as.numeric(unlist(strsplit(qc,split="")))
    }
  }

  # make final tibble
  tib <- tibble::tibble(wmo = wmo, cycle = cycle, juld = juld, mc = mc, depth = depth, cp = value, qc = qc)
  # clean data
  tib <- tib |>
    dplyr::filter(cycle >= 1, mc == 290) |>
    tidyr::drop_na(cp) |>
    dplyr::mutate(park_depth = dplyr::if_else(depth < 350, 200, dplyr::if_else(depth > 750, 1000, 500))) |>
    dplyr::select(-mc)

  # close nc file
  ncdf4::nc_close(nc_data)

  return(tib)
}

# derive optical sediment trap flux
derive_ost_flux <- function(data, wmo_float){

  # make sure that the cp signal is in chronological order
  tmp <- data |>
    dplyr::filter(wmo == wmo_float) |>
    dplyr::arrange(juld)

  # despike cp data with a 7-point moving window
  tmp$cp <- oce::despike(tmp$cp, k = 3)

  # smooth cp data with a 3-point moving median, n time(s)
  tmp$cp <- slide(tmp$cp, fun = median, k = 3, n = 1, na.rm=T)

  # compute slope between two adjacent points (except first point) # we could start after 1h to let the float stabilize
  delta_x <- as.numeric(tmp$juld - dplyr::lag(tmp$juld), units = 'days')
  delta_y <- tmp$cp - dplyr::lag(tmp$cp)
  tmp$slope <- delta_y/delta_x

  # compute a Z score (assuming a normal distribution of the slopes) on the slopes
  tmp <- tmp |>
     dplyr::mutate(zscore = (slope - mean(slope, na.rm = T))/sd(slope, na.rm = T))

  # spot outliers on the Z score signal
  # interquartile range between Q25 and Q75 -> had to used that and not the despike function because slopes are often close (or equal) to 0 so it can miss clear jumps. Q25 and Q75 are more trustworthy in this case than the despike function of Jean-Olivier (see package castr on his github: https://github.com/jiho/castr)
  IQR <- quantile(tmp$zscore, probs = 0.75, na.rm=T) - quantile(tmp$zscore, probs = 0.25, na.rm=T)
  # outliers ('spikes' in the Z score signal)
  spikes_down <- tmp$zscore < quantile(tmp$zscore, 0.25, na.rm=T) - 1.5 *IQR
  spikes_up <- tmp$zscore > quantile(tmp$zscore, 0.75, na.rm=T) + 1.5 *IQR
  spikes <- as.logical(spikes_down + spikes_up)

  # assign spikes
  tmp$spikes <- spikes

  # assign colour code to cp signal
  tmp$colour <- 'base signal' # base signal = smoothed despiked cp signal
  tmp[which(tmp$spikes == TRUE),]$colour <- 'jump'

  # add group to compute the slope of each group of points, separated by a jump
  tmp$group <- NA

  # index of jumps in the array
  jump_index <- which(tmp$colour == 'jump')

  # assign group identity to each group of points, separated by a jump (= subgroup)
  for (i in jump_index){
    for (j in 1:nrow(tmp)){
      if ((j < i) & (is.na(tmp$group[j]))){
        tmp$group[j] <- paste0('group_',i)
      }
    }
  }
  tmp$group[which(is.na(tmp$group))] <- 'last_group'

  # compute slope for each subgroup
  slope_df <- tmp |>
    dplyr::filter(colour == 'base signal', slope != 'NA') %>%
    dplyr::group_by(group) %>%
    dplyr::summarise(min_time = min(juld),
                     max_time = max(juld),
                     nb_points = dplyr::n(),
                     first_cp = cp[1],
                     last_cp = cp[nb_points],
                     delta_x = as.numeric(difftime(max_time, min_time, units = 'days')),
                     delta_y = (last_cp-first_cp)*0.25, slope = delta_y/delta_x) # *0.25 to convert cp to ATN

  # remove negative slope from the mean slope (no physical meaning)
  slope_df <- slope_df |>
    dplyr::filter(slope > 0)

  # remove if only one point (cannot fit a slope with one point) -> switched to 3 points
  slope_df <- slope_df |>
    dplyr::filter(nb_points > 3)

  # compute weighted average slope (to take into account the fact that some subgroups might have 2 points and a high slope vs. large group of points with a small slope)
  mean_slope <- sum(slope_df$nb_points * slope_df$slope)/sum(slope_df$nb_points)

  # convert cp to POC using Estapa's relationship
  poc_flux <- 633*(mean_slope**0.77) # /!\ slope computed for ATN on y axis (delta_y *0.25 because ATN = cp*0.25) -> should be OK

  # build dataframe to plot each subgroup
  part1 <- slope_df |> dplyr::select(group, time = min_time, cp = first_cp)
  part2 <- slope_df |> dplyr::select(group, time = max_time, cp = last_cp)
  part_slope <- rbind(part1, part2)

  # spot negative jump
  tmp$colour[which((tmp$colour == 'jump') & (tmp$slope < 0))]  <- 'negative jump'

  # add large particles flux to the party
  rows_to_keep <- c(jump_index, jump_index-1)
  tmp2 <- tmp[rows_to_keep,] |> 
    dplyr::select(juld, cp, slope, colour, group) |>
    dplyr::arrange(juld)

  # remove negative jumps, if any
  check_colour <- unique(tmp2$colour)
  if(length(check_colour) >= 2){ # there is a least a jump (positive or negative)
    tmp2 <- tmp2 |>
      dplyr::mutate(diff_jump = cp - dplyr::lag(cp))
    even_indexes <- seq(2,nrow(tmp2),2)
    tmp2 <- tmp2[even_indexes,]
  }else{ # No jump
    tmp2 <- NULL
  }

  if(is.null(tmp2)){ # no jump
    large_part_poc_flux <- 0
    tmp3 <- NULL
  }else{
    tmp3 <- tmp2 |>
       dplyr::filter(diff_jump > 0)
    if(nrow(tmp3) == 0){ # no positive jumps
      large_part_poc_flux <- 0
    }else{
      delta_y <- sum(tmp3$diff_jump) *0.25 # to get ATN (= cp*0.25)
      max_time <- max(tmp$juld)
      min_time <- min(tmp$juld)
      delta_x <- as.numeric(difftime(max_time, min_time, units = 'days'))
      slope_large_part <- delta_y/delta_x
      large_part_poc_flux <- 633*(slope_large_part**0.77)
    }
  }

  # compute total drifting time
  max_time <- max(tmp$juld)
  min_time <- min(tmp$juld)
  drifting_time <- as.numeric(difftime(max_time, min_time, units = 'days'))

  # to plot subgroups
  part_slope_tmp <- part_slope |>
   dplyr::mutate(juld = time, colour = 'slope')

  # adapt script to return large and small flux
  df <- tibble::tibble('max_time' = max_time, 'min_time' = min_time, 'small_flux' = poc_flux, 'large_flux' = large_part_poc_flux, park_depth = data$park_depth[1], wmo = data$wmo[1],
               cycle = data$cycle[1])

  return(df)
}

# extract optical sediment trap data
extract_ost_data <- function(wmo_float, path_to_data){

  # parking depths (so far we only have those 3 but that might change in the future)
  park_depth <- c(200, 500, 1000)

  # extract cp data from the float
  data <- extract_cp_data(wmo_float, path_to_data)

  res <- data.frame()
  max_cycle <- max(data$cycle)
  for (i in park_depth){
    for (j in seq(1:max_cycle)){
      tmp <- data |> dplyr::filter(park_depth == i, cycle == j)
      if(nrow(tmp) == 0){ # no data for this cycle or at this parking depth
        next
      }else if(nrow(tmp) < 3){ # case where there is not enough data
        next
      }else{
        output <- derive_ost_flux(tmp, wmo_float)
        res <- rbind(res, output)
      }
    }
  }
  return(res)
}

# Function to remove outliers based on IQR
remove_outliers <- function(x) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = TRUE)
  H <- 1.5 * IQR(x, na.rm = TRUE)
  x[x < (qnt[1] - H) | x > (qnt[2] + H)] <- NA
  return(x)
}

# extract data
path_to_data <- './docs/data/argo_core_trajectory_files/'
WMO <- c(1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028)
tmp <- purrr::map_dfr(WMO, extract_ost_data, path_to_data = path_to_data)

# a bit of cleaning
tmp <- tmp |>
  mutate(max_time = lubridate::date(max_time)) |>
  mutate(max_time = lubridate::as_datetime(max_time)) |>
  select(-min_time) |>
  rowwise() |>
  mutate(total_flux = small_flux + large_flux) |>
  group_by(wmo, park_depth) |>
  mutate(total_flux = remove_outliers(total_flux)) |>
  filter(!is.na(total_flux)) |>
  dplyr::mutate(zone = dplyr::case_when(
    wmo %in% c(6904240, 6904241, 1902578, 4903634) ~ 'Labrador Sea',
    wmo %in% c(4903660, 6990514) ~ 'Arabian Sea',
    wmo %in% c(3902498, 1902601) ~ 'Guinea Dome',
    wmo %in% c(1902637, 4903740, 4903739) ~ 'Apero mission',
    wmo %in% c(2903787, 4903657) ~ 'West Kerguelen',
    wmo %in% c(1902593, 4903658) ~ 'East Kerguelen',
    wmo %in% c(5906970, 3902473, 6990503, 3902471) ~ 'Tropical Indian Ocean',
    wmo %in% c(2903783) ~ 'South Pacific Gyre',
    wmo %in% c(6903093, 6903094) ~'California Current',
    wmo %in% c(7901028, 2903794) ~ 'Nordic Seas',
    wmo %in% c(1902685) ~ 'North Pacific Gyre',
    .default = NA
  )) 


#cat(readr::format_csv(tmp))
temp_file <- tempfile(fileext = ".parquet")
arrow::write_parquet(tmp, sink = temp_file)

system2('/bin/cat', args = temp_file)
