library(ncdf4)
library(tidyverse)
library(oce)
library(lubridate)
library(purrr)
library(arrow)

# extract particle data at parking depth
extract_LPM <- function(ncfile){

  # open NetCDF
  nc_data <- ncdf4::nc_open(ncfile)

  # extract depth (pres), measurement code (mc), julian day, cycle number and particle size spectra
  pres <- ncdf4::ncvar_get(nc_data, 'PRES')
  mc <- ncdf4::ncvar_get(nc_data, 'MEASUREMENT_CODE')
  juld <- ncdf4::ncvar_get(nc_data, 'JULD')
  cycle <- ncdf4::ncvar_get(nc_data, 'CYCLE_NUMBER')
  wmo <- stringr::str_trim(ncdf4::ncvar_get(nc_data, 'PLATFORM_NUMBER'))
  part_spectra <- ncdf4::ncvar_get(nc_data, 'NB_SIZE_SPECTRA_PARTICLES')

  # transpose part spectra matrix
  part_spectra <- tibble::as_tibble(t(part_spectra))

  # particle class sizes
  lpm_classes <- c('NP_Size_50.8','NP_Size_64','NP_Size_80.6', 'NP_Size_102','NP_Size_128','NP_Size_161','NP_Size_203',
                       'NP_Size_256','NP_Size_323','NP_Size_406','NP_Size_512','NP_Size_645','NP_Size_813','NP_Size_1020','NP_Size_1290',
                       'NP_Size_1630','NP_Size_2050','NP_Size_2580')
  # rename columns for clarity
  colnames(part_spectra) <- lpm_classes

  # extract number of images
  image_number <- ncdf4::ncvar_get(nc_data, 'NB_IMAGE_PARTICLES')

  # finalize dataframe
  part_spectra <- part_spectra |> # # divide particle concentrations by number of images, 0.7L = UVP6 image volume
    dplyr::mutate(dplyr::across(NP_Size_50.8:NP_Size_2580, ~.x/(0.7*image_number))) |> # add data extracted from NetCDF
    dplyr::mutate(depth = pres, mc = mc, cycle = cycle, juld = juld, wmo = as.character(wmo)) |> # convert julian day to human time
    dplyr::mutate(juld = oce::argoJuldToTime(juld)) |> # drop NA in particle size spectra
    tidyr::drop_na(NP_Size_50.8) |> # only keep data when the float is parked
    dplyr::filter(mc == 290) |> # remove measurement code (not needed anymore)
    dplyr::select(-mc) |> # define "standard" parking depths (200 m, 500 m and 1000 m)
    dplyr::mutate(park_depth = dplyr::if_else(depth < 350, 200, dplyr::if_else(depth > 750, 1000, 500))) |> # reorder tibble
    dplyr::select(depth, park_depth, cycle, juld, dplyr::everything()) |> # remove some weird numbers associated to BAD QCs (but all QCs are at 0 so not JULD_QC is not useful here)
    dplyr::filter(juld > '2021-01-01', juld < '2025-01-01') |> # format time data
    dplyr::mutate(juld = format(juld, "%Y-%m-%d"))

    # close NetCDF
    ncdf4::nc_close(nc_data)

    return(part_spectra)
}

# remove outliers (could be skipped if we could zoom or filtered easily the domain plot or observable plot)
# Function to remove outliers based on IQR
remove_outliers <- function(x) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = TRUE)
  H <- 1.5 * IQR(x, na.rm = TRUE)
  x[x < (qnt[1] - H) | x > (qnt[2] + H)] <- NA
  return(x)
}

# extract particle data for each float
WMO <- c(1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028)
# NOTE: 6904094 does not have Traj file and 6904093 does have the same var names than the others....
tmp <- map_dfr(WMO, ~extract_LPM(paste0("./docs/data/argo_trajectory_files/", .x, "/", .x, "_Rtraj_aux.nc"))) |>
  bind_rows()

# some additional cleaning 
tmp <- tmp |>
    mutate(across(where(~ is.array(.x)), ~ as.double(unlist(.)))) |> # needed to work with Observable
    pivot_longer(cols = starts_with("NP_Size_"), names_to = "size", values_to = "concentration") |> # format tibble
    mutate(size = as.numeric(str_split_i(size, '_', 3))) |> # keep only the size in the column name
    mutate(juld = as_datetime(juld)) # datetime object needed because Observable Plot doesn’t parse dates; convert your strings to Date instances with d3.utcParse or d3.autoType, or by passing typed: true to Observable’s FileAttachment function.
    # check info here: https://observablehq.com/@ee2dev/analyzing-time-series-data-with-plot

# remove outliers and add oceanic zones 
tmp <- tmp |>
          group_by(wmo, size, cycle, park_depth) |>
          mutate(concentration = remove_outliers(concentration)) |>
          filter(!is.na(concentration)) |>
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


# example here: https://github.com/observablehq/data-loader-examples/blob/main/docs/data/penguin-kmeans.csv.R
#cat(format_csv(tmp))

# based on https://github.com/observablehq/framework/issues/915 and https://github.com/observablehq/framework/issues/873
# Write the data frame to a temporary Parquet file
temp_file <- tempfile(fileext = ".parquet")
arrow::write_parquet(tmp, sink = temp_file)

system2('/bin/cat', args = temp_file)
