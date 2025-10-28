library(ncdf4)
library(tidyverse)
#library(readr)
library(oce)
library(lubridate)
library(purrr)
library(arrow)

extract_LPM <- function(ncfile){

    # open NetCDF
    nc_data <- try(ncdf4::nc_open(ncfile))

    # extract pressure field
    pres <- ncdf4::ncvar_get(nc_data, 'PRES')
    # extract measurement code
    mc <- ncdf4::ncvar_get(nc_data, 'MEASUREMENT_CODE')
    # extract time
    juld <- ncdf4::ncvar_get(nc_data, 'JULD')
    # extract cycle number
    cycle <- ncdf4::ncvar_get(nc_data, 'CYCLE_NUMBER')
    # extract wmo (and remove leading and trailing whitespaces)
    wmo <- stringr::str_trim(ncdf4::ncvar_get(nc_data, 'PLATFORM_NUMBER'))

    # extract particle size spectra
    part_spectra <- try(ncdf4::ncvar_get(nc_data, 'NB_SIZE_SPECTRA_PARTICLES'))

    # transpose part spectra matrix
    part_spectra <- tibble::as_tibble(t(part_spectra))
    # lpm classes
    lpm_classes <- c('NP_Size_50.8','NP_Size_64','NP_Size_80.6', 'NP_Size_102','NP_Size_128','NP_Size_161','NP_Size_203',
                       'NP_Size_256','NP_Size_323','NP_Size_406','NP_Size_512','NP_Size_645','NP_Size_813','NP_Size_1020','NP_Size_1290',
                       'NP_Size_1630','NP_Size_2050','NP_Size_2580')
    # rename columns
    colnames(part_spectra) <- lpm_classes

    # extract number of images
    image_number <- ncdf4::ncvar_get(nc_data, 'NB_IMAGE_PARTICLES')

    # divide particle concentrations by number of images
    part_spectra <- part_spectra %>% dplyr::mutate(dplyr::across(NP_Size_50.8:NP_Size_2580, ~.x/(0.7*image_number))) # 0.7 = UVP6 image volume

    # add depth/juld/cycle to part_spectra
    part_spectra$depth <- pres
    part_spectra$mc <- mc
    part_spectra$cycle <- cycle
    part_spectra$juld <- juld
    part_spectra$wmo <- as.character(wmo)

    # convert julian day to human time
    part_spectra <- part_spectra %>% dplyr::mutate(juld = oce::argoJuldToTime(juld))

    # drop NA in particle size spectra
    part_spectra <- part_spectra %>% tidyr::drop_na(NP_Size_50.8)

    # keep data when the float is parked
    part_spectra <- part_spectra %>%
        dplyr::filter(mc == 590) %>% # if == 290, it means we are dealing with parking data
        dplyr::select(-mc)

      # reorder tibble
      part_spectra <- part_spectra %>% dplyr::select(depth, cycle, juld, dplyr::everything())

      # remove some weird numbers associated to BAD QCs (but all QCs are at 0 so not JULD_QC is not useful here)
      part_spectra <- part_spectra %>% dplyr::filter(juld > '2021-01-01', juld < '2025-01-01')

      # format data
      part_spectra$juld <- format(part_spectra$juld, "%Y-%m-%d")

      # close NetCDF
      ncdf4::nc_close(nc_data)

      return(part_spectra)
}

# extract data
WMO <- c(1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028)
# NOTE: 6904094 does not have Traj file and 6904093 does have the same var names than the others....
#WMO <- c(1902685, 6904240, 7901028)
tmp <- map_dfr(WMO, ~extract_LPM(paste0("/home/fricour/test/argo_trajectory_files/", .x, "/", .x, "_Rtraj_aux.nc"))) |>
  bind_rows()

# some cleaning
#tmp$juld <- format(tmp$juld, "%Y-%m-%d")
tmp <- tmp %>%
    mutate(across(where(~ is.array(.x)), ~ as.double(unlist(.)))) |>
    pivot_longer(cols = starts_with("NP_Size_"), names_to = "size", values_to = "concentration") |>
    mutate(size = as.numeric(str_split_i(size, '_', 3))) |>
    mutate(juld = as_datetime(juld)) # datetime object needed because Plot doesn’t parse dates; convert your strings to Date instances with d3.utcParse or d3.autoType, or by passing typed: true to Observable’s FileAttachment function.
    # check info here: https://observablehq.com/@ee2dev/analyzing-time-series-data-with-plot

# bin data
tmp <- tmp %>%
  # Create depth bins
  mutate(depth_bin = cut(depth, 
                         breaks = seq(0, 
                                      max(depth, na.rm = TRUE) + 10, 
                                      by = 10),
                         labels = FALSE)) %>%
  # Group by the relevant variables
  group_by(cycle, wmo, size, depth_bin) %>%
  # Calculate median concentration
  summarise(
    concentration = median(concentration, na.rm = TRUE),
    depth = mean(depth, na.rm = TRUE),
    juld = unique(juld),
    .groups = 'drop'
  )

#tmp <- tmp |>
#        group_by(wmo, size, cycle) |>
#        mutate(concentration = remove_outliers(concentration)) |>
#        filter(!is.na(concentration)) 


# example here: https://github.com/observablehq/data-loader-examples/blob/main/docs/data/penguin-kmeans.csv.R
#cat(format_csv(tmp))

# based on https://github.com/observablehq/framework/issues/915 and https://github.com/observablehq/framework/issues/873
# Write the data frame to a temporary Parquet file
temp_file <- tempfile(fileext = ".parquet")
arrow::write_parquet(tmp, sink = temp_file)

system2('/bin/cat', args = temp_file)
