library(tidyverse)
library(ncdf4)
library(tibble)
library(purrr)
library(lubridate)
library(oce)

extract_taxo <- function(ncfile, class_number){

    taxo_class <- c('Acantharia', 'Actinopterygii', 'Appendicularia', 'Aulacanthidae',
       'Calanoida', 'Chaetognatha', 'Collodaria', 'Creseis',
       'Foraminifera', 'Rhizaria', 'Salpida', 'artefact', 'crystal',
       'detritus', 'fiber', 'other<living', 'puff', 'small<Cnidaria',
       'solitaryglobule', 'tuff')
    
    #print(ncfile)
    #print(class_number)

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

     # extract TAXO
    concentration_objects_category <- ncvar_get(nc_data, 'CONCENTRATION_CATEGORY')
    #reshape concentration
    concentration_objects_category <- as_tibble(t(concentration_objects_category))
    index_category <- ncvar_get(nc_data, 'INDEX_CATEGORY')
    #reshape index
    index_category <- as_tibble(t(index_category))

    # find absolute position of calanoid objects in the category dataframe and make a tibble out of it
    object_position <- as_tibble(apply(index_category, c(1,2), function(x) any(x == class_number)))
    object_position <- replace(object_position, object_position == F, NA) 
    # multiply boolean position dataframe with concentration
    concentration_object <- concentration_objects_category * object_position
    # add meta data
    taxo_object <- concentration_object |> 
        cbind(depth = pres, juld = juld, cycle = cycle, wmo = wmo, mc = mc) |>
        filter(mc == 590) |> # only keep vertical data, not data taken at parking
        select(-mc) |>
        mutate(juld = argoJuldToTime(juld),
               juld = lubridate::as_date(juld),
               juld = lubridate::as_datetime(juld), # "needed" for Observable plot otherwise I have issues with the date format
               concentration_total = rowSums(across(1:40), na.rm = TRUE)) |>
        select(depth, cycle, juld, wmo, concentration_total) |>
        mutate(taxo_class = taxo_class[class_number]) |>
        filter(concentration_total > 0) # remove rows with no concentration (it's more a presence/absence tool, and it saves space)

    return(taxo_object)
}

# compute the taxo data for a list of WMOs
WMO <- c(1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028)
classes <- seq(1, 20, 1)

#tmp <- map_dfr(WMO, ~extract_taxo(paste0("/home/fricour/test/argo_trajectory_files/", .x, "/", .x, "_Rtraj_aux.nc"), class_number = 14)) |>
#  bind_rows()

#tmp <- extract_taxo("/home/fricour/test/argo_trajectory_files/6904240/6904240_Rtraj_aux.nc", class_number = 14)

tmp <- map_dfr(WMO, ~map_dfr(classes, function(class) {
  extract_taxo(
    paste0("/home/fricour/test/argo_trajectory_files/", .x, "/", .x, "_Rtraj_aux.nc"),
    class_number = class
  )
}))


# Write the data frame to a temporary Parquet file
temp_file <- tempfile(fileext = ".parquet")
arrow::write_parquet(tmp, sink = temp_file)

system2('/bin/cat', args = temp_file)


#wmo <- 6904240
#test <- map_dfr(classes, ~extract_taxo(paste0("/home/fricour/test/argo_trajectory_files/", wmo, "/", wmo, "_Rtraj_aux.nc"), class_number = .x))
