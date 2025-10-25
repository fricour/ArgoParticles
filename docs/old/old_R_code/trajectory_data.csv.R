library(data.table)
library(lubridate)
library(tidyverse)

# read index file
index <- fread('./data/argo_bio-profile_index.txt')

# floats WMO
WMO <- c(1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028)

tmp <- index %>%
  mutate(
    wmo = as.numeric(str_extract(file, "\\d{7}")),
    cycle = as.numeric(str_extract(file, "\\d{3}(?=\\.nc$)")),
    date = ymd(substr(date_update, 1, 8))
  ) %>%
  select(wmo, cycle, latitude, longitude, date) %>%
  filter(wmo %in% WMO) |>
  drop_na()

cat(format_csv(tmp))