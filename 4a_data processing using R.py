library(readr) library(tibble) library(data.table)

IP_DATA_ALL <- read_csv("D:/Dinesh/DS/4/IP_DATA_ALL.csv") View(IP_DATA_ALL)
spec(IP_DATA_ALL)
IP_DATA_ALL_FIX <- set_tidy_names(IP_DATA_ALL, syntactic = TRUE, quiet = TRUE) View(IP_DATA_ALL_FIX)
sapply(IP_DATA_ALL_FIX, typeof)


hist_country <- data.table(Country = unique(IP_DATA_ALL_FIX[is.na(IP_DATA_ALL_FIX['Country']) == 0, ]$Country)) setorder(hist_country, 'Country')

hist_country_with_id <- rowid_to_column(hist_country, var = "RowIDCountry") View(hist_country_with_id)

IP_DATA_COUNTRY_FREQ <- data.table(with(IP_DATA_ALL_FIX, table(Country))) View(IP_DATA_COUNTRY_FREQ)

hist_latitude <- data.table(Latitude = unique(IP_DATA_ALL_FIX[is.na(IP_DATA_ALL_FIX['Latitude']) == 0, ]$Latitude)) setkeyv(hist_latitude, 'Latitude')
setorder(hist_latitude)
hist_latitude_with_id <- rowid_to_column(hist_latitude, var = "RowID")
 
View(hist_latitude_with_id)


IP_DATA_Latitude_FREQ <- data.table(with(IP_DATA_ALL_FIX, table(Latitude))) View(IP_DATA_Latitude_FREQ)

min_latitude <- sapply(IP_DATA_ALL_FIX[,'Latitude'], min, na.rm = TRUE) min_country <- sapply(IP_DATA_ALL_FIX[,'Country'], min, na.rm = TRUE) max_latitude <- sapply(IP_DATA_ALL_FIX[,'Latitude'], max, na.rm = TRUE) max_country <- sapply(IP_DATA_ALL_FIX[,'Country'], max, na.rm = TRUE) mean_latitude <- sapply(IP_DATA_ALL_FIX[,'Latitude'], mean, na.rm = TRUE) median_latitude <- sapply(IP_DATA_ALL_FIX[,'Latitude'], median, na.rm = TRUE) range_latitude <- sapply(IP_DATA_ALL_FIX[,'Latitude'], range, na.rm = TRUE) quantile_latitude <- sapply(IP_DATA_ALL_FIX[,'Latitude'], quantile, na.rm = TRUE) sd_latitude <- sapply(IP_DATA_ALL_FIX[,'Latitude'], sd, na.rm = TRUE) sd_longitude <- sapply(IP_DATA_ALL_FIX[,'Longitude'], sd, na.rm = TRUE)

min_latitude min_country max_latitude max_country mean_latitude median_latitude range_latitude quantile_latitude sd_latitude sd_longitude
