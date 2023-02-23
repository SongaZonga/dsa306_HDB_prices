#hdb_prod

library(sparklyr); library(dplyr)

config <- spark_config()
config$`sparklyr.shell.driver-memory` <- "6G"
config$`sparklyr.shell.executor-memory` <- "6G"
sc <- spark_connect(master = "local", config = config, version = "3.3.0")

hdb_reloaded <- ml_load(sc, path = "hdb_cv_model")

#* @post /predict
function(year, town, flat_type, storey, area_sqm, lease_start, lease_rem, pri_dist, sec_dist, mall_dist, mrt_dist) {
  new_data <- data.frame(
    year = as.numeric(year),
    town = toString(town),
    flat_type = toString(flat_type),
    storey = as.numeric(storey),
    area_sqm = as.numeric(area_sqm),
    lease_start = as.numeric(lease_start),
    lease_rem = as.numeric(lease_rem),
    resale_price = 0, 
    pri_dist = as.numeric(pri_dist),
    sec_dist = as.numeric(sec_dist),
    mall_dist = as.numeric(mall_dist),
    mrt_dist = as.numeric(mrt_dist)
  )
  new_data_r <- copy_to(sc, new_data, overwrite = TRUE) 
  ml_transform(hdb_reloaded, new_data_r) |>
    pull(prediction) |>
    exp()
}