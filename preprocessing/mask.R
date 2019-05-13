#install.packages("RPostgreSQL")
#install.packages("yaml")
#install.packages("anonymizer")
#install.packages("digest")

library(RPostgreSQL)
library(yaml)
library(anonymizer)
library(digest)

# Read in yaml config
config <- yaml.load_file("config.yaml")

# Connect to database
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = config$db$database,
                 host = config$db$host, 
                 port = 5432, user = config$db$username, 
                 password = config$db$password)

# Test connection
dbExistsTable(con, c("raw", "inspections_complete"))

# Load column from database
df <- dbGetQuery(con, 
                 "SET ROLE direccion_trabajo_inspections_write; SELECT DISTINCT rutempresa, comuna_entity FROM cleaned.inspections_datadump;")

# Mask column
df$rutmask <- anonymize(.x=df$rut, .algo="sha256", .seed = 0, .n_chars=8L)

# Write original + masked columns to temp table
dbWriteTable(con, c("staging", "masking"), 
             value = df,
             append = FALSE,
             row.names = FALSE,
             overwrite = TRUE)

# Merge masked column onto original table
# and drop temp table
# dbSendQuery(con, 
#            "SET ROLE direccion_trabajo_inspections_write; DROP TABLE IF EXISTS staging.masks; 
#            CREATE TABLE staging.temp AS (SELECT * FROM cleaned.empresas FULL JOIN staging.masking USING rut); DROP TABLE IF EXISTS cleaned.empresas; CREATE TABLE cleaned.empresas AS SELECT * FROM staging.temp; DROP TABLE IF EXISTS staging.temp;")

# Close connection
dbDisconnect(con)
dbUnloadDriver(drv)
