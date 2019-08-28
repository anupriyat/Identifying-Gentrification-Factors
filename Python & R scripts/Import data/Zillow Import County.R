### Loading County files into Dataframes
County <- "/Users/anupriyat/Desktop/Gentrification Project/County/"
file_list_County <- list.files(path=County, pattern="*.csv")

install.packages("RPostgreSQL")
library('RPostgreSQL')
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user= "postgres", password="SelectALL2017", host="104.197.103.221", port="5432",
                 dbname="postgres")

for (i in 1:length(file_list_County)){
  assign(file_list_County[i], 
         read.csv(paste(County, file_list_County[i], sep=''))
  )}

zip <- "/Users/anupriyat/Desktop/Gentrification Project/zip/"
file_list_zip <- list.files(path=zip, pattern="*.csv")

for (i in 1:length(file_list_zip)){
  assign(file_list_zip[i], 
         read.csv(paste(zip, file_list_zip[i], sep=''))
  )}