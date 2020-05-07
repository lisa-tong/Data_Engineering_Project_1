# Data Engineering Project 1
## Project Description
This project will take a large dataset of Airbnb listings in the Greater Toronto Area (GTA) and convert the file from csv to parquet to optimize the size (Phase 1). Once converted, the parquet files will be queried for condos only and integrated into a map UI using the latitudes and longitudes of each listing (Phase 2). The goal of this project is to review the number of Airbnb listings that are in condominium buildings which will help prospective home buyers to determine if it's the right choice for them.

## Project Goals
The goal of this project is to reduce the file size of the csv file through Amazon Web Services.

## Architecture
![Phase1(1)](https://user-images.githubusercontent.com/48896326/80536837-05cf4180-8971-11ea-997e-4a44b322c439.jpg)

## Tools/Languages Used
Apache Spark, SQL, Docker, Airflow, Scala, sbt using the Amazon Web Services platform.

### Loading DataFrame into Spark
Initially there were some issues reading the csv file due to how the information was populated. There were new lines in cells and arrays had commas embedded in the cell. It would cause some issues parsing the file correctly because the cells would shift unexpectedly and data would not fall under the right columns.

![wrongdatacolumns](https://user-images.githubusercontent.com/48896326/80407661-f7f7be80-8893-11ea-805c-ab2bab578f9e.jpg)


To fix this, the following Spark syntax was used:

`val data = spark.read.option("multiLine", true).option("quote", "\"").option("escape", "\"").option("header", "true").option("delimiter", ",").option("treatEmptyValuesAsNulls","true").csv(params.inPath)`

![fixeddatacolumns](https://user-images.githubusercontent.com/48896326/80407670-fd550900-8893-11ea-9582-7e6127cd8c24.jpg)

Since full use condos are the only building type considered:

`val filter = newData.where(newData("property_type") === "Condominium" && newData("room_type") === "Entire home/apt").show()`

The next step would be to run a `sbt clean assembly` to create a jar file for `spark-submit`

We want to get into the bash command in the Docker container running sbt

`docker run -it --rm -p 8080:8080 bigtruedata/sbt bash` then `sbt clean assembly` to create a jar file.

The jar file and source files (csv) can be uploaded in S3 buckets on AWS. A destination folder for the output can also be placed as well.

![s3bucket-redacted](https://user-images.githubusercontent.com/48896326/81202971-55c38f00-8f95-11ea-883d-f4b4c594d4d8.jpg)

EC2 and an EMR cluster would be initiated.

![ec2](https://user-images.githubusercontent.com/48896326/81203089-80ade300-8f95-11ea-86c4-faeea93a868f.jpg)

The EMR cluster would be pending for instruction from the lambda function. A lambda function was created to handle the event of dropping files into the source bucket. The lambda function initiates the workers in Airflow and the data would be processed through the EMR. The S3 bucket would be linked to the Lambda function.

![s3trigger](https://user-images.githubusercontent.com/48896326/81126450-4a2a8680-8f09-11ea-8764-2d2e77e5a15a.jpg)

The resulting parquet file will be housed in the destination folder. 