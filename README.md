# Udactiy Data Lake In EMR
This project focuses on building a star schema from a set of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) and log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above.
The project uses AWS and especially EMR with Spark to perform the ETL process
For storage AWS S3 is used and the format utilised is parquet
The project provides the file modelling and etl pipeline to read data from udacity's s3 buckets transform the data accordingly and store them
partitioned in s3 for analytics purposes for the fictitious company Sparkify 
## Installation
The project follows the structure as defined in the blog post at [production-grade-pyspark](https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f#.wg3iv4kie)
to allow for smooth submissions of Pyspark jobs with all the required dependencies

the process to run the project is as follows:
```
pip install -r requirements.txt -t src/libs                                                                       
```
This command will install the required libraries in the libs directory (Pyspark is not included as it is provided by EMR)
```
make build
```
This will built the dist directory copying the main.py and creating zip files for all the 3rd party libraries and jobs
The libraries can be normally imported as we use the sys.path.insert(0, 'some.zip') to include these dependencies.

The dist directory will contain:
```
  .
├── src
├── dist                  
│   ├── main.py          # runnable script provided to spark-submit
│   ├── libs.zip         # 3rd party libraries
│   └── jobs.zip         # Jobs module
└── ...
```
```
scp -i  spark-udacity.pem -r project/data-lake-with-spark/config hadoop@ec2-xx-xx-xxx-xxx.us-xx-xx.compute.amazonaws.com:~/
scp -i  spark-udacity.pem -r project/data-lake-with-spark/dist hadoop@ec2-xx-xx-xxx-xxx.us-xx-xx.compute.amazonaws.com:~/
```
We copy the dist directory and config directory to the master 
```
spark-submit --py-files jobs.zip,libs.zip main.py --job songs --input-data s3a://udacity-dend --output-data s3a://<YOUR_BUCKET>
spark-submit --py-files jobs.zip,libs.zip main.py --job logs --input-data s3a://udacity-dend --output-data s3a://<YOUR_BUCKET>
```
The jobs were split into two separate jobs that can run independently. the s3a format optimised for hadoop is used
 
## Configuration 
The config file dl.cfg is expected with the following structure
```
[AWS]
AWS_ACCESS_KEY_ID=<YOUR_AWS_KEY>
AWS_ACCESS_KEY_ID=<YOUR_AWS_SECRET>
```

## Purpose
This database that is being created helps the fictional company Sparkify create a fact and dimensions (star schema). That enables to:
* Supporting de-normalized data
* Enforcing referential integrity
* Reducing the time required to load large batches of data into a database 
* Improving query performance, i.e. queries generally run faster
* Enabling dimensional tables to be easily updated
* Enabling new ‘facts’ to be easily added i.e. regularly or selectively
* Simplicity i.e. navigating through the data is easy (analytical queries are easier)

## Database design
For the database the decision was made to create 5 tables with 1 fact table (songplays ) storing the fake events and 4 dimension tables 
* songplays
* users
* artists
* songs
* time

#### EXTRACT

For the initial load of the data we read from udacity's s3 bucket that contains the log_data and song_data paths in the udacity-dend bucket

## TRANSFORM
The songs and logs jobs accordingly transform the data to create the necessary tables, drop duplicates where needed and join tables to create 
the facts table songplays.

## LOAD 
Each of the five tables are written to parquet files in a separate analytics directory on S3. 
Each table has its own folder within the directory. 
Songs table files are partitioned by year and then artist. 
Time table files are partitioned by year and month. 
Songplays table files are partitioned by year and month.


