#import all the modules
from google.cloud import storage, bigquery #used to interact python with gcs and big query
import pandas as pd 
from pyspark.sql import SparkSession
import datetime
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("RetailerMySQLToLanding").getOrCreate()

# Google Cloud Storage (GCS) Configuration variables
GCS_BUCKET = "retailer-datalake-project-01042025" # so as to get the data or ingest the data from cloud sql to gcs
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/archive/" #for the purpose of archieving(old files are archieved)
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/retailer_config.csv" #configuration files

# BigQuery Configuration
BQ_PROJECT = "bold-sorter-450512-d9" 
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log" # we have used big query to store the audit logs
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"  

# MySQL Configuration #to read data from mysql we need to setup mysql configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.56.33.76:3306/retailerDB?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "mypass"
}

# Initialize GCS & BigQuery Clients #as we are using bigquery and storage we need to initialize it
storage_client = storage.Client()
bq_client = bigquery.Client()

# Logging Mechanism
log_entries = []  # Stores logs before writing to GCS
##---------------------------------------------------------------------------------------------------##
def log_event(event_type, message, table=None):
    """Log an event and store it in the log list"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")  # Print for visibility

##---------------------------------------------------------------------------------------------------##

# First step is read the config file
# Function to Read Config File from GCS
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "✅ Successfully read the config file")
    return df
##---------------------------------------------------------------------------------------------------##
# Function to Move Existing Files to Archive
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/retailer-db/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")] #if files end with json then archieve it
    
    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return
    
    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
        
        # Extract Date from File Name (products_27032025.json)
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        
        # Move to Archive
        archive_path = f"landing/retailer-db/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)
        
        # Copy file to archive and delete original
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()
        
        log_event("INFO", f"✅ Moved {file} to {archive_path}", table=table)    
        
##---------------------------------------------------------------------------------------------------##

# Function to Get Latest Watermark from BigQuery Audit Table
def get_latest_watermark(table_name): #this part is used to get the incremental data load
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp 
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}'
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00" #if that table is not available then get the old timestamp
    return "1900-01-01 00:00:00"
        
##---------------------------------------------------------------------------------------------------##

# Function to Extract Data from MySQL and Save to GCS
# We already now the logic i.e. if it is a incremental load table we need to get it from audit table get the timestamp and for full load 
# dont do anything as mentioned in the first block of try block using watermark 
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        # Get Latest Watermark
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)
        
        # Generate SQL Query
        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full load" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        
        # Read Data from MySQL
        df = (spark.read
                .format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())
        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)
        
        # Convert Spark DataFrame to JSON #why pandas df because we need convert into json file it will create success file and log file
        pandas_df = df.toPandas()
        json_data = pandas_df.to_json(orient="records", lines=True)
        
        # Generate File Path in GCS #after converting it into json set the gcs path
        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/retailer-db/{table}/{table}_{today}.json"
        
        # Upload JSON to GCS
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(json_data, content_type="application/json")

        log_event("SUCCESS", f"✅ JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)
        
        # Insert Audit Entry
        audit_df = spark.createDataFrame([
            (table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")], ["tablename", "load_type", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save())

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)
    
    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)


##---------------------------------------------------------------------------------------------------##

# Main Execution
config_df = read_config_file()

#Second step and according the function mentioned inside the for loop add it above
for row in config_df.collect():
    if row["is_active"] == '1': #check for the active records only and take it in archive folder
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table) #this function is used so we need to create logic for this function
        extract_and_save_to_landing(table, load_type, watermark) #now extract from mysql and load into landing
save_logs_to_gcs()
save_logs_to_bigquery()       
        
print("✅ Pipeline completed successfully!")