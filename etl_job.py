import requests
import json
import logging
import boto3
import psycopg2
from datetime import datetime
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    concat_ws,
    hour,
    month,
    year,
    dayofweek,
    when,
    to_json,
    max as spark_max,
)

from pyspark.sql import SparkSession

# Import all configuration constants from config.py
from config import (
    CONFIG_BUCKET,
    CONFIG_FILE_KEY,
    LOGS_BUCKET,
    LOGS_KEY_PREFIX,
    DATA_BUCKET,
    PROCESSED_DATA_PREFIX,
    BASE_URL,
    LIMIT,
    REDSHIFT_HOST,
    REDSHIFT_PORT,
    REDSHIFT_DB,
    REDSHIFT_USER,
    REDSHIFT_PASSWORD,
    REDSHIFT_IAM_ROLE,
    REDSHIFT_TABLE,
)

# ---------------------
# Setup Logging
# ---------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"etl_log_{timestamp}.log"
logger = logging.getLogger("nyc_crashes_etl")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(log_file)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info("Starting NYC Crashes Data ETL")

# Initialize SparkSession (assuming Spark is already available)
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def read_last_collision_date():
    """Read the last collision date from the S3 JSON configuration file using boto3."""
    s3 = boto3.client("s3")
    logger.info(
        f"Reading last_collision_date from s3://{CONFIG_BUCKET}/{CONFIG_FILE_KEY}"
    )
    obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_FILE_KEY)
    file_content = obj["Body"].read().decode("utf-8")
    config_data = json.loads(file_content)
    last_collision_date = config_data["last_collision_date"]
    logger.info(f"Last collision date from config: {last_collision_date}")
    return last_collision_date


def update_last_collision_date(new_date):
    """Update the last collision date in the S3 JSON configuration file."""
    s3 = boto3.client("s3")
    content = json.dumps({"last_collision_date": new_date})
    s3.put_object(Bucket=CONFIG_BUCKET, Key=CONFIG_FILE_KEY, Body=content)
    logger.info(f"Updated last_collision_date to {new_date} and saved to S3.")


def download_new_data(last_collision_date):
    """Download new data from Socrata API since last_collision_date, return as Spark DataFrame."""
    offset = 0
    all_data_df = None
    logger.info("Starting data download from Socrata.")

    while True:
        query_params = {
            "$where": f"crash_date > '{last_collision_date}'",
            "$limit": LIMIT,
            "$offset": offset,
        }

        response = requests.get(BASE_URL, params=query_params)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data. Status code: {response.status_code}"
            )
            break

        data = response.json()

        if not data:
            logger.info("No more new data to download.")
            break

        batch_df = spark.createDataFrame(data)

        if all_data_df is None:
            all_data_df = batch_df
        else:
            all_data_df = all_data_df.unionByName(batch_df)

        offset += LIMIT
        logger.info(
            f"Downloaded a batch of {len(data)} records, offset is now {offset}"
        )

    return all_data_df


def transform_data(data):
    data = data.dropna(subset=["collision_id"])

    # 2. Cast collision_id to integer
    data = data.withColumn("collision_id", col("collision_id").cast("int"))

    # 3. Cast relevant columns to integer
    int_cols = [
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed",
    ]
    for c in int_cols:
        if c in data.columns:
            data = data.withColumn(c, col(c).cast("int"))

    # 4. Parse and create crash_timestamp
    # Assuming crash_date is in 'yyyy-MM-dd' format and crash_time is in 'HH:mm' format
    data = data.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd"))
    data = data.withColumn(
        "crash_timestamp",
        to_timestamp(
            concat_ws(" ", col("crash_date"), col("crash_time")), "yyyy-MM-dd HH:mm"
        ),
    )

    # 5. Extract year, month, day_of_week from crash_timestamp
    data = (
        data.withColumn("year", year(col("crash_timestamp")))
        .withColumn("month", month(col("crash_timestamp")))
        .withColumn("day_of_week", dayofweek(col("crash_timestamp")))
    )

    # 6. Determine if the crash occurred on a weekend
    data = data.withColumn(
        "is_weekend",
        when((col("day_of_week") == 1) | (col("day_of_week") == 7), True).otherwise(
            False
        ),
    )

    # 7. Categorize the time of day
    data = data.withColumn("hour", hour(col("crash_timestamp")))
    data = data.withColumn(
        "time_of_day",
        when((col("hour") >= 0) & (col("hour") < 6), "Late Night")
        .when((col("hour") >= 6) & (col("hour") < 12), "Morning")
        .when((col("hour") >= 12) & (col("hour") < 17), "Afternoon")
        .when((col("hour") >= 17) & (col("hour") < 21), "Evening")
        .otherwise("Night"),
    )

    # 8. Calculate total_injuries and total_fatalities
    data = data.withColumn(
        "total_injuries",
        col("number_of_persons_injured")
        + col("number_of_pedestrians_injured")
        + col("number_of_cyclist_injured")
        + col("number_of_motorist_injured"),
    )

    data = data.withColumn(
        "total_fatalities",
        col("number_of_persons_killed")
        + col("number_of_pedestrians_killed")
        + col("number_of_cyclist_killed")
        + col("number_of_motorist_killed"),
    )

    # 9. Categorize severity
    data = data.withColumn(
        "severity_category",
        when((col("total_injuries") + col("total_fatalities")) == 0, "Minor")
        .when((col("total_injuries") + col("total_fatalities")) <= 2, "Moderate")
        .otherwise("Severe"),
    )

    # 10. Cast latitude and longitude to double
    data = data.withColumn("latitude", col("latitude").cast("double"))
    data = data.withColumn("longitude", col("longitude").cast("double"))

    # 11. Convert location MapType to JSON string
    if "location" in data.columns:
        data = data.withColumn("location", to_json(col("location")))

    # 12. Select and order the desired columns
    desired_columns = [
        "collision_id",
        "crash_timestamp",
        "crash_date",
        "crash_time",
        "year",
        "month",
        "day_of_week",
        "is_weekend",
        "time_of_day",
        "borough",
        "zip_code",
        "latitude",
        "longitude",
        "location",
        "on_street_name",
        "cross_street_name",
        "off_street_name",
        "total_injuries",
        "total_fatalities",
        "severity_category",
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed",
        "contributing_factor_vehicle_1",
        "contributing_factor_vehicle_2",
        "contributing_factor_vehicle_3",
        "contributing_factor_vehicle_4",
        "contributing_factor_vehicle_5",
        "vehicle_type_code1",
        "vehicle_type_code2",
        "vehicle_type_code_3",
        "vehicle_type_code_4",
        "vehicle_type_code_5",
    ]

    existing_columns = [c for c in desired_columns if c in data.columns]
    data = data.select(existing_columns)

    # 13. Write the transformed DataFrame to S3 in Parquet format
    # Ensure to use 's3://' prefix for Redshift compatibility
    processed_data_path = f"s3://{DATA_BUCKET}/{PROCESSED_DATA_PREFIX}/"
    data.write.mode("overwrite").format("parquet").save(processed_data_path)
    logger.info(f"Processed data saved to {processed_data_path}")
    return processed_data_path


def load_data_into_redshift(processed_data_path):
    """Run a Redshift COPY command to load processed data into the Redshift table."""
    # Construct the COPY command
    copy_command = f"""
    COPY {REDSHIFT_TABLE}
    FROM '{processed_data_path}'
    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
    FORMAT AS PARQUET;
    """

    # Connect to Redshift and run the COPY
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()
    logger.info("Running Redshift COPY command.")
    cur.execute(copy_command)
    cur.close()
    conn.close()
    logger.info("Data successfully loaded into Redshift.")


def upload_logs_to_s3():
    """Upload log file to S3."""
    logger.info("Uploading log file to S3.")
    s3_client = boto3.client("s3")
    s3_client.upload_file(log_file, LOGS_BUCKET, f"{LOGS_KEY_PREFIX}{log_file}")
    logger.info("Log file uploaded to S3.")


def main():
    try:
        # Extract
        last_collision_date = read_last_collision_date()
        all_data_df = download_new_data(last_collision_date)

        if all_data_df is None:
            logger.info(
                "No new data found since the last collision date. Nothing to process."
            )
        else:
            record_count = all_data_df.count()
            logger.info(f"Total new records downloaded: {record_count}")

            # Update the last_collision_date
            new_last_date_row = all_data_df.agg(
                spark_max(col("crash_date")).alias("max_crash_date")
            ).collect()[0]
            new_last_collision_date = new_last_date_row["max_crash_date"]
            new_last_collision_date = new_last_collision_date.split("T")[0]

            # Transform
            processed_data_path = transform_data(all_data_df)

            # Load
            load_data_into_redshift(processed_data_path)

            update_last_collision_date(new_last_collision_date)

    except Exception as e:
        logger.exception("An error occurred during the ETL process.")
    finally:
        upload_logs_to_s3()
        spark.stop()


if __name__ == "__main__":
    main()
