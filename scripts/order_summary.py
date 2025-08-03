import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, countDistinct, min as _min, max as _max, col
from dotenv import load_dotenv

load_dotenv()
findspark.init()

DB2_HOSTNAME = os.getenv('DB2_HOSTNAME')
DB2_UID = os.getenv('DB2_UID')
DB2_PWD = os.getenv('DB2_PWD')
DB2_DATABASE = os.getenv('DB2_DATABASE')
DB2_PORT = os.getenv('DB2_PORT')
DB2_SCHEMA = os.getenv('DB2_SCHEMA')

JDBC_DRIVER_PATH = os.getenv('JDBC_DRIVER_PATH')
SOURCE_DATA_DIR = os.getenv('SOURCE_DATA_DIR')

def get_spark_session():
    abs_jdbc_path = os.path.abspath(JDBC_DRIVER_PATH)
    if not os.path.exists(abs_jdbc_path):
        print(f"ERROR: JDBC driver not found at {abs_jdbc_path}.")
        print("Please ensure 'db2jcc4.jar' is in your 'drivers' folder and JDBC_DRIVER_PATH in .env is correct.")
        exit(1)

    print("Initializing Spark Session for Order Summary Generation...")
    spark = SparkSession.builder \
        .appName("Order Summary Generation") \
        .config("spark.jars", abs_jdbc_path) \
        .config("spark.driver.extraClassPath", abs_jdbc_path) \
        .getOrCreate()
    print("Spark Session created successfully.")
    return spark

def get_jdbc_properties():
    if not all([DB2_UID, DB2_PWD, DB2_SCHEMA]):
        print("ERROR: Db2 user, password, or schema missing from .env.")
        exit(1)
    return {
        "user": DB2_UID,
        "password": DB2_PWD,
        "driver": "com.ibm.db2.jcc.DB2Driver",
        "sslConnection": "true",
        "currentSchema": DB2_SCHEMA,
        "batchsize": "200"
    }

def get_jdbc_url():
    
    if not all([DB2_HOSTNAME, DB2_PORT, DB2_DATABASE]):
        print("ERROR: Db2 hostname, port, or database name missing from .env.")
        exit(1)
    return f"jdbc:db2://{DB2_HOSTNAME}:{DB2_PORT}/{DB2_DATABASE}"

def process_and_upload_order_summary(spark_session, file_limits=None):
    
    print("\n--- Processing and Uploading ORDER_SUMMARY to DB2 ---")

    required_csv_details = {
        "order_items_df": {"file_name": "cleaned_order_items.csv"},
        "products_df": {"file_name": "cleaned_products.csv"},
        "inventory_df": {"file_name": "cleaned_inventory_items.csv"}
    }

    loaded_dfs = {}
    missing_file_found = False

    try:
       
        for alias, details in required_csv_details.items():
            file_name = details["file_name"]
            file_path = os.path.join(SOURCE_DATA_DIR, file_name)

            if not os.path.exists(file_path):
                print(f"ERROR: Required file for Order Summary not found: {file_path}. Skipping this operation.")
                missing_file_found = True
                break

            print(f"Reading {file_name} as '{alias}'...")
            temp_df = spark_session.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(file_path)

            limit = file_limits.get(file_name) if file_limits else None
            if limit is not None:
                print(f"Limiting {file_name} to first {limit} rows.")
                temp_df = temp_df.limit(limit)
            
            loaded_dfs[alias] = temp_df.alias(alias) 

        if missing_file_found:
            print("ERROR: Missing required input files. Aborting ORDER_SUMMARY generation.")
            return

        order_items_df = loaded_dfs.get("order_items_df")
        products_df = loaded_dfs.get("products_df")
        inventory_df = loaded_dfs.get("inventory_df")

        if not all([order_items_df, products_df, inventory_df]):
            print("ERROR: Not all required DataFrames could be loaded. Aborting ORDER_SUMMARY ETL.")
            return

        joined_df = order_items_df.join(
            inventory_df,
            order_items_df["inventory_item_id"] == inventory_df["id"],
            "left"
        )

        joined_df = joined_df.join(
            products_df,
            col("inventory_df.product_id") == products_df["id"],
        )
       
        order_summary_df = joined_df.groupBy(order_items_df["order_id"]).agg(
            count("*").alias("total_items"), 
            _sum(order_items_df["sale_price"]).alias("total_amount"), 
            countDistinct(col("inventory_df.product_id")).alias("unique_products"), 
            _min(order_items_df["created_at"]).alias("first_order_date"), 
            _max(order_items_df["created_at"]).alias("last_order_date") 
        ).orderBy("order_id")

        print("Schema for ORDER_SUMMARY:")
        order_summary_df.printSchema()
        print("First 5 rows of ORDER_SUMMARY:")
        order_summary_df.show(5)

        print(f"Attempting to write ORDER_SUMMARY with JDBC batchsize: {get_jdbc_properties()['batchsize']}")
        order_summary_df.write.format("jdbc") \
            .option("url", get_jdbc_url()) \
            .option("dbtable", f"{DB2_SCHEMA}.ORDER_SUMMARY") \
            .options(**get_jdbc_properties()) \
            .mode("overwrite") \
            .save()
        print(f"Successfully uploaded ORDER_SUMMARY to {DB2_SCHEMA}.ORDER_SUMMARY")

    except Exception as e:
        print(f"ERROR: Failed to process and upload ORDER_SUMMARY: {e}")
        print("Please check data integrity of source CSVs, column names, and Db2 configuration.")
        print("TIP: If running into 200MB limit, reduce input row counts using `file_limits`.")

if __name__ == "__main__":
    spark = None
    try:
        spark = get_spark_session()

        order_summary_file_limits = {
            "cleaned_order_items.csv": 50000, 
            "cleaned_products.csv": None,     
            "cleaned_inventory_items.csv": 20000,
           
        }

        process_and_upload_order_summary(spark, file_limits=order_summary_file_limits)
    except Exception as e:
        print(f"An unexpected error occurred during the Order Summary analysis and upload process: {e}")
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")

