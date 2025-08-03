
import os
import findspark
from pyspark.sql import SparkSession
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

    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Upload Cleaned CSVs to DB2") \
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

def upload_csv_to_db2(spark_session, csv_file_name, db_table_name, limit_rows=None): 
    print(f"\n--- Processing and Uploading {csv_file_name} to {DB2_SCHEMA}.{db_table_name} ---")
    
    file_path = os.path.join(SOURCE_DATA_DIR, csv_file_name)
    
    if not os.path.exists(file_path):
        print(f"ERROR: Source CSV file not found: {file_path}. Skipping upload for {db_table_name}.")
        return

    try:
        df = spark_session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        if limit_rows is not None:
            print(f"Limiting to first {limit_rows} rows for {csv_file_name}.")
            df = df.limit(limit_rows)

        print(f"Schema for {csv_file_name}:")
        df.printSchema()
        print(f"First 5 rows of {csv_file_name} (or fewer if limited):")
        df.show(5)

        print(f"Attempting to write with JDBC batchsize: {get_jdbc_properties()['batchsize']}")
        df.write.format("jdbc") \
            .option("url", get_jdbc_url()) \
            .option("dbtable", f"{DB2_SCHEMA}.{db_table_name}") \
            .options(**get_jdbc_properties()) \
            .mode("overwrite") \
            .save()
        print(f"Successfully uploaded {csv_file_name} to {DB2_SCHEMA}.{db_table_name}")

    except Exception as e:
        print(f"ERROR: Failed to upload {csv_file_name} to {db_table_name}: {e}")
        print("Please check Db2 connectivity, table permissions, and ensure CSV schema matches expected table schema.")
        print("This often indicates a Db2 tablespace/storage limit or transaction log issue (SQLCODE=-1476, SQLSTATE=40506, SQLERRMC=-289).")
        print(f"ACTION REQUIRED: Given your 200MB Db2 storage limit, you are likely exceeding capacity. Consider uploading fewer or smaller CSV files.")



if __name__ == "__main__":
    spark = None
    try:
        spark = get_spark_session()

        files_to_upload = [
            {"csv": "cleaned_distribution_centers.csv", "table": "DISTRIBUTION_CENTERS"},
            {"csv": "cleaned_products.csv", "table": "PRODUCTS"},
           #{"csv": "cleaned_orders.csv", "table": "ORDERS"},
           #{"csv": "cleaned_users.csv", "table": "USERS"},
            {"csv": "cleaned_inventory_items.csv", "table": "INVENTORY_ITEMS", "limit_rows": 500000},
            {"csv": "cleaned_order_items.csv", "table": "ORDER_ITEMS", "limit_rows": 100000},

        ]

        for item in files_to_upload:
            csv_file = item["csv"]
            db_table = item["table"]
            limit = item.get("limit_rows")
            upload_csv_to_db2(spark, csv_file, db_table, limit_rows=limit) 

    except Exception as e:
        print(f"An unexpected error occurred during the overall data upload process: {e}")
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")