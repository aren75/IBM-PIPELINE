import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, lit
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

    print("Initializing Spark Session for Most Sold Products Analysis...")
    spark = SparkSession.builder \
        .appName("Most Sold Products Analysis") \
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
        "batchsize": "50" 
    }

def get_jdbc_url():
    if not all([DB2_HOSTNAME, DB2_PORT, DB2_DATABASE]):
        print("ERROR: Db2 hostname, port, or database name missing from .env.")
        exit(1)
    return f"jdbc:db2://{DB2_HOSTNAME}:{DB2_PORT}/{DB2_DATABASE}"


def process_and_upload_most_sold_products(spark_session, file_limits=None): 
   
    print("\n--- Processing and Uploading MOST_SOLD_PRODUCTS to DB2 ---")

    required_csv_files = {
        "order_items": "cleaned_order_items.csv",
        "products": "cleaned_products.csv",
        "distribution": "cleaned_distribution_centers.csv",
        "inventory": "cleaned_inventory_items.csv"
    }

    order_items_df = None
    products_df = None
    distribution_df = None
    inventory_df = None

    try:
        for df_name, file_name in required_csv_files.items():
            file_path = os.path.join(SOURCE_DATA_DIR, file_name)
            
            if not os.path.exists(file_path):
                print(f"ERROR: Required file for Most Sold Products not found: {file_path}. Skipping this operation.")
                return

            print(f"Reading {file_name}...")
            temp_df = spark_session.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(file_path)

            limit = file_limits.get(file_name) if file_limits else None
            if limit is not None:
                print(f"Limiting {file_name} to first {limit} rows.")
                temp_df = temp_df.limit(limit)

            if df_name == "order_items":
                order_items_df = temp_df
            elif df_name == "products":
                products_df = temp_df
            elif df_name == "distribution":
                distribution_df = temp_df
            elif df_name == "inventory":
                inventory_df = temp_df
        
        if not all([order_items_df, products_df, distribution_df, inventory_df]):
            print("ERROR: Not all required DataFrames could be loaded. Aborting Most Sold Products analysis.")
            return

        most_sold_products = order_items_df \
            .join(inventory_df, order_items_df["inventory_item_id"] == inventory_df["id"], "left") \
            .join(products_df, inventory_df["product_id"] == products_df["id"], "left") \
            .join(distribution_df, inventory_df["product_distribution_center_id"] == distribution_df["id"], "left") \
            .groupBy(
                products_df["id"].alias("product_id"),
                products_df["name"].alias("product_name"),
                distribution_df["name"].alias("distribution_center")
            ) \
            .agg(
                _sum(lit(1)).alias("total_quantity_sold") 
            ) \
            .orderBy("total_quantity_sold", ascending=False)

        print("Schema for MOST_SOLD_PRODUCTS:")
        most_sold_products.printSchema()
        print("First 5 rows of MOST_SOLD_PRODUCTS:")
        most_sold_products.show(5)

        print(f"Attempting to write MOST_SOLD_PRODUCTS with JDBC batchsize: {get_jdbc_properties()['batchsize']}")
        most_sold_products.write.format("jdbc") \
            .option("url", get_jdbc_url()) \
            .option("dbtable", f"{DB2_SCHEMA}.MOST_SOLD_PRODUCTS") \
            .options(**get_jdbc_properties()) \
            .mode("overwrite") \
            .save()
        print(f"Successfully uploaded MOST_SOLD_PRODUCTS to {DB2_SCHEMA}.MOST_SOLD_PRODUCTS")

    except Exception as e:
        print(f"ERROR: Failed to process and upload MOST_SOLD_PRODUCTS: {e}")
        print("Please check data integrity of source CSVs and Db2 connectivity/permissions.")
        print(f"ACTION REQUIRED: Given your 200MB Db2 storage limit, you are likely exceeding capacity. Consider reducing row limits for input CSVs drastically.")
        print("HINT: You might need to try limits as low as 1000-5000 rows for the larger CSVs.")

if __name__ == "__main__":
    spark = None
    try:
        spark = get_spark_session()

        most_sold_file_limits = {
            "cleaned_order_items.csv": 50000, 
            "cleaned_products.csv": None,    
            "cleaned_distribution_centers.csv": None, 
            "cleaned_inventory_items.csv": 20000,
        }

        process_and_upload_most_sold_products(spark, file_limits=most_sold_file_limits)
    except Exception as e:
        print(f"An unexpected error occurred during the Most Sold Products analysis and upload process: {e}")
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")