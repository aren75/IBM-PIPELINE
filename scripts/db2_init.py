
import ibm_db
import pandas as pd
import os

DB2_HOSTNAME = os.getenv('DB2_HOSTNAME', '2f3279a5-73d1-4859-88f0-a6c3e6b4b907.c3n41cmd0nqnrk39u98g.databases.appdomain.cloud')
DB2_UID = os.getenv('DB2_UID', 'hgs24706')
DB2_PWD = os.getenv('DB2_PWD', 'mH70XOjwcmwpmtQx')
DB2_DATABASE = os.getenv('DB2_DATABASE', 'bludb')
DB2_PORT = os.getenv('DB2_PORT', '30756')

def get_db2_connection():
    dsn = (
        f"DATABASE={DB2_DATABASE};"
        f"HOSTNAME={DB2_HOSTNAME};"
        f"PORT={DB2_PORT};"
        f"PROTOCOL=TCPIP;"
        f"UID={DB2_UID};"
        f"PWD={DB2_PWD};"
        "SECURITY=SSL;"
    )
    try:
        conn = ibm_db.connect(dsn, "", "")
        print("Successfully connected to Db2 database")
        return conn
    except Exception as e:
        print("Connection failed:", e)
        return None

def create_and_insert_distribution_centers(conn, csv_path):
    create_table_sql = """
    CREATE TABLE distribution_centers (
        id INTEGER,
        name VARCHAR(100),
        latitude DOUBLE,
        longitude DOUBLE
    )
    """
    try:
        ibm_db.exec_immediate(conn, create_table_sql)
        print("Table 'distribution_centers' created (if it didn't exist).")
    except Exception as e:
        print(f"Table 'distribution_centers' might already exist or an error occurred: {e}")

    try:
        df = pd.read_csv(csv_path)
        print(f"Inserting {len(df)} rows into distribution_centers...")
        for i, row in df.iterrows():
            insert_sql = f"""
            INSERT INTO distribution_centers (id, name, latitude, longitude)
            VALUES ({int(row['id'])}, '{row['name'].replace("'", "''")}', {row['latitude']}, {row['longitude']})
            """
            ibm_db.exec_immediate(conn, insert_sql)
        print("Data inserted successfully into 'distribution_centers'.")
    except Exception as e:
        print(f"Error inserting data into 'distribution_centers': {e}")

if __name__ == "__main__":
    conn = get_db2_connection()
    if conn:
        create_and_insert_distribution_centers(conn, 'data/Source_Data/cleaned_distribution_centers.csv')
        ibm_db.close(conn)