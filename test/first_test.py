import pymysql
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--mode", default="full")
parser.add_argument("--table", required=True)
args = parser.parse_args()

connection = pymysql.connect(
    host = "10.25.84.152",
    port = 9030,
    user = "jason1",
    password = "sunshina01",
    db = "sandbox",
)

source_db = "sandbox"
target_db = "sandbox"
table = "dim_gg_cgsjmx_cxsjzd"

print(f"mode={args.mode}, table={args.table}")

try:
    with connection.cursor() as cursor:

        # Select target table
        cursor.execute(f"TRUNCATE TABLE {target_db}.{table}")

        # Sync all data
        sql = f"""
        INSERT INTO {target_db}.{table}
        SELECT * FROM {source_db}.{table}
        """
        cursor.execute(sql)

        # Confirmation of success
        print("Full sync completed successfully")

        print(dir(pymysql))
        # abc test

finally: # If connection fails
    connection.close()
