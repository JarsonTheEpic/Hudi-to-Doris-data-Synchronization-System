import pymysql

connection = pymysql.connect(
    host = "192.168.30.129",
    port = 9030,
    user = "root",
    password = "",
    db = "test_database"
)

source_db = "test_database"
target_db = "backup_database"
table = "students"

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
