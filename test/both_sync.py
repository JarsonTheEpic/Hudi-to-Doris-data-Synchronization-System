import pymysql
import argparse

# ==========================================================
# CLI arguments
# ==========================================================
parser = argparse.ArgumentParser()

parser.add_argument(
    "--mode",
    choices=["full", "incremental", "none"],
    default="none",
    help="sync mode: full or incremental",
)

parser.add_argument(
    "--project",
    required = False,
    help="Choose a project to sync",
)

parser.add_argument(
    "--table",
    default = None,
    help="Choose a single table to sync",
)

parser.add_argument(
    "--dry_run",
    default = False,
    help="Dry run: Only prints tables to sync",
)


args = parser.parse_args()

mode = args.mode
project = args.project
chosen_table = args.table
dry = args.dry_run


def full_sync(target_db, target_tbl, source_catalog, source_db, source_tbl):
    try:
        cursor.execute(f"TRUNCATE TABLE {target_db}.{target_tbl}")

        sql = f"""
        INSERT INTO {target_db}.{target_tbl}
        SELECT *
        EXCEPT (
            _hoodie_commit_time,
            _hoodie_commit_seqno,
            _hoodie_record_key,
            _hoodie_partition_path,
            _hoodie_file_name
        )
        FROM {source_catalog}.{source_db}.{source_tbl}
        """

        cursor.execute(sql)

    except pymysql.err.OperationalError as e:
        print(f"[WARNING] Skipping {target_tbl}: {e}")
        return


def incremental_sync(target_db, target_tbl, source_catalog, source_db, source_tbl):
    try:
        cursor.execute(f"""
            SELECT last_sync_time
            FROM com.sync_checkpoint
            WHERE database_name = '{target_db}'
            AND table_name = '{target_tbl}'
        """)

        row = cursor.fetchone()
        last_sync_time = row[0] if row and row[0] else "1970-01-01 00:00:00"

        sql = f"""
        INSERT INTO {target_db}.{target_tbl}
        SELECT *
        EXCEPT (
            _hoodie_commit_time,
            _hoodie_commit_seqno,
            _hoodie_record_key,
            _hoodie_partition_path,
            _hoodie_file_name
        )
        FROM {source_catalog}.{source_db}.{source_tbl}
        WHERE _hoodie_commit_time > '{last_sync_time}'
        """

        cursor.execute(sql)

    except pymysql.err.OperationalError as e:
        print(f"[WARNING] Skipping {target_tbl}: {e}")
        return


# ==========================================================
# Doris connection
# ==========================================================
connection = pymysql.connect(
    host = "192.168.30.129",
    port = 9030,
    user = "root",
    password = ""
)


try:
    with connection.cursor() as cursor:

        # ======================================================
        # Load sync config
        # ======================================================
        cursor.execute(f"""
            SELECT target_db, target_tbl,
                   source_catalog, source_db, source_tbl, sync_mode
            FROM com.sync_tables_config
            WHERE project_code = '{project}'
        """)

        tasks = cursor.fetchall()

        if chosen_table != None:
            cursor.execute(f"""
                SELECT target_db, target_tbl,
                       source_catalog, source_db, source_tbl
                FROM com.sync_tables_config
                WHERE target_tbl = '{chosen_table}'
            """)

            t = cursor.fetchone()
            print(t)

            target_db, target_tbl, source_catalog, source_db, source_tbl = t

            if mode == "full" and not dry:
                full_sync(target_db, target_tbl, source_catalog, source_db, source_tbl)

            elif mode == "incremental" and not dry:
                incremental_sync(target_db, target_tbl, source_catalog, source_db, source_tbl)   

            print(f"Done {target_tbl}\n")
            connection.close()

        if len(tasks) == 0:
            print("Project not found.")
        
        print(f"Mode: {mode}")
        print(f"Tables: {len(tasks)}\n")

        # ======================================================
        # Loop all tables
        # ======================================================
        for t in tasks:

            target_db, target_tbl, source_catalog, source_db, source_tbl, sync_mode = t

            print(f"Syncing {target_db}.{target_tbl} ...")

            if mode == "full" and not dry:
                full_sync(target_db, target_tbl, source_catalog, source_db, source_tbl)

            elif mode == "incremental" and not dry:
                incremental_sync(target_db, target_tbl, source_catalog, source_db, source_tbl)
            
            # ==================================================
            # If sync mode not selected (none)
            # ==================================================
            else:
                if sync_mode == "full" and not dry:
                    full_sync(target_db, target_tbl, source_catalog, source_db, source_tbl)
                elif sync_mode == "incremental" and not dry:
                    incremental_sync(target_db, target_tbl, source_catalog, source_db, source_tbl)


            # ==================================================
            # update checkpoint
            # ==================================================
            if not dry:
                cursor.execute(f"""
                INSERT INTO com.sync_checkpoint
                (database_name, table_name, last_sync_time)
                VALUES
                ('{target_db}', '{target_tbl}', NOW())
                """)

            print(f"Done {target_tbl}\n")

finally:
    if connection.open:  
        connection.close()
