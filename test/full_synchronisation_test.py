import pymysql

# ==========================================================
# Doris connection configuration
# ==========================================================

connection = pymysql.connect(
    host = "10.25.84.152",
    port = 9030,
    user = "jason1",
    password = "sunshina01",
    db = "sandbox",
)

try:
    with connection.cursor() as cursor:

        # ======================================================
        # Read all synchronization tasks
        # ======================================================
        cursor.execute("""
            SELECT
                target_db,
                target_tbl,
                source_catalog,
                source_db,
                source_tbl
            FROM sandbox.sync_tables_config
        """)

        tasks = cursor.fetchall()

        print(f"Found {len(tasks)} table(s) to synchronize.\n")

        # ======================================================
        # Synchronize each table
        # ======================================================
        for task in tasks:

            target_db = task[0]
            target_tbl = task[1]
            source_catalog = task[2]
            source_db = task[3]
            source_tbl = task[4]

            print(f"Synchronizing {target_db}.{target_tbl}...")

            # --------------------------------------------------
            # Step 1: Clear the target table
            # --------------------------------------------------
            cursor.execute(f"""
                TRUNCATE TABLE {target_db}.{target_tbl}
            """)

            # --------------------------------------------------
            # Step 2: Full synchronization
            # Ignore all Hudi metadata columns
            # --------------------------------------------------
            sync_sql = f"""
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

            cursor.execute(sync_sql)

            # --------------------------------------------------
            # Step 3: Update checkpoint table
            # UNIQUE KEY table will overwrite existing record
            # --------------------------------------------------
            checkpoint_sql = f"""
                INSERT INTO sandbox.sync_checkpoint
                (
                    database_name,
                    table_name,
                    last_sync_time
                )
                VALUES
                (
                    '{target_db}',
                    '{target_tbl}',
                    NOW()
                )
            """

            cursor.execute(checkpoint_sql)

            print(f"✓ Finished {target_tbl}")

    print("\nAll tables synchronized successfully.")

except Exception as e:
    print("\nSynchronization failed.")
    print(e)

finally:
    connection.close()
    