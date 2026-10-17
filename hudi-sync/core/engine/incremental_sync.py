"""
Module: incremental_sync.py
Function: Perform incremental sync from Hudi source to Doris target.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

What is incremental sync?
----------------------------------------------------------------
Incremental sync only inserts new data that has been added since the last sync.

Key mechanism:
    1. Read last_commit_time from checkpoint table
    2. Query Hudi source table: SELECT * WHERE _hoodie_commit_time > last_commit_time
    3. Insert the new rows into Doris target table
    4. Update checkpoint with new max commit time

Why use _hoodie_commit_time?
    - Hudi automatically adds this field to every row
    - It records the time when the row was committed to the Hudi table
    - It is monotonically increasing, making it perfect for incremental queries

Fallback logic:
    - If no checkpoint exists (first sync): automatically fallback to full sync
    - If _hoodie_commit_time field is missing: raise error, caller handles fallback
"""

import logging

from core.connector.doris_client import DorisClient
from core.validator.schema_validator import SchemaValidator
from common.exceptions import SyncFailedException, TableNotFoundException
from config.constants import HUDI_SYSTEM_FIELDS

logger = logging.getLogger(__name__)


class IncrementalSync:
    """
    Execute incremental sync from Hudi source to Doris target.

    Incremental sync: INSERT new rows based on _hoodie_commit_time > last_commit_time.
    """

    def __init__(self):
        """Initialize the incremental sync engine with a Doris client."""
        self.client = DorisClient()
        self.schema_validator = SchemaValidator()

    def _build_full_table_name(self, catalog: str, schema: str, table: str) -> str:
        """
        Build a fully qualified table name.

        Examples:
            catalog='hudi', schema='default', table='dim_xxx' → hudi.default.dim_xxx
            catalog='hudi', schema='', table='dim_xxx'        → hudi.dim_xxx
            catalog='internal', schema='ods', table='dim_xxx' → internal.ods.dim_xxx

        Args:
            catalog: Catalog name (e.g., 'hudi', 'internal')
            schema: Schema name (may be empty string)
            table: Table name

        Returns:
            str: Fully qualified table name
        """
        if schema:
            return f'{catalog}.{schema}.{table}'
        return f'{catalog}.{table}'

    def _get_business_columns(self, hudi_catalog: str, hudi_schema: str, hudi_table: str) -> list:
        """
        Get business columns from Hudi table (exclude Hudi system fields).

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name

        Returns:
            list: Business column names

        Raises:
            TableNotFoundException: If the Hudi table does not exist
        """
        full_table = self._build_full_table_name(hudi_catalog, hudi_schema, hudi_table)
        sql = f'DESC {full_table}'

        try:
            result = self.client.query(sql)
            if not result:
                raise TableNotFoundException(f'Hudi table not found: {full_table}')

            all_columns = [row.get('Field') for row in result]
            business_columns = [
                col for col in all_columns
                if col not in HUDI_SYSTEM_FIELDS
            ]
            return business_columns

        except Exception as e:
            raise TableNotFoundException(f'Failed to get columns from Hudi table {full_table}: {e}')

    def _check_hudi_commit_time_exists(self, hudi_catalog: str, hudi_schema: str, hudi_table: str) -> bool:
        """
        Check if the Hudi table has the _hoodie_commit_time field.

        This field is required for incremental sync. If missing, incremental sync cannot proceed.

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name

        Returns:
            bool: True if _hoodie_commit_time exists, False otherwise
        """
        full_table = self._build_full_table_name(hudi_catalog, hudi_schema, hudi_table)
        sql = f'DESC {full_table}'

        try:
            result = self.client.query(sql)
            if not result:
                return False

            for row in result:
                if row.get('Field') == '_hoodie_commit_time':
                    return True
            return False

        except Exception:
            return False

    def sync(
        self,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str,
        doris_schema: str,
        doris_table: str,
        last_commit_time: str
    ) -> int:
        """
        Execute incremental sync from Hudi source to Doris target.

        Args:
            hudi_catalog: Hudi catalog name (e.g., 'hudi')
            hudi_schema: Hudi schema name (e.g., 'default', may be empty)
            hudi_table: Hudi table name
            doris_schema: Doris schema name (e.g., 'ods')
            doris_table: Doris table name
            last_commit_time: Last synced commit time (format: YYYYMMDDHHMMSS)

        Returns:
            int: Number of rows inserted

        Raises:
            SyncFailedException: If sync fails
            TableNotFoundException: If source or target table does not exist
        """
        # Step 1: Validate schema before syncing
        logger.info(f'Validating schema for {hudi_table} -> {doris_schema}.{doris_table}')
        self.schema_validator.validate(
            hudi_catalog=hudi_catalog,
            hudi_schema=hudi_schema,
            hudi_table=hudi_table,
            doris_schema=doris_schema,
            doris_table=doris_table
        )

        # Step 2: Check if _hoodie_commit_time field exists
        logger.info(f'Checking _hoodie_commit_time field in Hudi table: {hudi_table}')
        if not self._check_hudi_commit_time_exists(hudi_catalog, hudi_schema, hudi_table):
            raise SyncFailedException(
                f'Hudi table {hudi_catalog}.{hudi_schema}.{hudi_table} '
                'does not have _hoodie_commit_time field. Incremental sync not supported.'
            )

        # Step 3: Get business columns from Hudi table
        logger.info(f'Getting business columns from Hudi table: {hudi_table}')
        business_columns = self._get_business_columns(hudi_catalog, hudi_schema, hudi_table)

        if not business_columns:
            raise SyncFailedException(
                f'No business columns found in Hudi table {hudi_table}. '
                'All columns are Hudi system fields?'
            )

        columns_str = ', '.join(business_columns)
        logger.info(f'Business columns ({len(business_columns)}): {columns_str}')

        # Step 4: Build table names
        hudi_full_table = self._build_full_table_name(hudi_catalog, hudi_schema, hudi_table)
        doris_full_table = self._build_full_table_name('internal', doris_schema, doris_table)

        # Step 5: Insert incremental data
        logger.info(f'Inserting incremental data from {hudi_full_table} to {doris_full_table}')
        logger.info(f'Filter condition: _hoodie_commit_time > {last_commit_time}')

        insert_sql = f'''
            INSERT INTO {doris_full_table} ({columns_str})
            SELECT {columns_str}
            FROM {hudi_full_table}
            WHERE _hoodie_commit_time > '{last_commit_time}'
        '''

        try:
            rows_inserted = self.client.execute(insert_sql)
            logger.info(f'Incremental sync completed: {rows_inserted} rows inserted')
            return rows_inserted
        except Exception as e:
            raise SyncFailedException(f'Failed to insert incremental data: {e}')