"""
Module: full_sync.py
Function: Perform full sync from Hudi source to Doris target.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

What is full sync?
----------------------------------------------------------------
Full sync replaces all data in the target table with all data from the source table.

Steps:
    1. DELETE all data from Doris target table
    2. INSERT INTO Doris target table SELECT all business columns from Hudi source table

Key points:
    - Hudi system fields (_hoodie_*) are excluded from SELECT and INSERT
    - This ensures target table only contains business data
    - Full sync is more expensive than incremental sync but guarantees data consistency
"""

import logging

from core.connector.doris_client import DorisClient
from core.validator.schema_validator import SchemaValidator
from common.exceptions import SyncFailedException, TableNotFoundException
from config.constants import HUDI_SYSTEM_FIELDS

logger = logging.getLogger(__name__)


class FullSync:
    """
    Execute full sync from Hudi source to Doris target.

    Full sync: DELETE all target data, then INSERT all source data.
    """

    def __init__(self):
        """Initialize the full sync engine with a Doris client."""
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

        Uses DESC to get all columns, then filters out _hoodie_* fields.

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

            # Extract column names and filter out Hudi system fields
            all_columns = [row.get('Field') for row in result]
            business_columns = [
                col for col in all_columns
                if col not in HUDI_SYSTEM_FIELDS
            ]
            return business_columns

        except Exception as e:
            raise TableNotFoundException(f'Failed to get columns from Hudi table {full_table}: {e}')

    def sync(
        self,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str,
        doris_schema: str,
        doris_table: str
    ) -> int:
        """
        Execute full sync from Hudi source to Doris target.

        Args:
            hudi_catalog: Hudi catalog name (e.g., 'hudi')
            hudi_schema: Hudi schema name (e.g., 'default', may be empty)
            hudi_table: Hudi table name
            doris_schema: Doris schema name (e.g., 'ods')
            doris_table: Doris table name

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

        # Step 2: Get business columns from Hudi table
        logger.info(f'Getting business columns from Hudi table: {hudi_table}')
        business_columns = self._get_business_columns(hudi_catalog, hudi_schema, hudi_table)

        if not business_columns:
            raise SyncFailedException(
                f'No business columns found in Hudi table {hudi_table}. '
                'All columns are Hudi system fields?'
            )

        columns_str = ', '.join(business_columns)
        logger.info(f'Business columns ({len(business_columns)}): {columns_str}')

        # Step 3: Build table names
        hudi_full_table = self._build_full_table_name(hudi_catalog, hudi_schema, hudi_table)
        doris_full_table = self._build_full_table_name('internal', doris_schema, doris_table)

        # Step 4: Delete all data from Doris target table
        logger.info(f'Deleting all data from target: {doris_full_table}')
        delete_sql = f'DELETE FROM {doris_full_table}'

        try:
            self.client.execute(delete_sql)
            logger.info(f'Target table cleared: {doris_full_table}')
        except Exception as e:
            raise SyncFailedException(f'Failed to delete target table data: {e}')

        # Step 5: Insert all data from Hudi source to Doris target
        logger.info(f'Inserting data from {hudi_full_table} to {doris_full_table}')
        insert_sql = f'''
            INSERT INTO {doris_full_table} ({columns_str})
            SELECT {columns_str}
            FROM {hudi_full_table}
        '''

        try:
            rows_inserted = self.client.execute(insert_sql)
            logger.info(f'Full sync completed: {rows_inserted} rows inserted')
            return rows_inserted
        except Exception as e:
            raise SyncFailedException(f'Failed to insert data: {e}')