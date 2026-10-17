"""
Module: schema_validator.py
Function: Validate that source and target tables have matching column structures.
Author: jason
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

Why need schema validation?
----------------------------------------------------------------
1. Doris INSERT INTO SELECT requires that column count and order match
2. If columns are misaligned, data will be written to wrong columns (silent data corruption)
3. Doris does not support column name mapping like some other databases
4. Hudi system fields (_hoodie_*) must be excluded from sync

Validation checks:
    - Column count: Source and target must have the same number of columns
    - Column order: Column order must be identical (Doris is position-sensitive)
    - Hudi system fields are automatically excluded from comparison
"""

from core.connector.doris_client import DorisClient
from common.exceptions import SchemaNotMatchException, TableNotFoundException
from config.constants import HUDI_SYSTEM_FIELDS


class SchemaValidator:
    """
    Validate that source Hudi table and target Doris table have matching schemas.

    Compares column count and column order between source and target.
    Hudi system fields are automatically excluded from the comparison.
    """

    def __init__(self):
        """Initialize the validator with a Doris client."""
        self.client = DorisClient()

    def _get_hudi_columns(self, hudi_catalog: str, hudi_schema: str, hudi_table: str) -> list:
        """
        Get column names from Hudi source table, excluding Hudi system fields.

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name

        Returns:
            list: Column names in original order

        Raises:
            TableNotFoundException: If the Hudi table does not exist
        """
        if hudi_schema:
            full_table = f'{hudi_catalog}.{hudi_schema}.{hudi_table}'
        else:
            full_table = f'{hudi_catalog}.{hudi_table}'

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
            if 'does not exist' in str(e).lower():
                raise TableNotFoundException(f'Hudi table not found: {full_table}')
            raise

    def _get_doris_columns(self, schema_name: str, table_name: str) -> list:
        """
        Get column names from Doris target table.

        Args:
            schema_name: Doris schema name
            table_name: Doris table name

        Returns:
            list: Column names in original order

        Raises:
            TableNotFoundException: If the Doris table does not exist
        """
        sql = f'DESC {schema_name}.{table_name}'

        try:
            result = self.client.query(sql)
            if not result:
                raise TableNotFoundException(f'Doris table not found: {schema_name}.{table_name}')

            return [row.get('Field') for row in result]

        except Exception as e:
            if 'does not exist' in str(e).lower():
                raise TableNotFoundException(f'Doris table not found: {schema_name}.{table_name}')
            raise

    def validate(
        self,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str,
        doris_schema: str,
        doris_table: str
    ) -> None:
        """
        Validate that Hudi source and Doris target have matching column structures.

        Checks:
            1. Both tables exist
            2. Column count matches (after excluding Hudi system fields)
            3. Column order matches

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name
            doris_schema: Doris schema name
            doris_table: Doris table name

        Raises:
            TableNotFoundException: If either table does not exist
            SchemaNotMatchException: If column count or order does not match
        """
        # Get columns from both tables
        hudi_columns = self._get_hudi_columns(hudi_catalog, hudi_schema, hudi_table)
        doris_columns = self._get_doris_columns(doris_schema, doris_table)

        # Check 1: Column count must match
        if len(hudi_columns) != len(doris_columns):
            raise SchemaNotMatchException(
                f'Column count mismatch: '
                f'Hudi has {len(hudi_columns)} columns, '
                f'Doris has {len(doris_columns)} columns'
            )

        # Check 2: Column order must match
        for i, (hudi_col, doris_col) in enumerate(zip(hudi_columns, doris_columns)):
            if hudi_col != doris_col:
                raise SchemaNotMatchException(
                    f'Column mismatch at position {i + 1}: '
                    f'Hudi column "{hudi_col}" vs Doris column "{doris_col}"'
                )

        # All checks passed
        return