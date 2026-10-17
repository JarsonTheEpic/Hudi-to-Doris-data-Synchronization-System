"""
Module: primary_key_validator.py
Function: Validate that Hudi source and Doris target have matching primary keys.
Author: jason
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

Why need primary key validation?
----------------------------------------------------------------
1. Hudi uses _hoodie_record_key as the record identifier
2. Doris uses UNIQUE KEY to identify records (for updates and deduplication)
3. If primary keys don't match, the sync may produce duplicate records or update wrong rows
4. This validation prevents data integrity issues

Validation strategy:
    - Extract primary key fields from Hudi _hoodie_record_key
    - Extract UNIQUE KEY fields from Doris table (via SHOW CREATE TABLE)
    - Compare if they are equivalent (order doesn't matter, set comparison)
    - If mismatch: log WARNING but do NOT block the sync (non-blocking)

Why non-blocking?
    - In some cases, Doris UNIQUE KEY may include additional fields for partitioning
    - Users may have intentionally designed different key structures
    - Blocking would prevent sync even when data correctness is not affected
"""

import re
import logging

from core.connector.doris_client import DorisClient
from common.exceptions import TableNotFoundException

logger = logging.getLogger(__name__)


class PrimaryKeyValidator:
    """
    Validate that Hudi source and Doris target have matching primary keys.

    Hudi primary key is extracted from _hoodie_record_key field (format: "key1:1,key2:2").
    Doris primary key is extracted from UNIQUE KEY clause in CREATE TABLE statement.
    """

    def __init__(self):
        """Initialize the validator with a Doris client."""
        self.client = DorisClient()

    def _get_hudi_primary_keys(self, hudi_catalog: str, hudi_schema: str, hudi_table: str) -> set:
        """
        Extract primary key fields from Hudi _hoodie_record_key.

        Hudi _hoodie_record_key format: "field1:1,field2:2,field3:3"
        The number after colon is the field order (1-based).
        We extract only the field names.

        Example:
            Input:  "id:1,name:2"
            Output: {"id", "name"}

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name

        Returns:
            set: Primary key field names, or empty set if unable to parse

        Raises:
            TableNotFoundException: If the Hudi table does not exist
        """
        if hudi_schema:
            full_table = f'{hudi_catalog}.{hudi_schema}.{hudi_table}'
        else:
            full_table = f'{hudi_catalog}.{hudi_table}'

        sql = f'SELECT _hoodie_record_key FROM {full_table} LIMIT 1'

        try:
            result = self.client.query_one(sql)
            if not result:
                logger.warning(f'Hudi table {full_table} has no data, cannot extract primary key')
                return set()

            record_key = result.get('_hoodie_record_key')
            if not record_key:
                logger.warning(f'Table {full_table} has no _hoodie_record_key field')
                return set()

            # Parse format: "field1:1,field2:2,field3:3"
            # Extract field names (before colon)
            fields = []
            for part in record_key.split(','):
                if ':' in part:
                    field_name = part.split(':')[0].strip()
                    if field_name:
                        fields.append(field_name)

            return set(fields)

        except Exception as e:
            raise TableNotFoundException(f'Failed to get primary key from Hudi table {full_table}: {e}')

    def _get_doris_primary_keys(self, schema_name: str, table_name: str) -> set:
        """
        Extract UNIQUE KEY fields from Doris table via SHOW CREATE TABLE.

        Parses the CREATE TABLE statement to find UNIQUE KEY clause.

        Examples:
            UNIQUE KEY(`id`)                      -> {"id"}
            UNIQUE KEY(`id`, `name`)              -> {"id", "name"}
            UNIQUE KEY( id, name )                -> {"id", "name"}

        Args:
            schema_name: Doris schema name
            table_name: Doris table name

        Returns:
            set: UNIQUE KEY field names, or empty set if no UNIQUE KEY found

        Raises:
            TableNotFoundException: If the Doris table does not exist
        """
        sql = f'SHOW CREATE TABLE {schema_name}.{table_name}'

        try:
            result = self.client.query_one(sql)
            if not result:
                raise TableNotFoundException(f'Doris table not found: {schema_name}.{table_name}')

            create_sql = result.get('Create Table', '')
            if not create_sql:
                return set()

            # Parse UNIQUE KEY clause
            # Pattern: UNIQUE KEY(`field1`, `field2`)  or UNIQUE KEY(field1, field2)
            # Note: Doris UNIQUE KEY can also be written as UNIQUE KEY `key_name` (`field1`, `field2`)
            pattern = r'UNIQUE\s+KEY\s*(?:`?\w+`?\s*)?\s*\(([^)]+)\)'
            match = re.search(pattern, create_sql, re.IGNORECASE)

            if not match:
                logger.warning(f'No UNIQUE KEY found in Doris table: {schema_name}.{table_name}')
                return set()

            # Extract field names from the parentheses
            # Remove backticks and split by comma
            key_part = match.group(1)
            fields = []
            for field in key_part.split(','):
                # Remove backticks and whitespace
                field_name = field.strip().strip('`')
                if field_name:
                    fields.append(field_name)

            return set(fields)

        except Exception as e:
            raise TableNotFoundException(f'Failed to get primary key from Doris table {schema_name}.{table_name}: {e}')

    def validate(
        self,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str,
        doris_schema: str,
        doris_table: str
    ) -> bool:
        """
        Validate that Hudi and Doris primary keys match.

        This is a non-blocking check. Returns True if keys match,
        False with warning log if they don't match.

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name
            doris_schema: Doris schema name
            doris_table: Doris table name

        Returns:
            bool: True if keys match or cannot be compared,
                  False if keys explicitly mismatch

        Raises:
            TableNotFoundException: If either table does not exist
        """
        try:
            hudi_keys = self._get_hudi_primary_keys(hudi_catalog, hudi_schema, hudi_table)
            doris_keys = self._get_doris_primary_keys(doris_schema, doris_table)

            # If Hudi has no primary key data, cannot compare
            if not hudi_keys:
                logger.warning(f'Cannot get Hudi primary key for {hudi_table}, skipping validation')
                return True

            # If Doris has no UNIQUE KEY, this is a problem
            if not doris_keys:
                logger.warning(f'Doris table {doris_schema}.{doris_table} has no UNIQUE KEY')
                return False

            # Compare as sets (order doesn't matter)
            if hudi_keys == doris_keys:
                logger.info(f'Primary keys match: {hudi_keys}')
                return True
            else:
                logger.warning(
                    f'Primary key mismatch: Hudi keys={hudi_keys}, Doris keys={doris_keys}'
                )
                return False

        except TableNotFoundException:
            # Re-raise table not found errors
            raise
        except Exception as e:
            # Other errors: log warning but don't block
            logger.warning(f'Primary key validation failed with unexpected error: {e}')
            return True