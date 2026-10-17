"""
Module: table_config.py
Function: Read table mapping configuration from project_table_mapping table.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

Why need a config reader layer?
----------------------------------------------------------------
1. Isolate database query logic from business logic
2. Single place to maintain SQL queries for configuration tables
3. Easy to add caching or other optimizations in the future
4. Clear interface for the worker layer to call
"""

from core.connector.doris_client import DorisClient
from common.exceptions import ConfigNotFoundException


class TableConfigReader:
    """
    Read table mapping configurations from project_table_mapping.

    Provides methods to retrieve table configurations for a given project.
    Only returns active configurations (is_active = 1) ordered by sync_order.
    """

    def __init__(self):
        """Initialize the config reader with a Doris client."""
        self.client = DorisClient()

    def get_tables_by_project(self, project_code: str) -> list:
        """
        Get all active table mappings for a project.

        Args:
            project_code: The project code to query

        Returns:
            List of dictionaries, each containing table mapping configuration.
            Each dict includes: project_code, schema_name, table_name,
            hudi_catalog, hudi_schema, hudi_table, sync_mode, sync_order, etc.

        Raises:
            ConfigNotFoundException: If no active configurations found for the project
        """
        sql = f"""
            SELECT
                project_code,
                schema_name,
                table_name,
                hudi_catalog,
                hudi_schema,
                hudi_table,
                sync_mode,
                sync_order,
                is_active,
                remark
            FROM project_table_mapping
            WHERE project_code = '{project_code}'
              AND is_active = 1
            ORDER BY sync_order ASC
        """

        results = self.client.query(sql)

        if not results:
            raise ConfigNotFoundException(
                f'No active table configurations found for project: {project_code}'
            )

        return results

    def get_table_count(self, project_code: str) -> int:
        """
        Get the number of active tables for a project.

        Args:
            project_code: The project code

        Returns:
            int: Count of active table mappings
        """
        sql = f"""
            SELECT COUNT(*) as cnt
            FROM project_table_mapping
            WHERE project_code = '{project_code}'
              AND is_active = 1
        """

        result = self.client.query_one(sql)
        return result.get('cnt', 0) if result else 0

    def get_table_by_target(self, project_code: str, schema_name: str, table_name: str) -> dict:
        """
        Get a specific table mapping by target table name.

        Args:
            project_code: The project code
            schema_name: The Doris schema name
            table_name: The Doris table name

        Returns:
            dict: Table mapping configuration

        Raises:
            ConfigNotFoundException: If the specific configuration is not found
        """
        sql = f"""
            SELECT
                project_code,
                schema_name,
                table_name,
                hudi_catalog,
                hudi_schema,
                hudi_table,
                sync_mode,
                sync_order,
                is_active,
                remark
            FROM project_table_mapping
            WHERE project_code = '{project_code}'
              AND schema_name = '{schema_name}'
              AND table_name = '{table_name}'
              AND is_active = 1
        """

        result = self.client.query_one(sql)

        if not result:
            raise ConfigNotFoundException(
                f'Table configuration not found: {project_code}.{schema_name}.{table_name}'
            )

        return result