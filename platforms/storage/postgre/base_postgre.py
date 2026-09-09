from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
from platforms.storage.base_storage import StorageBackend
import psycopg2


class PostgreSQLWriter:
    """
    Lightweight PostgreSQL writer. Uses psycopg2 for database interactions.
    """

    def __init__(self, host: str, port: int, database: str, username: str, password: str, logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.database = database
        self.user = username
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        


    def _get_connection(self):
        
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def insert(self, table: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Insert list of dictionaries into a PostgreSQL table."""
        if not data:
            return {"inserted_count": 0}

        keys = data[0].keys()
        columns = ', '.join(keys)
        values_placeholder = ', '.join([f'%({key})s' for key in keys])
        query = f"INSERT INTO {table} ({columns}) VALUES ({values_placeholder})"

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(query, data)
                    conn.commit()
                    self.logger.info("Inserted %d rows into %s", len(data), table)
                    return {"inserted_count": len(data)}
        except Exception as e:
            self.logger.error("Insert failed: %s", e)
            return {"inserted_count": 0, "error": str(e)}


class PostgreSQLStorageBackend(StorageBackend):
    """Adapter to expose PostgreSQLWriter as StorageBackend."""

    def __init__(self, pipeline_logger, postgres_writter: PostgreSQLWriter):
        self.pg = postgres_writter
        self.logger = pipeline_logger or logging.getLogger(__name__)
        print(self.pg.user)

    def save(self, dataset_name: str, data: Any, fmt: Optional[str] = None) -> Dict[str, Any]:
        """Save data to a PostgreSQL table."""
        try:
            if not isinstance(data, list):
                data = [data]
            result = self.pg.insert(dataset_name, data)
            return {"ok": True, **result}
        except Exception as e:
            self.logger.exception("PostgreSQL save failed")
            return {"ok": False, "error": str(e)}

    def create_database(self, name: str) -> Dict[str, Any]:
        """Create a new PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(f"CREATE DATABASE {name}")
                    return {"ok": True, "database": name}
        except Exception as e:
            self.logger.error("Create database failed: %s", e)
            return {"ok": False, "error": str(e)}

    def delete_database(self, name: str) -> Dict[str, Any]:
        """Delete a PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(f"DROP DATABASE IF EXISTS {name}")
                    return {"ok": True, "database": name}
        except Exception as e:
            self.logger.error("Delete database failed: %s", e)
            return {"ok": False, "error": str(e)}

    def create_schema(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        """Create a schema in the PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {name}")
                    return {"ok": True, "schema": name}
        except Exception as e:
            self.logger.error("Create schema failed: %s", e)
            return {"ok": False, "error": str(e)}

    def rename_schema(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename a schema in the PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"ALTER SCHEMA {old_name} RENAME TO {new_name}")
                    return {"ok": True, "from": old_name, "to": new_name}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def create_table(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        """Create a table in the PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    columns = ', '.join([f"{col} {dtype}" for col, dtype in schema.items()]) if schema else "id SERIAL PRIMARY KEY"
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {name} ({columns})")
                    return {"ok": True, "table": name}
        except Exception as e:
            self.logger.error("Create table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def truncate_table(self, name: str) -> Dict[str, Any]:
        """Truncate a table in the PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {name}")
                    return {"ok": True, "table": name}
        except Exception as e:
            self.logger.error("Truncate table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def delete_table(self, name: str) -> Dict[str, Any]:
        """Delete a table in the PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {name}")
                    return {"ok": True, "table": name}
        except Exception as e:
            self.logger.error("Delete table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def rename_table(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename a table in the PostgreSQL database."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
                    return {"ok": True, "from": old_name, "to": new_name}
        except Exception as e:
            self.logger.error("Rename table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        """Insert data into a PostgreSQL table."""
        try:
            result = self.pg.insert(target, data)
            return {"ok": True, **result}
        except Exception as e:
            self.logger.error("Insert failed: %s", e)
            return {"ok": False, "error": str(e)}

    def update(self, target: str, query: Dict[str, Any], update_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Update rows in a PostgreSQL table."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    set_clause = ', '.join([f"{k} = %s" for k in update_doc.keys()])
                    where_clause = ' AND '.join([f"{k} = %s" for k in query.keys()])
                    values = list(update_doc.values()) + list(query.values())
                    cur.execute(f"UPDATE {target} SET {set_clause} WHERE {where_clause}", values)
                    conn.commit()
                    return {"ok": True, "updated_count": cur.rowcount}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def execute(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        """Execute a raw SQL query."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    conn.commit()
                    return {"ok": True}
        except Exception as e:
            self.logger.error("Execute failed: %s", e)
            return {"ok": False, "error": str(e)}
    
    def bulk_insert(self, table: str, data) -> Dict[str, Any]:
        """Bulk insert data into a PostgreSQL table."""
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        return self.insert(table.upper(), data)
    
    def query(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        """Execute a query and return results."""
        try:
            with self.pg._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    results = [dict(zip(columns, row)) for row in rows]
                    return {"ok": True, "results": results}
        except Exception as e:
            self.logger.error("Query failed: %s", e)
            return {"ok": False, "error": str(e)}
            