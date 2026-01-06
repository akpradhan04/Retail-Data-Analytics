import snowflake.connector
import os


class SnowflakeConnection:
    def __init__(self, params: dict):
        self.params = params
        self.conn = snowflake.connector.connect(**self.params)
        self.cur = self.conn.cursor()
        print("Snowflake connection established")

    # -------------------- CONNECTION --------------------
    def close_connection(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("Snowflake connection closed")

    # -------------------- QUERY EXECUTION --------------------
    def run_query(self, query: str):
        self.cur.execute(query)

    def fetch_one(self, query: str):
        self.cur.execute(query)
        return self.cur.fetchone()

    def fetch_all(self, query: str):
        self.cur.execute(query)
        return self.cur.fetchall()

    def get_data(self, query: str):
        return self.fetch_all(query)

    # -------------------- DML OPERATIONS --------------------
    def truncate_table(self, table_name: str):
        query = f"TRUNCATE TABLE {self.params['database']}.{self.params['schema']}.{table_name}"
        print(query)
        self.cur.execute(query)

    # -------------------- LOAD FROM FILE --------------------

    def load_from_file(self, table_name: str, file_path: str, file_format: str):
        """
        Uploads a local file to the table stage and loads it into the table.

        file_format example: CSV_FORMAT
        Uses internal table stage: @%table_name
        """

        # Normalize path for Snowflake PUT
        abs_path = os.path.abspath(file_path).replace("\\", "/")

        put_query = f"""
            PUT file://{abs_path}/{table_name}.{file_format}
            @%{table_name}
            AUTO_COMPRESS = TRUE
            OVERWRITE = TRUE
        """
        print(put_query)

        copy_query = f"""
            COPY INTO {self.params['database']}.{self.params['schema']}.{table_name}
            FROM @%{table_name}
            FILE_FORMAT = (TYPE = '{file_format}'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1)
            PURGE = TRUE
        """

        print(copy_query)

        self.cur.execute(put_query)
        self.cur.execute(copy_query)

