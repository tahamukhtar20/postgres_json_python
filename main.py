import psycopg2
import json
from decimal import Decimal


class PostgresJsonConvertor:
    # Dunder Methods
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.SUPPORTED_COMMANDS = {
            'UPDATE': lambda cursor: self.other_queries(),
            'CREATE': lambda cursor: self.other_queries(),
            'INSERT': lambda cursor: self.other_queries(),
            'DELETE': lambda cursor: self.other_queries(),
            'DROP': lambda cursor: self.other_queries(),
            'SELECT': lambda cursor: self.select_query(cursor),

        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # Main Query Execution function
    def execute_query(self, query: str):
        with self.connection, self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
                command = query.upper().split()[0]
                if command in self.SUPPORTED_COMMANDS:
                    return self.SUPPORTED_COMMANDS[command](cursor)
                return self.unsupported_command()
            except psycopg2.Error as e:
                return json.dumps({"status_code": 500, "error_message": str(e)})

    # Helper Functions

    @staticmethod
    def decimal_conversion_to_python_datatypes(value):
        if isinstance(value, Decimal):
            if value % 1 == 0:
                return int(value)
            return float(value)
        else:
            return value

    @staticmethod
    def other_queries():
        return json.dumps({"status_code": 200, "message": "Query executed successfully."})

    @staticmethod
    def unsupported_command():
        return json.dumps({"status_code": 400, "message": "Unsupported SQL command."})

    def select_query(self, cursor):
        columns = [desc[0] for desc in cursor.description]
        if cursor.description is not None and (json_results := self.create_json(cursor, columns)):
            return json.dumps({"status_code": 200, "data": json_results})
        return json.dumps({"status_code": 204, "message": "No data found."})

    def create_json(self, cursor, columns):
        if results := cursor.fetchall():
            return self.create_json_helper(results, columns)
        return []

    def create_json_helper(self, results, columns):
        json_results = []
        for row in results:
            json_row = {}
            for i, column_name in enumerate(columns):
                json_row[column_name] = self.decimal_conversion_to_python_datatypes(row[i])
            json_results.append(json_row)
        return json_results

    # Connection/Disconnection Functions
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except psycopg2.Error as e:
            raise ConnectionError(f"Error connecting to the database: {e}") from e

    def close(self):
        if self.connection is not None:
            self.connection.close()
