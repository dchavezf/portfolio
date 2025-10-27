import duckdb
import pandas as pd
from typing import List, Tuple

class DbClass:
    def __init__(self, connstr: str ):
        self.connstr =connstr
    
    def connect(self):
        """
        Establishes a connection to the DuckDB database.
        """
        return duckdb.connect(self.connstr)

    def execute_script(self, script_path: str):
        """
        Reads and executes a SQL script file.

        Args:
            script_path (str): The path to the SQL script file.
        """

        with open(script_path, "r") as f:
            sql_script = f.read()
            result=self.execute_sql(sql_script)
        return result

    def execute_sql(self, sql_script: str):
        """
        Reads and executes a SQL script file.

        Args:
            script_path (str): The path to the SQL script file.
        """
        with self.connect() as con:
            return con.execute(sql_script).fetchdf()

    def register_dataframe(self, df: pd.DataFrame, view_name: str):
        """
        Registers a pandas DataFrame as a DuckDB view.

        Args:
            df (pd.DataFrame): The DataFrame to register.
            view_name (str): The name of the view to create in DuckDB.
        """
        with self.connect() as con:
            con.register(view_name, df)

    def fetch_dataframe(self, query: str, dataframes: List[Tuple[ str,pd.DataFrame]] | None = None) -> pd.DataFrame:
        if dataframes is None:
            dataframes = []
        """
        Executes a SQL query and returns the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: The result of the query.
        """
        with self.connect() as con:
            for df in dataframes:
                con.register(df[0], df[1])  
            return con.execute(query).df()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Inserts a pandas DataFrame into a DuckDB table.

        Args:
            df (pd.DataFrame): The DataFrame to insert.
            table_name (str): The name of the table to insert into.
        """
        with self.connect() as con:
            con.register("df", df)
            nombres_columnas_lista = df.columns.tolist()

            # 2. Unir la lista usando ", " como separador
            columnas_str = ", ".join(nombres_columnas_lista)
            sql=f"INSERT INTO {table_name} ({columnas_str}) SELECT {columnas_str} FROM df"
            con.execute(sql)
            #result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            #print(f"Registros insertados: {result[0]} en tabla {table_name}")
            
    def create_table_sql(self,table_name: str, dtype_map: dict) -> str:
        """
        Genera una instrucción SQL CREATE TABLE para DuckDB a partir de un mapa de tipos de datos de Pandas/Python.

        Args:
            nombre_tabla (str): El nombre que tendrá la tabla en DuckDB.
            dtype_map (dict): Un diccionario donde la clave es el nombre de la columna 
                            y el valor es el tipo de dato de Pandas/Python.

        Returns:
            str: La instrucción SQL CREATE TABLE lista para ser ejecutada.
        """
        
        # Mapeo de tipos de Pandas/Python a tipos de DuckDB
        # DuckDB utiliza SQL Standard types que son muy parecidos a los de Postgres.
        duckdb_type_map = {
            'str': 'VARCHAR',                 # Para cadenas de texto
            'object': 'VARCHAR',              # Alias para str
            'datetime64[ns]': 'TIMESTAMP',    # Para fechas y horas con precisión
            'float64': 'DOUBLE',              # Para números decimales (doble precisión)
            'int64': 'BIGINT',                # Para enteros grandes
            # Puedes añadir más mapeos aquí si los necesitas
        }

        column_definitions = []
        for columna, pd_dtype in dtype_map.items():
            # Obtener el tipo de DuckDB, si no se encuentra, usamos VARCHAR como fallback
            duckdb_dtype = duckdb_type_map.get(pd_dtype, 'VARCHAR')
            
            # Formatear la definición de la columna (nombre_columna TIPO_DUCKDB)
            column_definitions.append(f"{columna} {duckdb_dtype}")

        # Unir todas las definiciones de columnas con comas
        column_sql = ',\n    '.join(column_definitions)

        # Construir la instrucción CREATE TABLE final
        sql_instruction = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_sql}
            );
        """
        return sql_instruction

    
def update_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Updates existing records in a DuckDB table from a pandas DataFrame.
        Assumes the DataFrame has a 'fecha' column for matching.

        Args:
            df (pd.DataFrame): The DataFrame containing data to update.
            table_name (str): The name of the table to update.
        """
        if df.empty:
            print(f"No hay datos para actualizar en la tabla '{table_name}'.")
            return

        with self.connect() as con:
            # Register the DataFrame as a temporary view
            con.register("df_temp_update", df)

            # Construct the SET clause dynamically based on DataFrame columns
            # Exclude 'fecha' from the SET clause as it's used in the WHERE clause
            set_clauses = [f"{col} = df_temp_update.{col}" for col in df.columns if col != 'fecha']
            set_clause_str = ", ".join(set_clauses)

            if not set_clause_str:
                print(f"Advertencia: No hay columnas para actualizar en la tabla '{table_name}' (excluyendo 'fecha').")
                return

            update_query = f"""
                UPDATE {table_name}
                SET {set_clause_str}
                FROM df_temp_update
                WHERE {table_name}.fecha = df_temp_update.fecha;
            """
            con.execute(update_query)
            print(f"{len(df)} registros actualizados en la tabla '{table_name}' ✅")
