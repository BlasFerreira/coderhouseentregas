# Importar bibliotecas
import psycopg2
import requests
import pandas as pd
from psycopg2.extras import execute_values

# Función para extraer datos
def extract_data():
    url = 'https://tradestie.com/api/v1/apps/reddit'
    headers = {"Accept-Encoding": "gzip, deflate"}

    response = requests.get(url, headers=headers)
    data = response.json()
    df = pd.DataFrame(data)
    return df

# Función para conectar a la base de datos
def connection_db():
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base = "data-engineer-database"
    user = "fblasfacundo_coderhouse"
    password = 'password'
    
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=password,
            port='5439'
        )
        print("Conectado a Redshift exitosamente!")
        return conn

    except Exception as e:
        print("No se puede conectar a Redshift.")
        print(e)

# Función para cargar datos en Redshift
def cargar_en_redshift(conn, table_name, dataframe):
    cur = conn.cursor()

    # Comprobar si la tabla ya existe
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    table_exists = cur.fetchone()[0]

    if not table_exists:
        # Obtener información sobre los tipos de datos
        dtypes = dataframe.dtypes
        cols = list(dtypes.index)
        tipos = list(dtypes.values)
        type_map = {'int64': 'INT', 'int32': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(50)', 'bool': 'BOOLEAN'}
        sql_dtypes = [type_map[str(dtype)] for dtype in tipos]

        # Definir formato SQL VARIABLE TIPO_DATO
        column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]

        # Combinar definiciones de columnas en la declaración CREATE TABLE
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_defs)}
            )
            SORTKEY (sentiment);
            """
        # Crear la tabla
        cur.execute(table_schema)

    # Generar los valores a insertar
    values = [tuple(x) for x in dataframe.to_numpy()]

    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"

    # Ejecutar la transacción para insertar los datos
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')

if __name__ == '__main__':
    # Extraer datos
    data = extract_data()

    # Conectar a la base de datos
    db_connection = connection_db()

    # Cargar datos en Redshift
    cargar_en_redshift(conn=db_connection, table_name='trade_data', dataframe=data)
