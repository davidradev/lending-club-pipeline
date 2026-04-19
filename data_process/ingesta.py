import os
import psycopg2
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse

# Cargamos las variables de entorno
load_dotenv()

def ingest_raw_data():
    sas_url = os.getenv("AZURE_STORAGE_SAS_URL")
    db_host = os.getenv("DB_HOST")
    
    if not sas_url or not db_host:
        print("Error: No se pudieron cargar las variables del .env")
        return

    parsed_url = urlparse(sas_url)
    account_name = parsed_url.netloc.split('.')[0]
    sas_token = parsed_url.query
    base_url = sas_url.split('?')[0]

    try:
        # 1. Obtener los nombres de las columnas reales del CSV en Azure
        print("Analizando estructura del CSV en Azure...")
        r = requests.get(sas_url, headers={'Range': 'bytes=0-4096'}) # Leemos solo el inicio del archivo
        first_line = r.text.split('\n')[0]
        actual_columns = [c.strip() for c in first_line.split(',')]
        print(f"Detectadas {len(actual_columns)} columnas en el archivo original.")

        # 2. Conectar a la base de datos
        print(f"Conectando a {db_host}...")
        conn = psycopg2.connect(
            host=db_host,
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            sslmode='require'
        )
        conn.autocommit = True
        cur = conn.cursor()

        # 3. Habilitar extension y credenciales
        cur.execute("CREATE EXTENSION IF NOT EXISTS azure_storage;")
        cur.execute(f"SELECT azure_storage.account_add('{account_name}', '{sas_token}');")

        # 4. Crear tabla temporal con TODAS las columnas
        print("Creando tabla temporal para la carga masiva...")
        cur.execute("DROP TABLE IF EXISTS raw_loans_temp;")
        
        # Generamos dinamicamente los nombres de columnas para la tabla temporal
        # Si el CSV tiene un indice sin nombre al inicio, lo llamamos 'idx'
        cols_definition = []
        for i, col in enumerate(actual_columns):
            col_name = col if col and not col.isspace() else f"col_{i}"
            # Limpiamos nombres de columnas problematicos
            col_name = col_name.replace(' ', '_').replace(':', '').replace('-', '_').replace('.', '_')
            cols_definition.append(f'"{col_name}" TEXT')
            
        cur.execute(f"CREATE TABLE raw_loans_temp ({', '.join(cols_definition)});")

        # 5. Carga masiva a la tabla temporal
        print("Iniciando transferencia masiva a tabla temporal (esto tomara unos minutos)...")
        copy_sql = f"COPY raw_loans_temp FROM '{base_url}' WITH (FORMAT csv, HEADER true, DELIMITER ',');"
        cur.execute(copy_sql)

        # 6. Seleccionamos solo lo que dbt necesita para la tabla raw_loans final
        print("Estructurando tabla raw_loans final...")
        cur.execute("DROP TABLE IF EXISTS raw_loans;")
        
        # Mapeamos las columnas clave. Nota: Segun el error, la primera columna es un indice (0, 1, 2...)
        # Ajustamos los nombres segun lo que vimos en el error
        cur.execute("""
            CREATE TABLE raw_loans AS 
            SELECT 
                id, loan_amnt, term, int_rate, installment, grade, 
                emp_length, home_ownership, annual_inc, verification_status, 
                issue_d, loan_status, purpose, dti, revol_util, 
                mort_acc, pub_rec_bankruptcies, zip_code
            FROM raw_loans_temp;
        """)

        print("Limpiando tablas temporales...")
        cur.execute("DROP TABLE IF EXISTS raw_loans_temp;")

        print("¡Exito total! Los datos filtrados ya estan en Azure PostgreSQL listos para dbt.")
        
    except Exception as e:
        print(f"Error fatal durante la ingesta: {e}")
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

if __name__ == "__main__":
    ingest_raw_data()
