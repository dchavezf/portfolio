from dbclass import DbClass 
from banxico import Banxico
from coinapi import CoinAPI
from helper import log
import os
import datetime
import pathlib
import shutil
import zipfile

def uploadBitsoFiles(source_dir_name='data/upload', dest_dir_name='data/bitso'):
    """
    Busca archivos con el patrón 'bitso.com-{nombre}-{fecha}-{id}.csv'
    en la carpeta de origen y los mueve a 'bitso/{nombre}.csv'.
    """    
    # Definición de las rutas usando pathlib
    SOURCE_DIR = pathlib.Path(source_dir_name)
    DEST_DIR = pathlib.Path(dest_dir_name)
    BACKUP_DIR = SOURCE_DIR / "backup"

    # 1. Crear la carpeta de destino si no existe
    # 1. Crear las carpetas de destino y respaldo si no existen
    try:
        DEST_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Directorio de destino '{dest_dir_name}' asegurado.")
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Directorio de respaldo '{BACKUP_DIR}' asegurado.")
    except OSError as e:
        print(f"Error al crear directorios: {e}")
        return

    # 2. Iterar sobre los archivos en la carpeta de origen
    # Usamos glob para encontrar archivos que comienzan con 'bitso.com-' y terminan en '.csv'
    patron_busqueda = "bitso.com-*.csv"
    
    archivos_encontrados = list(SOURCE_DIR.glob(patron_busqueda))
    
    if not archivos_encontrados:
        print(f"No se encontraron archivos que coincidan con el patrón '{patron_busqueda}' en '{source_dir_name}/'.")
        return

    print(f"\n--- Procesando {len(archivos_encontrados)} archivos ---")
    
    archivos_movidos = 0
    archivos_respaldados = [] # Lista para guardar las rutas de los archivos respaldados

    for file_path in archivos_encontrados:
        original_filename = file_path.name
        
        # 3. Extraer el componente {nombre}
        try:
            # Ejemplo de nombre: 'bitso.com-BTC-20250101-123.csv'
            # file_path.stem quita la extensión .csv -> 'bitso.com-BTC-20250101-123'
            # Al dividir por '-', el nombre es el segundo elemento (índice 1)
            parts = file_path.stem.split('-')
            
            # Verificación básica del formato esperado
            if len(parts) < 3 or parts[0] != 'bitso.com':
                print(f"  [OMITIDO] '{original_filename}' no sigue el formato esperado. Ignorando.")
                continue

            nombre_crypto = parts[1]
            
        except Exception:
            print(f"  [ERROR] No se pudo parsear el nombre '{original_filename}'. Ignorando.")
            continue

        # 4. Construir la ruta de destino
        new_filename = f"{nombre_crypto.lower()}.csv" # Convertir a minúsculas por convención
        dest_path = DEST_DIR / new_filename

        # 5. Mover y renombrar el archivo
        # 5. Respaldar y luego mover el archivo
        try:
            backup_path = BACKUP_DIR / original_filename
            shutil.copy2(file_path, backup_path) # copy2 preserva metadatos
            print(f"  [RESPALDADO] '{original_filename}' -> '{backup_path}'")
            archivos_respaldados.append(backup_path) # Añadir a la lista para zipear

            shutil.move(file_path, dest_path)
            print(f"  [MOVIDO] '{original_filename}' -> '{dest_path}'")
            archivos_movidos += 1
        except Exception as e:
            print(f"  [FALLO] No se pudo mover '{original_filename}': {e}")

    # 6. Añadir data/transaction.csv al respaldo si existe
    transaction_csv_path = pathlib.Path("data/transaction.csv")
    if transaction_csv_path.exists():
        try:
            backup_path = BACKUP_DIR / transaction_csv_path.name
            shutil.copy2(transaction_csv_path, backup_path)
            print(f"  [RESPALDADO] '{transaction_csv_path.name}' -> '{backup_path}'")
            archivos_respaldados.append(backup_path) # Añadir a la lista para zipear
        except Exception as e:
            print(f"  [FALLO] No se pudo respaldar '{transaction_csv_path.name}': {e}")

    # 6. Comprimir los archivos respaldados y eliminarlos
    if archivos_respaldados:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        zip_filename = BACKUP_DIR / f"bitso_backup_{timestamp}.zip"
        print(f"\n--- Creando archivo ZIP: '{zip_filename}' ---")
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_to_zip in archivos_respaldados:
                # Añadir archivo al zip usando solo su nombre
                zipf.write(file_to_zip, file_to_zip.name)
                # Eliminar el archivo original del respaldo
                os.remove(file_to_zip)
        print(f"Archivos de respaldo comprimidos y eliminados.")

    print(f"\n--- Proceso completado. {archivos_movidos} archivos movidos exitosamente ---")

def profit_analysis(db:DbClass):
    """Análisis de ganancias y pérdidas por transacción"""
    sql="""
    SELECT * from xr_transactions_valuated_view"""

try:
    #uploadBitsoFiles()
    log("main","Conectandose a base de datos")    
    db = DbClass("data/portfolio/portfolio.db")

    path="data/sql"
    log("main","Creando tablas")
    db.execute_script(f"{path}/schema.sql")
    
    log("main","Datacleansing")
    db.execute_script(f"{path}/datacleansing.sql")    

    log("main","Cargando tablas")
    db.execute_script(f"{path}/load.sql")

    log("main","Tipos de cambio MXN/UDI")
    banxico=Banxico(db)
    sql= """
        SELECT DISTINCT 
            CAST(DATE(t.datetime) AS TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'UDI' as from_currency
        FROM xr_transactions_view t
        LEFT JOIN XR_Prices b
            ON DATE(t.datetime) = b.target_time
            and b.to_currency='MXN'
            and b.from_currency='UDI'
        WHERE 
            b.price IS NULL
        UNION
        SELECT DISTINCT 
            CAST(DATE(t.datetime) AS TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'USD' as from_currency
        FROM xr_transactions_view t
        LEFT JOIN XR_Prices b
            ON DATE(t.datetime) = b.target_time
            and b.to_currency='MXN'
            and b.from_currency='USD'
        WHERE 
            b.price IS NULL
        UNION
        SELECT  
            cast(cURRENT_DATE as TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'USD' AS from_currency
        UNION
        SELECT  
            cast(cURRENT_DATE as TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'UDI' AS from_currency
        ;
    """
    banxico.update_in_batch(sql)
    log("main","Ajusto Tipos de Cambio MXN/UDI")
    db.execute_script(f"{path}/transform_1.sql")

    log("main","Tipos de cambio USD")
    coinapi=CoinAPI(db)

    sql= """
        SELECT DISTINCT 
            t.datetime AS datetime,
            'USD' AS to_currency,
            currency AS from_currency
        FROM xr_transactions t 
        WHERE
            COALESCE(xr_usd,0) <=0
        UNION
        SELECT DISTINCT 
            CAST(DATE(CURRENT_DATE) AS TIMESTAMPTZ) AS datetime,
            'USD' AS to_currency,
            from_xr_currency AS from_currency
        FROM xr_transactions t 
        ;
    """
    coinapi.update_in_batch(sql)
    log("main","Ajusto Tipos de Cambio USD")
    db.execute_script(f"{path}/transform_2.sql")
    
    sql="""
        select *
        from xr_transactions_valuated_view t
        ;
    """
    # Backup output file if it exists
    output_file = "data/transaction-output.csv"
    df=db.fetch_dataframe(sql)
    # Salva dataframe en csv file
    df.to_csv(output_file, index=False)
except Exception as e:
    log("main",f"Error: {e}",True)
log("main","Listo")
"""
TODO
- generacion de archivo de salida
- integracion de carga de archivos
"""