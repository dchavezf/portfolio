import pathlib
import shutil
import re
import os

def uploadBitsoFiles(source_dir_name='data/upload', dest_dir_name='data/bitso'):
    """
    Busca archivos con el patrón 'bitso.com-{nombre}-{fecha}-{id}.csv'
    en la carpeta de origen y los mueve a 'bitso/{nombre}.csv'.
    """
    
    # Definición de las rutas usando pathlib
    SOURCE_DIR = pathlib.Path(source_dir_name)
    DEST_DIR = pathlib.Path(dest_dir_name)

    # 1. Crear la carpeta de destino si no existe
    try:
        DEST_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Directorio de destino '{dest_dir_name}' asegurado.")
    except OSError as e:
        print(f"Error al crear el directorio de destino {DEST_DIR}: {e}")
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
        try:
            shutil.move(file_path, dest_path)
            print(f"  [MOVIDO] '{original_filename}' -> '{dest_path}'")
            archivos_movidos += 1
        except Exception as e:
            print(f"  [FALLO] No se pudo mover '{original_filename}': {e}")

    print(f"\n--- Proceso completado. {archivos_movidos} archivos movidos exitosamente ---")


# --- Ejecución del script ---
if __name__ == "__main__":
    # NOTA: Para probar el script, debes crear manualmente la carpeta 'upload'
    # y colocar dentro archivos que sigan el patrón, ej:
    # upload/bitso.com-BTC-20241015-xyz.csv
    # upload/bitso.com-ETH-20241015-abc.csv
    uploadBitsoFiles()