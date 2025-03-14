import os
import duckdb
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# Cargar variables de entorno (útil para desarrollo local)
load_dotenv()

# Obtener las variables de entorno
account = os.getenv("AZURE_STORAGE_ACCOUNT")
key = os.getenv("AZURE_STORAGE_KEY")
if not account or not key:
    raise ValueError("Falta AZURE_STORAGE_ACCOUNT o AZURE_STORAGE_KEY en el entorno.")

# Construir la cadena de conexión para Azure Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={account};"
    f"AccountKey={key};"
    f"EndpointSuffix=core.windows.net"
)

# Registrar la aplicación de Azure Functions
app = func.FunctionApp()

@app.function_name(name="RunDuckDBQuery")
@app.route(route="RunDuckDBQuery")  # La función estará disponible en /api/RunDuckDBQuery
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Conectar a DuckDB
        conn = duckdb.connect()

        # Instalar y cargar la extensión 'azure'
        conn.execute("INSTALL azure;")
        conn.execute("LOAD azure;")
        
        # Configurar la conexión a Azure en DuckDB usando la connection string construida
        conn.execute(f"SET azure_storage_connection_string='{connection_string}';")
        
        # Especificar el file system y el archivo Parquet
        file_system_name = "testkaizen2"
        file_path = "embeddings_ecom.parquet"
        
        # Consulta SQL utilizando la extensión azure en DuckDB
        query = f"""
        SELECT DISTINCT(lower(brand)) AS brand, COUNT(*) AS count
        FROM read_parquet('azure://{file_system_name}/{file_path}')
        GROUP BY brand
        ORDER BY count DESC
        LIMIT 10
        """
        
        # Ejecutar la consulta y obtener el resultado en un DataFrame
        df = conn.query(query).to_df()
        
        # Convertir el DataFrame a JSON para la respuesta HTTP
        result_json = df.to_json(orient="records")
        
        return func.HttpResponse(result_json, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
