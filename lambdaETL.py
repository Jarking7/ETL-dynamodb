import json
import boto3
from collections import Counter
from botocore.exceptions import ClientError
import uuid
from datetime import datetime

# Crear clientes de DynamoDB y S3
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Parámetros de la tabla y bucket
    table_name = 'Peliculas_S3D2_xideral'  
    clasificacion_key = 'clasificacion'  
    titulo_key = 'nombre' 
    bucket_name = 'jafetethenaresults'  
    
    # Definir palabras clave para clasificación "terror"
    palabras_clave_terror = ['terror', 'miedo', 'horror', 'espanto', 'susto', 'sangre', 'fantasía', 'asesino', 'muerto', 'monstruo']
    
    try:
        # Realizamos un scan para obtener todos los ítems de la tabla
        response = dynamodb.scan(
            TableName=table_name
        )

        # Obtener los ítems de la respuesta
        items = response.get('Items', [])
        
        if not items:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No se encontraron ítems en la tabla'
                })
            }

        # Extraer las clasificaciones de los ítems
        clasificaciones = [item.get(clasificacion_key, {}).get('S', None) for item in items]

        # Contar las ocurrencias de cada clasificación
        clasificacion_count = Counter(clasificaciones)

        # Si no se encontraron clasificaciones
        if not clasificacion_count:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No se encontraron clasificaciones'
                })
            }

        # Encontrar la clasificación más repetida
        clasificacion_mas_repetida = clasificacion_count.most_common(1)[0]  # El más común

        # Clasificación de terror (basado en el título)
        peliculas_terror = []
        
        # Filtrar las películas de terror basadas en palabras clave en el título
        for item in items:
            titulo = item.get(titulo_key, {}).get('S', '').lower()  # Obtener el título y pasarlo a minúsculas
            
            # Comprobar si alguna de las palabras clave está en el título
            if any(palabra in titulo for palabra in palabras_clave_terror):
                peliculas_terror.append({
                    'titulo': titulo,
                    'clasificacion': 'terror'
                })
        
        # Si no se encontraron películas de terror, agregar un mensaje indicando eso
        if not peliculas_terror:
            peliculas_terror = [{
                'mensaje': 'No se encontraron películas de terror en los títulos.'
            }]

        # Estructurar el resultado
        result = {
            'clasificacion_mas_repetida': {
                'clasificacion': clasificacion_mas_repetida[0],
                'repeticiones': clasificacion_mas_repetida[1]
            },
            'peliculas_terror': peliculas_terror,
            'timestamp': datetime.utcnow().isoformat()  # Timestamp actual en formato ISO
        }

        # Generar un nombre único para el archivo usando un UUID
        file_name = f'clasificacion_{str(uuid.uuid4())}.json'

        # Subir el archivo a S3
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(result),  # Convertir el resultado a JSON
            ContentType='application/json'  # Especificamos que el contenido es JSON
        )

        # Responder con éxito
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Clasificación más repetida y películas de terror guardadas en S3',
                'file_name': file_name
            })
        }

    except ClientError as e:
        # Manejo de errores
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error al consultar DynamoDB o escribir en S3',
                'error': str(e)
            })
        }
