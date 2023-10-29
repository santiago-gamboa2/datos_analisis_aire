import pandas as pd
import requests
import json

# Descargar los datos demográficos y guardarlos en "datos.csv"
url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
data = pd.read_csv(url, sep=';')
data.to_csv("datos.csv", index=False)

# Limpieza de datos demográficos
data = data.drop(['Race', 'Count', 'Number of Veterans'], axis=1)
data = data.drop_duplicates()

# Definir la URL de la API de calidad del aire
airquality_api_url = "https://api.api-ninjas.com/v1/airquality"

# Tu clave de API
api_key = 'IWs6R9DlBbWUAYqWAxSI91gqGY8wYcHODvddTRrN'

# Crear una nueva columna en el DataFrame para almacenar los datos de calidad del aire
data['air_quality_concentration'] = None

# Iterar a través de las filas de la tabla demográfica y obtener los datos de calidad del aire
to_drop = []  # Lista para almacenar índices de filas que deben eliminarse

for index, row in data.iterrows():
    city = row['City']
    
    # Realizar una solicitud a la API para obtener los datos de calidad del aire
    response = requests.get(f"{airquality_api_url}?city={city}", headers={'X-Api-Key': api_key})

    if response.status_code == 200:
        try:
            air_quality_data = response.json()
            # Extraer el elemento "concentration" de los datos y almacenarlo en el DataFrame
            data.at[index, 'air_quality_concentration'] = air_quality_data.get('concentration')
        except json.JSONDecodeError as e:
            print(f"Error al analizar JSON para {city}: {e}")
            to_drop.append(index)
    else:
        print(f"Error al obtener datos para {city}")
        to_drop.append(index)

# Eliminar las filas con errores
data = data.drop(to_drop)

# Guardar el DataFrame actualizado en un archivo CSV
data.to_csv("datos_con_calidad_aire.csv", index=False)
