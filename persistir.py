import os
import urllib.request as urllib
import json
import pandas as pd
from datetime import datetime
from time import sleep
import pytz


argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')

def consultar_y_persistir():
    url_datos = 'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation?client_id='+os.environ['ECOBICIID']+'&client_secret='+os.environ['ECOBICISECRET']
    response = urllib.urlopen(url_datos)
    datos = json.loads(response.read())
    estaciones = pd.DataFrame(datos['data']['stations'])
    estaciones = estaciones.reindex(columns=['station_id', 'name', 'capacity', 'lat', 'lon'])

    url_consulta = 'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus?client_id='+os.environ['ECOBICIID']+'&client_secret='+os.environ['ECOBICISECRET']
    argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
    tiempo_consulta = datetime.now(tz=argentina_tz)
    response = urllib.urlopen(url_consulta)
    status = json.loads(response.read())
    dt = pd.DataFrame(status['data']['stations'])
    dt = dt.merge(estaciones, how='inner', on='station_id')
    dt['timestamp_consulta'] = tiempo_consulta
    dt.to_csv('data/'+str(tiempo_consulta)[:16]+'.csv')

no_es_medianoche = True
while no_es_medianoche:
    if (datetime.now(tz=argentina_tz).hour==0) and (datetime.now(tz=argentina_tz).minute==0):
        no_es_medianoche = False
    print("waiting",datetime.now(tz=argentina_tz))
    sleep(60*1)

for i in range(48):
    sleep(60*30)
    print(i)
    consultar_y_persistir()


