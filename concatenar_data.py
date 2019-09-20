import pandas as pd
import os
from datetime import datetime, timedelta
import pytz
import numpy as np


def tiempo_desde_ultimo_reporte(dt):
    consulta = pd.to_datetime(
        dt.timestamp_consulta.iloc[0]).replace(tzinfo=None)
    dt['ult_reporte'] = dt.last_reported.map(datetime.fromtimestamp)
    dt['delta_ultimo_reporte'] = (
        (consulta - dt.ult_reporte) / np.timedelta64(1, 'm')).astype(int)
    return dt


fechas = os.listdir('data/desagregada/')

for fecha in fechas:
    archivos = os.listdir('data/desagregada/' + fecha)
    archivos

    concatenada = pd.concat([tiempo_desde_ultimo_reporte(pd.read_csv(
        'data/desagregada/' + fecha + '/' + archivo)) for archivo in archivos])
    concatenada = concatenada.drop('Unnamed: 0', axis=1)
    concatenada['sospechosa'] = False
    concatenada.loc[(concatenada.is_returning == 1) & (
        concatenada.delta_ultimo_reporte > 10), 'sospechosa'] = True

    concatenada.to_csv('data/agregada/un_dia_ecobici_' +
                       fecha + '.csv', index=False)

    col_interes = ['timestamp_consulta', 'num_bikes_available', 'num_bikes_disabled',
                   'num_docks_available', 'num_docks_disabled', 'sospechosa']

    agregada = concatenada.reindex(
        columns=col_interes).groupby('timestamp_consulta').sum()
    maximos = agregada.loc[agregada.num_bikes_available.idxmax()]

    agregada['max_bicis_available'] = maximos.num_bikes_available
    agregada['max_bicis_sistema'] = maximos.num_bikes_available + \
        maximos.num_bikes_disabled

    agregada.to_csv('data/agregada/un_dia_ecobici_' + fecha + '_agregada.csv')
