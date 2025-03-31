import os
import io
import glob
import json
import time
import requests

import numpy as np
import pandas as pd
import geopandas as gpd

import itertools as it

BASE_URL = 'https://onsc.senamhi.gob.bo/senamhiback'
BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
}

STATIONS_URL = BASE_URL + '/api/stations/datatable'
STATIONS_DATA = {
    'offset': 0,
    'limit': 2000,
    'order': None,
    'query': {
        'station': {'value': '', 'op': '*'}
    }
}

METEO_URL = BASE_URL + '/api/stations/exportxlsx'
METEO_DATA = {
    'data': 'diarios',
    'idStation': [],
    'idVariable': [1,2,3,4,5,6,7,8,9],
    'date': ['2000-01-01T04:00:00.000Z', '2012-01-01T04:00:00.000Z'],
    'multiDim': False
}

requests.packages.urllib3.disable_warnings()


def get_stations():
    req = requests.post(
        STATIONS_URL,
        json=STATIONS_DATA,
        headers=BASE_HEADERS,
        verify=False,
    )

    stations = req.json()['data']
    stations = pd.DataFrame(stations)
    stations = stations.set_index('id')

    stations['startDate'] = pd.to_datetime(stations['startDate'])
    stations = stations.sort_values('startDate')
    stations = stations[~stations['startDate'].isna()]

    return stations


def do_download_station_data(station_id, station, request_date):
    meteo_data = METEO_DATA.copy()
    meteo_data['idStation'] = [station_id]
    meteo_data['date'] = [
        (request_date - pd.DateOffset(years=1)).strftime('%Y-%m-%dT00:00:00.000Z'),
        request_date.strftime('%Y-%m-%dT23:59:59.000Z'),
    ]

    try:
        req = requests.post(
            METEO_URL,
            json=meteo_data,
            headers=BASE_HEADERS,
            timeout=90,
            verify=False,
        )
        assert(req.status_code == 200)
        return req.content

    except Exception as e:
        print(station_id)


def format_station_data(station_data):
    dt_df = station_data.rename(
        columns={
            'gestion': 'year',
            'mes': 'month',
            'dia': 'day'
        }
    )[['year', 'month', 'day']]
    dt_df = pd.to_datetime(dt_df)
    station_data['fecha'] = dt_df

    station_data = station_data[[
        'fecha',
        'estacion',
        '"Temperatura Máxima"',
        '"Temperatura Mínima"',
        '"Humedad Relativa Máxima"',
        '"Humedad Relativa Mínima"',
        'Precipitación',
    ]]
    station_data = station_data.set_index(['fecha', 'estacion'])

    station_data = station_data.astype(np.float64)
    station_data.loc[station_data['Precipitación'] == 0, 'Precipitación'] = np.nan

    station_data = station_data.dropna(how='all')
    station_data = station_data[~station_data.index.duplicated()]

    station_data.columns = [
        'temperatura_maxima',
        'temperatura_minima',
        'humedad_relativa_maxima',
        'humedad_relativa_minima',
        'precipitacion',
    ]
    station_data = station_data.sort_index()

    return station_data


def write_station_data(station_data):
    for data_date, data_df in station_data.groupby(pd.Grouper(level='fecha', freq='Y')):
        if data_date.year < 2023: # :S
            continue

        file_name = './data_historical/{}.csv'.format(data_date.year)

        if os.path.isfile(file_name):
            stored_df = pd.read_csv(
                file_name,
                index_col=['fecha', 'estacion'],
                parse_dates=['fecha']
            )

            data_df = pd.concat([stored_df, data_df])

        data_df = data_df[~data_df.index.duplicated()]
        data_df = data_df.sort_index()

        data_df.to_csv(file_name)

def download_stations_data(stations):
    request_date = pd.to_datetime('today')
    station_data = []

    for station_id, station in stations.iterrows():
        station = do_download_station_data(station_id, station, request_date)
        if not station:
            continue

        station = json.loads(station)
        if 'data' in station and len(station['data']):
            station_data.append(station['data'])

        time.sleep(.1)

    station_data = pd.DataFrame(list(it.chain(*station_data)))
    station_data = format_station_data(station_data)

    return station_data


if __name__ == '__main__':
    stations = get_stations()
    station_data = download_stations_data(stations)
    write_station_data(station_data)
