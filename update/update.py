import io
import os
import requests

import numpy as np
import pandas as pd
import geopandas as gpd

QUERIES = {
    1: 'precipitacion',
    2: 'temperatura',
    6: 'humedad_relativa',
    14: 'presion',
    15: 'viento_direccion',
    16: 'viento_velocidad',
}
QUERY_DATA = {
    'date': [],
    'idVariable': 0,
    'table': 'horario',
    'name': 'variable'
}
QUERY_URL = 'https://onsc.senamhi.gob.bo/senamhiback/api/generateshp'
QUERY_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://onsc.senamhi.gob.bo/public/public-geoanalisys',
    'Origin': 'https://onsc.senamhi.gob.bo',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)',
    'Connection': 'close'
}
QUERY_TIMEOUT = 180
QUERY_TIMEDIFF = 3
DOWNLOAD_URL = 'https://onsc.senamhi.gob.bo/senamhiback/shp/variable.zip'
DF_COLUMNS = [
    'fecha',
    'estacion',
    'viento_direccion',
    'viento_velocidad',
    'temperatura',
    'humedad_relativa',
    'precipitacion',
    'presion',
]


def download_latest():
    time_to = pd.to_datetime('now')
    time_from = time_to - pd.Timedelta(days=QUERY_TIMEDIFF)

    QUERY_DATA['date'] = [
        time_from.strftime('%Y-%m-%dT%H:%M:%SZ'),
        time_to.strftime('%Y-%m-%dT%H:%M:%SZ'),
    ]

    update_df = []
    for query_variable in QUERIES.keys():
        QUERY_DATA['idVariable'] = query_variable

        req = requests.post(
            QUERY_URL,
            json=QUERY_DATA,
            headers=QUERY_HEADERS,
            timeout=QUERY_TIMEOUT
        )
        print(req.content)

        req = req.json()
        if 'succes' not in req or not req['succes']:
            continue

        req = requests.get(DOWNLOAD_URL, timeout=QUERY_TIMEOUT)

        req_io = io.BytesIO(req.content)
        req_df = gpd.read_file(req_io)

        req_df = req_df.drop(columns=['geometry', 'ACTIVO'])

        req_df['TIME'] = pd.to_datetime(req_df['TIME'])
        req_df['QUERY_VARIABLE'] = QUERIES[query_variable]

        update_df.append(req_df)

    return pd.concat(update_df)


def format_df(update_df):
    update_df = update_df.set_index([
        'ESTACION', 'TIME', 'QUERY_VARIABLE'
    ])['VALOR_VARI']

    update_df = update_df[
        ~update_df.index.get_level_values(0).isna()
    ]
    update_df = update_df[
        ~update_df.index.duplicated()
    ]

    update_df = update_df.str[1:-1].astype(np.float64)
    update_df = update_df.round(2).unstack('QUERY_VARIABLE')

    update_df = update_df.reset_index()
    update_df = update_df.rename(columns={
        'ESTACION': 'estacion',
        'TIME': 'fecha'
    })
    update_df = update_df[DF_COLUMNS]
    update_df = update_df.sort_values(['fecha', 'estacion'])

    return update_df


def merge_df(update_df):
    update_groups = update_df.groupby(
        pd.Grouper(key='fecha', freq='M')
    )

    for timestamp, month_update_df in update_groups:
        file_name = './data/{}.csv'.format(
            timestamp.strftime('%Y%m')
        )

        month_update_df = month_update_df.set_index([
            'fecha', 'estacion'
        ])

        if os.path.isfile(file_name):
            stored_month_update_df = pd.read_csv(file_name)
            stored_month_update_df['fecha'] = pd.to_datetime(
                stored_month_update_df['fecha']
            )
            stored_month_update_df = stored_month_update_df.set_index([
                'fecha', 'estacion'
            ])

            month_update_df = pd.concat([
                stored_month_update_df, month_update_df
            ])
            month_update_df = month_update_df[
                ~month_update_df.index.duplicated()
            ]

            month_update_df = month_update_df.sort_index()

        month_update_df.to_csv(file_name)


if __name__ == '__main__':
    update_df = download_latest()
    update_df = format_df(update_df)

    merge_df(update_df)
