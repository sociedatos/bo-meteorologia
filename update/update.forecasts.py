import io
import os
import unidecode
import requests

import numpy as np
import pandas as pd

BASE_URL = 'https://senamhi.gob.bo'
BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
}

STATIONS_URL = BASE_URL + '/pronjsondiario.php'
FORECAST_URL = BASE_URL + '/pronosticoJson.php'


###############################################################################
# update
###############################################################################

def get_stations():
    req = requests.get(
        STATIONS_URL,
        headers=BASE_HEADERS
    )

    stations = req.json()
    stations = pd.DataFrame(stations)
    stations = stations[[
        'estacion', 'departamento', 'provincia',
        'municipio', 'latitud', 'long'
    ]].rename(columns={'long': 'longitud'}, errors='ignore')

    return stations


def get_station_forecast(station_name, now):
    req = requests.get(
        FORECAST_URL,
        params={'ciudad': station_name},
        headers=BASE_HEADERS
    )

    station_forecast = pd.DataFrame({__:___ for _ in req.json() for __, ___ in _.items()})
    station_forecast = station_forecast.drop(columns=['Imagen', 'Fenomeno', 'vi'], errors='ignore')

    station_forecast.columns = station_forecast.columns.map(
        lambda _: unidecode.unidecode(_).lower().replace(' ', '_')
    )
    station_forecast['fecha'] = pd.to_datetime(station_forecast['fecha'])

    station_forecast['fecha_diff'] = (station_forecast['fecha'] - now).dt.total_seconds() / 3600
    station_forecast = station_forecast[station_forecast['fecha_diff'] >= 1]
    station_forecast['fecha_diff'] = station_forecast['fecha_diff'].astype(int)

    station_forecast['fecha'] = now
    station_forecast['estacion'] = station_name

    station_forecast = station_forecast.set_index(['estacion', 'fecha', 'fecha_diff'])

    return station_forecast


def get_forecasts(stations, now):
    station_forecasts = []

    for station_name in stations['estacion'].values:
        try:
            station_forecast = get_station_forecast(station_name, now)
            station_forecasts.append(station_forecast)
        except:
            continue

    station_forecasts = pd.concat(station_forecasts)
    return station_forecasts


###############################################################################
# store
###############################################################################

def do_process_for_storage(df_t):
    df_t = df_t.groupby(['fecha', 'fecha_diff']).apply(
        lambda _: _['valor']
    ).droplevel(2)
    df_t = df_t[~df_t.index.duplicated()].unstack(level=1).ffill()

    df_t = df_t.apply(
        lambda _: _[~_.duplicated(keep='first')]
    ).dropna(how='all').stack()

    return df_t


def process_for_storage(df):
    df_f = df.set_index(['estacion', 'fecha', 'fecha_diff']).stack()
    df_f.index.names = [*df_f.index.names][:-1] + ['variable']

    df_f = df_f.rename('valor').reset_index()

    df_fx = df_f.groupby(['estacion', 'variable']).apply(do_process_for_storage)
    df_fx = df_fx.unstack(level='variable')

    df_fx = df_fx.reset_index()

    df_fx['fecha_diff'] = (df_fx['fecha_diff'] - df_fx['fecha']).dt.total_seconds() / 3600
    df_fx['fecha_diff'] = df_fx['fecha_diff'].astype(int)

    df_fx = df_fx.reset_index(drop=True)

    return df_fx


def update_store(station_forecasts, now):
    fn = './data_forecast/{}/{}.csv'.format(
        now.year, now.strftime('%Y%W')
    )

    station_forecasts = station_forecasts.reset_index()

    stored_forecasts = pd.DataFrame([])
    if not os.path.isfile(fn):
        station_forecasts.to_csv(fn, index=False)
        return

    stored_forecasts = pd.read_csv(fn)
    stored_forecasts['fecha'] = pd.to_datetime(stored_forecasts['fecha'])
    stored_forecasts = pd.concat([stored_forecasts, station_forecasts])

    stored_forecasts['fecha_diff'] = (
        stored_forecasts['fecha'] + stored_forecasts['fecha_diff'].apply(lambda _: pd.Timedelta(hours=_))
    )

    stored_forecasts = process_for_storage(stored_forecasts)
    stored_forecasts.to_csv(fn, index=False)


###############################################################################
# run
###############################################################################

if __name__ == '__main__':
    now = pd.to_datetime('now', utc=True)
    now = now.tz_convert("Etc/GMT+4").tz_localize(None).floor('h')

    stations = get_stations()
    station_forecasts = get_forecasts(stations, now)

    update_store(station_forecasts, now)
