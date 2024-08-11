import requests
import pyarrow.dataset as ds
import pyarrow.feather as feather
import pandas as pd

from copy import deepcopy
from datetime import datetime
from dateutil.parser import parse
from datetime import timedelta
from pyarrow import Table
from pathlib import Path

from tlib.data.constants import SPOT_DATA_PATH
from tlib.lib import get_int_dt, get_int_tm



current_year = 2021
ticker = 'nifty'

index_data = ds.dataset(SPOT_DATA_PATH / ticker / str(current_year) / 'table.feather', format='feather')
index_data = index_data.to_table()
index_df = index_data.to_pandas()

index_dict = index_df.to_dict()

kaggle_data = dict()
for i, date_time in index_dict['date_time'].items():
    row = dict()
    date_time = date_time.to_pydatetime()
    dt = get_int_dt(date_time)
    tm = get_int_tm(date_time)
    row['date_time'] = date_time
    row['open'] = index_dict['open'][i]
    row['high'] = index_dict['high'][i]
    row['low']  = index_dict['low'][i]
    row['close']= index_dict['close'][i]
    row['dt']   = get_int_dt(date_time)
    row['tm']   = get_int_tm(date_time)

    dt_dict = kaggle_data.setdefault(dt, dict())
    dt_dict[tm] = row





from_dt = datetime(current_year, 1, 1)
fetch_from = from_dt


raw_data = []

zerodha_ids = {'nifty': 256265, 'banknifty': 260105}

url = f'https://kite.zerodha.com/oms/instruments/historical/{zerodha_ids[ticker]}/minute?user_id=YP9593&oi=1'
headers = {
    'cookie': '_ga=GA1.2.2108418404.1644552367; kf_session=Y1JBRHJTyC5mXO18uExhWACwlk16EJnO; user_id=YP9593; _gid=GA1.2.450995624.1645576804; public_token=tj0G93DSWpiuLxtgFxkqIxTJXp5fGlRs; enctoken=hM5YIiX8GgNf2XybCRZvrHE5xgMwg3UmVvFBRJRvdvecg14JazcLhGc3X/aLh76/YbUC0LR3rtdvTblZTWDNIeKDKMAojZbferXDfoI2UoPExFVwhrM2IQ==',
    'authorization': 'enctoken hM5YIiX8GgNf2XybCRZvrHE5xgMwg3UmVvFBRJRvdvecg14JazcLhGc3X/aLh76/YbUC0LR3rtdvTblZTWDNIeKDKMAojZbferXDfoI2UoPExFVwhrM2IQ=='
}

while fetch_from.year == current_year:
    fetch_to = fetch_from + timedelta(days=60)
    fetch_to = min(fetch_to, datetime(current_year, 12, 31))
    data_url = url + f'&from={fetch_from.strftime("%Y-%m-%d")}&to={fetch_to.strftime("%Y-%m-%d")}'
    r = requests.get(data_url, headers=headers)
    raw_data += r.json()['data']['candles']
    fetch_from = fetch_to + timedelta(days=1)


zerodha_data = dict()
for row in raw_data:
    dt_tm, open_, high_, low_, close_, _, _ = row
    dt_tm = parse(dt_tm).replace(tzinfo=None)
    dt = get_int_dt(dt_tm)
    tm = get_int_tm(dt_tm)
    row_dict = dict()
    row_dict["date_time"] = dt_tm
    row_dict["open"] = open_
    row_dict["high"] = high_
    row_dict["low"] = low_
    row_dict["close"] = close_
    row_dict["dt"] = dt
    row_dict["tm"] = tm
    dt_dict = zerodha_data.setdefault(dt, dict())
    dt_dict[tm] = row_dict



def is_series_aligned(zd_map, kd_map):
  
    match = 0

    for tm in kd_map:
        if tm in zd_map:
            if zd_map[tm]['close'] == kd_map[tm]['close']:
                match += 1


    if match < 0.9 * len(kd_map):
        return False



    match = 0
    for tm in zd_map:
        if tm in kd_map:
            if zd_map[tm]['close'] == kd_map[tm]['close']:
                match += 1

    
    if match < 0.9 * len(kd_map):
        return False
    
    return True


def not_matching(dt):
    kd_map = kaggle_data[dt]
    zd_map = zerodha_data[dt]

    for tm in kd_map:
        if tm in zd_map:
            if kd_map[tm]['close'] != zd_map[tm]['close']:
                print(tm, kd_map[tm]['close'], zd_map[tm]['close'])


misalligned_dts = list()

for dt in kaggle_data:
    kd_map = kaggle_data[dt]
    if dt not in zerodha_data:
        continue
    zd_map = zerodha_data[dt]

    # first align

    series_aligned = is_series_aligned(zd_map, kd_map)
    if not series_aligned:
        # print(dt)
        misalligned_dts.append(dt)


same_vals_dts = set()

for dt in zerodha_data:
    dt_map = zerodha_data[dt]

    for tm in dt_map:
        tm_map = dt_map[tm]
        if tm_map['close'] == tm_map['open'] == tm_map['high'] == tm_map['low']:
            same_vals_dts.add(dt)


missing_tm = set()
for dt in zerodha_data:
    dt_map = zerodha_data[dt]

    for tm in dt_map:
        tm_map = dt_map[tm]
        if tm_map['close'] == tm_map['open'] == tm_map['high'] == tm_map['low']:
            missing_tm.add(dt)




kaggle_copy = deepcopy(kaggle_data)
for dt in misalligned_dts:
    
    # first check if zerodha has issues

    # if dt not in same_vals_dts and dt not in missing_tm:
    
    kaggle_copy[dt] = zerodha_data[dt]

    

for dt in zerodha_data:
    zd_map = zerodha_data[dt]

    if dt not in kaggle_copy:
        continue
    for tm in zd_map:
        if tm not in kaggle_copy[dt]:
            print(dt, tm)
            kaggle_copy[dt][tm] = zd_map[tm]

final_data = []
for dt in kaggle_copy:
    for tm in kaggle_copy[dt]:
        final_data.append(kaggle_copy[dt][tm])

df = pd.DataFrame(final_data)
df = df.drop_duplicates(subset=['date_time'], keep='last')
df = df.reset_index(drop=True)
df = df.sort_values(['dt', 'tm'])

ticker_table = Table.from_pandas(df)
yearly_data_path = Path(SPOT_DATA_PATH) / ticker / str(current_year)
yearly_data_path.mkdir(parents=True, exist_ok=True)
feather.write_feather(ticker_table, yearly_data_path / 'table.feather')











        
        