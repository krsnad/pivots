import requests
import pyarrow.dataset as ds
import pyarrow.feather as feather
import pandas as pd


from datetime import datetime
from dateutil.parser import parse
from datetime import timedelta
from pyarrow import Table
from pathlib import Path

from pivots.data.constants import SPOT_DATA_PATH
from pivots.lib import get_int_dt, get_int_tm







nifty_url = 'https://kite.zerodha.com/oms/instruments/historical/256265/minute?user_id=YP9593&oi=1'
banknifty_url = 'https://kite.zerodha.com/oms/instruments/historical/260105/minute?user_id=YP9593&oi=1'

headers = {
    'cookie': '_ga=GA1.2.2108418404.1644552367; kf_session=N9speCnYiYWiv2N41I9vuYkvflrujExg; user_id=YP9593; public_token=y0dn47f8NNqus5k7a76bdZCJMzUYF9sJ; enctoken=PNTXUHoZvi4e03JxKPrLz6KG9vseXgcRxqacLoxDfqVYzUiYZ/dmu+BrgBtqzZIf/1JiLMi247aD/O1WMEBjFOgnBqbnnl9zFlKwCqnzGdTZ++LmUdeRIw==',
    'authorization': 'enctoken PNTXUHoZvi4e03JxKPrLz6KG9vseXgcRxqacLoxDfqVYzUiYZ/dmu+BrgBtqzZIf/1JiLMi247aD/O1WMEBjFOgnBqbnnl9zFlKwCqnzGdTZ++LmUdeRIw=='
}

current_year = 2015

ticker = 'nifty'

index_data = ds.dataset(SPOT_DATA_PATH / ticker / str(current_year) / 'table.feather', format='feather')
index_data = index_data.to_table()
index_df = index_data.to_pandas()


year = 2015

from_dt = datetime(year, 1, 1)
fetch_from = from_dt


raw_data = []


while fetch_from.year == year:
    fetch_to = fetch_from + timedelta(days=60)
    fetch_to = min(fetch_to, datetime(year, 12, 31))
    data_url = banknifty_url + f'&from={fetch_from.strftime("%Y-%m-%d")}&to={fetch_to.strftime("%Y-%m-%d")}'
    r = requests.get(data_url, headers=headers)
    raw_data += r.json()['data']['candles']
    fetch_from = fetch_to + timedelta(days=1)


df_data = list()

for x in raw_data:
    dt_tm, open_, high_, low_, close_, _, _ = x
    dt_tm = parse(dt_tm).replace(tzinfo=None)
    row_dict = dict()
    row_dict['date_time'] = dt_tm
    row_dict['open'] = open_
    row_dict['high'] = high_
    row_dict['low'] = low_
    row_dict['close'] = close_
    row_dict['dt'] = get_int_dt(dt_tm)
    row_dict['tm'] = get_int_tm(dt_tm)
    df_data.append(row_dict)

latest_df = pd.DataFrame(df_data)
df = pd.concat([index_df, latest_df])
df = df.drop_duplicates(subset=['date_time'], keep='last')
df = df.reset_index(drop=True)
df = df.sort_values(['dt', 'tm'])

ticker_table = Table.from_pandas(df)
yearly_data_path = Path(SPOT_DATA_PATH) / ticker / str(current_year)
yearly_data_path.mkdir(parents=True, exist_ok=True)
feather.write_feather(ticker_table, yearly_data_path / 'table.feather')