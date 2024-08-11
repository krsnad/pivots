from datetime import datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


from tlib.resampler.resampler import resample
from tlib.data.constants import SPOT_DATA_PATH
from tlib.lib import get_int_dt, get_dttime_form_intdt




def parse_dt(dt):
    if type(dt) is int:
        return datetime.strptime(str(dt), '%Y%m%d')
    elif type(dt) in [str]:
        return datetime.strptime(dt, '%Y%m%d')


class OHLCData(object):

    def __init__(self, data, index, dates, freq):

        self.params = {'open': 0, 'high': 1, 'low': 2, 'close': 3, 'volume': 4}
        self.data = data
        self.freq = freq
        self.dates = dates
        self.reversed_dates = dict((x, i) for i, x in enumerate(dates))
        self.set_index(index)


    def set_index(self, index):
        self.index = index


    def iloc(self, start_index, end_index=None):
        if end_index is None:
            dt, tm = self.index[start_index]
            if self.freq in ['w', 'd']:
                return self[dt]
            else:
                return self[dt][tm]

        else:
            if self.freq in ['w', 'd']:
                raise NotImplemented
            result = []
            for i in range(start_index, end_index + 1):
                dt, tm = self.index[i]
                result.append(self[dt][tm])

            return result

    def get_df(self):
        pddata = []
        for dt in self.data:
            for tm in self.data[dt]:
                tick = self.data[dt][tm]
                pddata.append([parse(str(dt) + ' ' + str(tm)), tick.open, tick.high, tick.low, tick.close])
                df = pd.DataFrame(pddata)
        df.columns = ['dttime', 'open', 'high', 'low', 'close']
        df.set_index('dttime', inplace=True)
        return df


    def __getitem__(self, item):
        if isinstance(item, slice):
            return self._slice_data(item.start, item.stop)
        return self.data.get(item, None)


    def _slice_data(self, start_date, end_date):
        dt_index = self.reversed_dates[start_date]
        result = {key: [] for key in ['date_time', 'dt', 'tm', 'open', 'high', 'low', 'close', 'volume']}
        while dt_index < len(self.dates) and self.dates[dt_index] <= end_date:
            dt = self.dates[dt_index]
            for tm, tick in self.data[dt].items():
                result['dt'].append(dt)
                result['tm'].append(tm)
                result['date_time'].append(get_dttime_form_intdt(dt, tm))
                result['open'].append(tick.open)
                result['high'].append(tick.high)
                result['low'].append(tick.low)
                result['close'].append(tick.close)
                result['volume'].append(tick.volume)
            dt_index += 1
        return result



    def __getattr__(self, attr):
        if attr in self.params:
            raise NotImplemented
        else:
            raise AttributeError



class SpotData:

    def __init__(self, ticker, start_date, end_date, offset=20, resampling_freqs=None, columns=None):
        
        self.ticker         = ticker
        self.start_date     = parse_dt(start_date)
        self.end_date       = parse_dt(end_date)
        self.offset         = offset
        self.offseted_stdt  = self.start_date - relativedelta(days=offset * 1.45)
        
        self.columns = columns if columns else ['dt', 'tm', 'open', 'high', 'low', 'close']
        
        self.resampling_freqs = np.array(resampling_freqs, dtype=np.int32) if isinstance(resampling_freqs, list) else resampling_freqs
        self.table = self.load_data()
        print(self.resampling_freqs)
        self.raw_resampled_data, self.dates = resample(self.table, self.resampling_freqs)
        self.set_resampled_data(self.raw_resampled_data)


    def load_data(self):
        
        year_buckets = list(range(self.offseted_stdt.year, self.end_date.year+1))
        spot_table = None
        for year in year_buckets:
            year_data_ds = self.load_year_data(year)
            if not year_data_ds:
                continue

            year_data_table = year_data_ds.to_table(
                filter=(
                        (ds.field('dt') >= get_int_dt(self.offseted_stdt)) & (ds.field('dt') <= get_int_dt(self.end_date))
                ),
                columns=self.columns
            )
            
            spot_table = year_data_table if spot_table is None else pa.concat_tables([spot_table, year_data_table])
            
        return spot_table
    
    
    def load_year_data(self, year):
        try:
            return ds.dataset(SPOT_DATA_PATH / self.ticker / str(year) / 'table.feather', format='feather')
        except FileNotFoundError:
            return None


    def set_resampled_data(self, resampled_data):
        self.resampled_data = dict()
        for freq_i, freq in enumerate(self.resampling_freqs):
            self.resampled_data[freq] = OHLCData(data=resampled_data[freq], index=resampled_data['indices'][freq],
                                                 dates=self.dates,
                                                 freq=freq)

        for freq in [1, 'd', 'w']:
            self.resampled_data[freq] = OHLCData(data=resampled_data[freq], index=resampled_data['indices'][freq],
                                                 dates=self.dates,
                                                 freq=freq)











