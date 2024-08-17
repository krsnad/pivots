from pathlib import Path
from pyarrow import Table
from pivots.data.constants import SPOT_DATA_PATH
import pyarrow.feather as feather
import pandas as pd
import csv
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta



class ConvertIndexFiles(object):

    def __init__(self):
        self.base_path = '/datasets/raw/index_data'

    def convert_files(self):

        for ticker in ['banknifty']:

            for year in range(2015, 2016):
                print(year)

                file_path = Path(self.base_path) / ticker / f'{year}.csv'
                data_dict = self.convert_csv_to_dict(file_path)
                df = pd.DataFrame.from_dict(data_dict)
                df = df.sort_values(['dt', 'tm'])
                ticker_table = Table.from_pandas(df)

                yearly_data_path = Path(SPOT_DATA_PATH) / ticker / str(year)
                Path(yearly_data_path).mkdir(parents=True, exist_ok=True)
                feather.write_feather(ticker_table, yearly_data_path / 'table.feather')


    @staticmethod
    def get_empty_data_dict():
        return {'date_time': [], 'open': [], 'high': [], 'low': [], 'close': [], 'dt': [], 'tm': []}


    @staticmethod
    def convert_csv_to_dict(file_path):
        data_dict = ConvertIndexFiles.get_empty_data_dict()

        with open(file_path) as f:
            csv_reader = csv.reader(f)
            for row_i, row in enumerate(csv_reader):
                if row_i == 0:
                    continue
                index_csv_row = IndexCsvRow(row)

                for var in ['date_time', 'dt', 'tm', 'open', 'high', 'low', 'close']:
                    data_dict[var].append(getattr(index_csv_row, var))

        return data_dict



class IndexCsvRow:

    def __init__(self, row):
        _, dt, tm, open_, high, low, close = row

        self.open       = float(open_)
        self.high       = float(high)
        self.low        = float(low)
        self.close      = float(close)
        self.date_time  = parse(dt + ' ' + tm, dayfirst=False)
        self.date_time  = self.date_time - relativedelta(minutes=1) # kaggle dataset issue

        self.dt         = self.date_time.year * 10000 + self.date_time.month * 100 + self.date_time.day
        self.tm         = self.date_time.hour * 100 + self.date_time.minute
