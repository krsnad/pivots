from pathlib import Path


index_tickers = ['nifty', 'banknifty']


home_directory = Path.home()


DATASETS_PATH       = home_directory / 'bin/tlib/datasets'
OPTIONS_DATA_PATH   = DATASETS_PATH / 'options_data'
SPOT_DATA_PATH      = DATASETS_PATH / 'index_data'

RAW_FILES_PATH      = DATASETS_PATH / 'raw'

OPTIONS_RAW_PATH    = RAW_FILES_PATH / 'options'
ICHARTS_OPTIONS_RAW_PATH = OPTIONS_RAW_PATH / 'icharts'