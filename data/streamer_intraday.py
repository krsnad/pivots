from collections import namedtuple

import numpy as np


from tlib.data import SpotData
from tlib.ta.pivots import PivotManager


from line_profiler import profile


class StreamerIntraday:   

    def __init__(self, ticker='nifty', start_date='20180101', end_date='20181231',
                freqs=[5, 10, 15, 30],
                indicators=[
                    
                ]):
        
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date            
        self.spot_data = SpotData(ticker, start_date, end_date, resampling_freqs=freqs)     
        self.freqs = [1]  + list(freqs) + ['d']
        
        self.indicators = indicators
        self.pivot_manager = PivotManager(self.freqs)
        

        self.dates = self.spot_data.dates


    def init_intra(self, dt):
        self.dt = dt
        self.initialize_ticks()



    def initialize_ticks(self):
        for freq in self.freqs:
            setattr(self, f'tick_{freq}', None)


    def stream_intra(self):
        for i, dt in enumerate(self.dates):
            
            self.new_day = True
            self.init_intra(dt)
            yield from self._stream_day_ticks(dt)     
            self.new_day = False

    @profile
    def _stream_day_ticks(self, dt):
        print(dt)
        for tm, tick in self.spot_data.resampled_data[1][dt].items():
            
            self.tick_1 = tick
            self.tick = tick
            self.tm = tm
            self._process_timeframes(dt, tm)
            self.pivot_manager.process_pivots(self)
            if dt >= self.start_date:
                yield self.tick_1
                    

    def _process_timeframes(self, dt, tm):
        
        for freq in self.freqs[1:]:
            timeframe_data = self.spot_data.resampled_data[freq][dt]
            if tm not in timeframe_data:
                setattr(self, f'tick_{freq}', None)
                continue
            
            current_tick = timeframe_data.get(tm)
            setattr(self, f'tick_{freq}', current_tick)
            setattr(self, f'tick_index_{freq}', current_tick.index)
            
    
if __name__ == '__main__':
    print('running the file')
    ticker = 'nifty'
    startdate = 20170101
    enddate = 20170110

    streamer = StreamerIntraday(ticker, startdate, enddate)
    for _ in streamer.stream_intra():
        pass
