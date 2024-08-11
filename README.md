```python
from streamer_intraday import *
ticker      = 'nifty'
start_date  = 20210101
end_date    = 20210901


stream = StreamerIntraday(ticker, start_date, end_date)

for _ in stream.stream_intra():
    pass

stream.pivot_manager.pivots[15]
```
