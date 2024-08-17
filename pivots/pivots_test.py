from pivots.data.streamer_intraday import StreamerIntraday



ticker      = 'nifty'
start_date  = 20210101
end_date    = 20210901


stream = StreamerIntraday(ticker, start_date, end_date)

for dt in stream.stream_dates():
    print(dt)

    for tick in stream.stream_intra():
        pass