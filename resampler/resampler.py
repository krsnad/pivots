import numpy as np

offset_min = 539 + 15

def get_tf_tm(tf_bucket, tf):
    total_minutes = ((tf_bucket + 1) * tf) + offset_min
    hour = total_minutes // 60
    minutes = total_minutes % 60
    return hour * 100 + minutes

dow_t = (0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4)

def dayofweek(dt):
    y = dt // 10000
    md = dt % 10000
    m = md // 100
    d = md % 100

    if m < 3:
        y = y - 1

    yd4 = y // 4
    yd100 = y // 100
    yd400 = y // 400
    h = dow_t[m - 1]

    week = (y + yd4 - yd100 + yd400 + h + d) % 7
    week = week - 1

    return (week % 7 + 7) % 7

def get_minutes(tm):
    hour = tm // 100
    minutes = tm % 100
    return hour * 60 + minutes - offset_min - 1

def set_buffer(buffer, open_, high, low, close, volume):
    buffer[0] = open_
    buffer[1] = high
    buffer[2] = low
    buffer[3] = close
    buffer[4] = volume
    buffer[5] = 1

def init_tfdata(tf_data, timeframes):
    tf_data['indices'] = {}
    tf_data['markers'] = {}
    for f in timeframes:
        tf_data[f] = {}
        tf_data['indices'][f] = {}
        tf_data['markers'][f] = 0

    for f in [1, 'd', 'w']:
        tf_data[f] = {}
        tf_data['indices'][f] = {}
        tf_data['markers'][f] = 0

def resample(table, timeframes):
    dt_array = table['dt'].to_numpy()
    tm_array = table['tm'].to_numpy()
    open_ = table['open'].to_numpy()
    close = table['close'].to_numpy()
    high = table['high'].to_numpy()
    low = table['low'].to_numpy()

    array_len = len(dt_array)

    if 'volume' in table.schema.names:
        volume = table['volume'].to_numpy()
    else:
        volume = np.zeros((array_len,))

    num_tfs = len(timeframes)

    prev_dt = dt_array[0]
    for i in range(15):
        if tm_array[i] >= 915:
            break

    prev_tm = tm_array[i]
    minutes = get_minutes(prev_tm)

    tf_data = {}

    init_tfdata(tf_data, timeframes)

    tf_buffers = np.zeros((num_tfs + 2, 6))

    for i in range(num_tfs + 2):
        set_buffer(tf_buffers[i], open_[0], high[0], low[0], close[0], volume[0])

    tf_bucket_tracer = np.array([minutes // tf for tf in timeframes], dtype=int)

    dt_index = [prev_dt]
    for i in range(1, array_len):
        dt = dt_array[i]
        tm = tm_array[i]

        if tm < 915 or tm > 1529:
            continue

        minutes = get_minutes(tm)

        append_tf_data(1, tf_data, open_[i], high[i], low[i], close[i], volume[i], dt, tm)

        for tf_i in range(num_tfs):
            tf = timeframes[tf_i]
            tf_bucket = tf_bucket_tracer[tf_i]

            current_tf_bucket = minutes // tf
            tf_buffer = tf_buffers[tf_i]

            if current_tf_bucket == tf_bucket:
                update_buffer(tf_buffer, high[i], low[i], close[i], volume[i])
                continue

            tf_tm = get_tf_tm(tf_bucket, tf)
            buffer_len = int(tf_buffer[5])

            skip = 0
            if buffer_len < tf - 1:
                skip = incomplete_buffer(tf_buffer, tf, dt, tf_tm)

            if skip == 0:
                append_tf_data(tf, tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], prev_dt, tf_tm)

            set_buffer(tf_buffers[tf_i], open_[i], high[i], low[i], close[i], volume[i])
            tf_bucket_tracer[tf_i] = current_tf_bucket

        # Daily OHLC
        tf_i = num_tfs
        tf_buffer = tf_buffers[tf_i]

        if dt != prev_dt:
            dt_index.append(dt)
            append_tf_data('d', tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], prev_dt, prev_tm)
            set_buffer(tf_buffers[tf_i], open_[i], high[i], low[i], close[i], volume[i])
        else:
            update_buffer(tf_buffer, high[i], low[i], close[i], volume[i])

        # Weekly OHLC
        tf_i = num_tfs + 1
        tf_buffer = tf_buffers[tf_i]

        if dt != prev_dt and dayofweek(dt) < dayofweek(prev_dt):
            append_tf_data('w', tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], prev_dt, prev_tm)
            set_buffer(tf_buffers[tf_i], open_[i], high[i], low[i], close[i], volume[i])
        else:
            update_buffer(tf_buffer, high[i], low[i], close[i], volume[i])

        prev_dt = dt
        prev_tm = tm

    final_update_tf_data(num_tfs, timeframes, tf_data, tf_buffers, tf_bucket_tracer, prev_dt, prev_tm)

    return tf_data, dt_index

def final_update_tf_data(num_tfs, timeframes, tf_data, tf_buffers, tf_bucket_tracer, dt, tm):
    for tf_i in range(num_tfs + 2):
        tf_buffer = tf_buffers[tf_i]

        if tf_i < num_tfs:
            tf = timeframes[tf_i]
            tf_bucket = tf_bucket_tracer[tf_i]
            tf_tm = get_tf_tm(tf_bucket, tf)
            buffer_len = int(tf_buffer[5])

            skip = 0
            if buffer_len < tf - 1:
                skip = incomplete_buffer(tf_buffer, tf, dt, tf_tm)

            if skip == 0:
                append_tf_data(tf, tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], dt, tf_tm)
        else:
            tf = 'd' if tf_i == num_tfs else 'w'
            append_tf_data(tf, tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], dt, tm)

def incomplete_buffer(buffer, tf, dt, tf_tm):
    if tf_tm > 1529:
        return 0

    if tf_tm > 920:
        buffer[3] = float('nan')
    return 0

def append_tf_data(tf, tf_data, open_, high, low, close, volume, dt, tm):
    if dt not in tf_data[tf]:
        tf_data[tf][dt] = {}

    dt_dict = tf_data[tf][dt]
    index = tf_data['markers'][tf]
    dt_dict[tm] = Tick(index, dt, tm, open_, high, low, close, volume)
    tf_data['indices'][tf][index] = [dt, tm]
    tf_data['markers'][tf] = index + 1

def update_buffer(buffer, high, low, close, volume):
    buffer_high = buffer[1]
    buffer_low = buffer[2]
    buffer_volume = buffer[4]

    if high > buffer_high:
        buffer[1] = high

    if low < buffer_low:
        buffer[2] = low

    buffer[3] = close
    buffer[4] = buffer_volume + volume
    buffer[5] = buffer[5] + 1

class Tick:
    def __init__(self, index, dt, tm, open_, high, low, close, volume):
        self.index = index
        self.dt = dt
        self.tm = tm
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

        if self.open > self.close:
            self.oc_h = self.open
            self.oc_l = self.close
        else:
            self.oc_h = self.close
            self.oc_l = self.open

    def __str__(self):
        return f'{self.dt} {self.tm}: {self.close}'

    def __repr__(self):
        return self.__str__()