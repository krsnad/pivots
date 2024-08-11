# distutils: language=c++


cimport cython
import numpy as np
cimport numpy as np

from cpython cimport array
import array


cdef int offset_min = 539 + 15

@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef int get_tf_tm(int tf_bucket, int tf):
    cdef int total_minutes = ((tf_bucket + 1) * tf) + offset_min
    cdef int hour = total_minutes // 60
    cdef int minutes = total_minutes % 60
    return hour * 100 + minutes


dow_t = (0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4)


@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef int dayofweek(int dt):
    cdef int y = <int> dt / 10000
    cdef int md = dt % 10000
    cdef int m = <int> md / 100
    cdef int d = md % 100

    if m < 3:
        y = y - 1

    cdef int yd4 = <int> y / 4
    cdef int yd100 = <int> y / 100
    cdef int yd400 = <int> y / 400
    cdef int h = dow_t[m - 1]

    cdef int week = (y + yd4 - yd100 + yd400 + h + d) % 7
    week = week - 1

    return (week % 7 + 7) % 7


cpdef int get_minutes(long tm):
    cdef int hour = <int> (tm / 100)
    cdef int minutes = tm % 100
    return hour * 60  + minutes - offset_min - 1


cdef set_buffer(double[:] buffer, double open_, double high, double low, double close, double volume):
    buffer[0] = open_
    buffer[1] = high
    buffer[2] = low
    buffer[3] = close
    buffer[4] = volume
    buffer[5] = 1


cdef init_tfdata(dict tf_data, int[:] timeframes):
    tf_data['indices'] = dict()
    tf_data['markers'] = dict()
    for f in timeframes:
        tf_data[f] = dict()
        tf_data['indices'][f] = dict()
        tf_data['markers'][f] = 0

    for f in [1, 'd', 'w']:
        tf_data[f] = dict()
        tf_data['indices'][f] = dict()
        tf_data['markers'][f] = 0


@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef resample(table, int[:] timeframes):

    cdef np.ndarray[long]   dt_array    = table['dt'].to_numpy()
    cdef np.ndarray[long]   tm_array    = table['tm'].to_numpy()
    cdef np.ndarray[double] open_       = table['open'].to_numpy()
    cdef np.ndarray[double] close       = table['close'].to_numpy()
    cdef np.ndarray[double] high        = table['high'].to_numpy()
    cdef np.ndarray[double] low         = table['low'].to_numpy()
    cdef np.ndarray[double] volume

    cdef int array_len = len(dt_array)

    if 'volume' in table.schema.names:
         volume = table['volume'].to_numpy()
    else:
        volume = np.zeros((array_len,))

    cdef int num_tfs = len(timeframes)
    cdef int tf_i
    cdef int tf

    cdef int i

    cdef long prev_dt = dt_array[0]
    for i in range(15):
        if tm_array[i] >= 915:
            break

    cdef long prev_tm = tm_array[i]
    cdef long dt
    cdef long tm
    cdef int skip

    cdef int minutes = get_minutes(prev_tm)

    cdef dict tf_data = dict()

    init_tfdata(tf_data, timeframes)

    cdef double[:] tf_buffer
    cdef double[:, :] tf_buffers = np.zeros((num_tfs + 2, 6), dtype=np.double)

    cdef int tf_bucket, current_tf_bucket, buffer_len

    for i in range(num_tfs + 2):
        set_buffer(tf_buffers[i], open_[0], high[0], low[0], close[0], volume[0])

    cdef int[:] tf_bucket_tracer = np.ndarray(num_tfs, dtype=np.intc)

    for i in range(num_tfs):
        tf_bucket_tracer[i] = <int> (minutes/timeframes[i])


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

            current_tf_bucket = <int> minutes / tf
            tf_buffer = tf_buffers[tf_i]


            if current_tf_bucket == tf_bucket:
                update_buffer(tf_buffer, high[i], low[i], close[i], volume[i])
                continue

            tf_tm = get_tf_tm(tf_bucket, tf)
            buffer_len = <int> tf_buffer[5]

            # 2 conditions bucket is fully filled or not
            skip = 0
            if buffer_len < tf - 1:
                skip = incomplete_buffer(tf_buffer, tf, dt, tf_tm)


            if skip == 0:
                
                append_tf_data(tf, tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], prev_dt, tf_tm)


            set_buffer(tf_buffers[tf_i], open_[i], high[i], low[i], close[i], volume[i])
            tf_bucket_tracer[tf_i] = current_tf_bucket

        # ohlc
        tf_i = tf_i + 1
        tf_buffer = tf_buffers[tf_i]

        if dt != prev_dt:
            dt_index.append(dt)
            append_tf_data('d', tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], prev_dt, prev_tm)
            set_buffer(tf_buffers[tf_i], open_[i], high[i], low[i], close[i], volume[i])

        else:
            update_buffer(tf_buffer, high[i], low[i], close[i], volume[i])



        # weekly
        tf_i = tf_i + 1
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



cdef final_update_tf_data(int num_tfs, int[:] timeframes, dict tf_data, double[:, :] tf_buffers,
                          int [:] tf_bucket_tracer,  long dt, long tm):
    for tf_i in range(num_tfs + 2):
        tf_buffer = tf_buffers[tf_i]


        if tf_i < num_tfs:
            tf = timeframes[tf_i]
            tf_bucket   = tf_bucket_tracer[tf_i]
            tf_tm       = get_tf_tm(tf_bucket, tf)
            buffer_len  = <int> tf_buffer[5]

            # 2 conditions bucket is fully filled or not
            skip = 0

            if buffer_len < tf - 1:
                skip = incomplete_buffer(tf_buffer, tf, dt, tf_tm)

            if skip == 0:
                append_tf_data(tf, tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], dt, tf_tm)

        else:
            tf = 'd' if tf_i == num_tfs else 'w'
            append_tf_data(tf, tf_data, tf_buffer[0], tf_buffer[1], tf_buffer[2], tf_buffer[3], tf_buffer[4], dt, tm)



cdef int incomplete_buffer(double[:] buffer, int tf, int dt, int tf_tm):


    
    if tf_tm > 1529:
        # tf_tm is from the function get_tf_tm. we are not iterating over ticks which are outside of 915 and 1529
        return 0

    if tf_tm > 920:
        buffer[3] = <double> np.nan
    return 0



cdef append_tf_data(tf, tf_data, double open_, double high, double low, double close, double volume, int dt, int tm):
    cdef int index
    if dt not in tf_data[tf]:
        tf_data[tf][dt] = dict()

    dt_dict = tf_data[tf][dt]
    index = tf_data['markers'][tf]
    dt_dict[tm] = Tick(index, dt, tm, open_, high, low, close, volume)
    tf_data['indices'][tf][index] = [dt, tm]
    tf_data['markers'][tf] = index + 1






cdef void update_buffer(double[:] buffer, double high, double low, double close, double volume):
    cdef double buffer_high = buffer[1]
    cdef double buffer_low = buffer[2]
    cdef double buffer_volume = buffer[4]

    if high > buffer_high:
        buffer[1] = high

    if low < buffer_low:
        buffer[2] = low

    buffer[3] = close
    buffer[4] = buffer_volume + volume
    buffer[5] = buffer[5] + 1


cdef class Tick:

    cdef public int index
    cdef public int dt
    cdef public int tm
    cdef public double open
    cdef public double high
    cdef public double low
    cdef public double close
    cdef public double volume
    cdef public double oc_h
    cdef public double oc_l

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

