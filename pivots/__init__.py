from enum import Enum
from tlib.lib import get_dttime_form_intdt


class PivotType(Enum):
    low = 0
    high = 1


class Timestamp:

    def __init__(self, dt, tm):
        self.dt = dt
        self.tm = tm

    def __str__(self):
        return f'{self.dt} {self.tm}'

    def __repr__(self):
        return self.__str__()


class LocalPivot:
    def __init__(self, tick, pivot_type, formation_tick):
        self.tick = tick
        self.close = tick.close
        self.ts = get_dttime_form_intdt(tick.dt, tick.tm)
        self.pivot_type = pivot_type
        self.loc = None
        self.formation_tick = formation_tick
        

    def __str__(self):
        return f'{str(self.ts)}: {self.pivot_type} {self.close}: {self.formation_tick.tm}'

    def __repr__(self):
        return self.__str__()




class GlobalPivot:

    def __init__(self, tick, pivot_type, formation_tick):
        self.tick = tick
        self.close = tick.close
        self.pivot_type = pivot_type
        self.ts = get_dttime_form_intdt(tick.dt, tick.tm)
        self.formation_tick = formation_tick
        self.loc = None

    def __str__(self):
        return f'{str(self.ts)}: {self.close}'

    def __repr__(self):
        return self.__str__()



class PivotsBase:

    def __init__(self, interval):

        self.interval = interval


        self.local_minima = list()
        self.local_maxima = list()
        self.global_minima = list()
        self.global_maxima = list()

        self.locals = list()
        self.globals = list()
        self.local_map = dict()
        self.global_map = dict()
        
        self.ticks = list()
        
        self.latest_local_pivot_type    = None
        self.latest_global_pivot_type   = None
        self.current_tick               = None
        self.previous_tick              = None
        self.maxima_tick                = None
        self.minima_tick                = None




    def on_tick(self, tick):
        
        self.ticks.append(tick)
        self.current_tick = tick

        if tick.dt == 20170328 and self.interval == 5:
            
            debug = True
            

        if self.maxima_tick is None: 
            # or tick.high > self.maxima_tick.high:
            self.maxima_tick = tick
            
        else:
            if tick.high > self.maxima_tick.high:
                self.maxima_tick = tick


        if self.minima_tick is None:
            # or tick.low < self.minima_tick.low:
            self.minima_tick = tick
        else:
            if tick.low < self.minima_tick.low:
                self.minima_tick = tick



        self.on_tick_custom(tick)

        self.previous_tick = tick


    def mark_local_minima(self, tick):
        raise NotImplementedError()


    def mark_local_maxima(self, tick):
        raise NotImplementedError()

    
    def on_tick_custom(tick):
        raise NotImplementedError