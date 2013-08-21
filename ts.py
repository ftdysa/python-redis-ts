import base64
from time import time

# Python learning!!

# if/else/function defs/class def have : at the end

class TimeSeries:
    def __init__(self, prefix, timestep, r):
        self.prefix = prefix
        self.timestep = timestep
        self.redis = r

    def normalize_time(self, t):
        t - (t % self.timestep)
        return t

    def get_key(self, t):
        ret = "ts:" + self.prefix + ":" + str(self.normalize_time(t))
        print "get_key(" + str(t) + ") = " + ret
        return ret

    def tsencode(self, data):
        data = str(data)
        if "\x00" in data or "\x01" in data:
            ret = "E" + base64.b64encode(data)
        else:
            ret = "R" + data

        print "tsencode("+data+") = " + ret
        return ret

    def tsdecode(self, data):
        if data[0:1] == 'E':
            return base64.b64decode(data[1:])
        else:
            return data[1:]

    def add(self, data, origin_time=None):
        data = self.tsencode(data)
        
        if origin_time:
            origin_time = self.tsencode(origin_time)

        now = time()

        value = str(now) + "\x01" + data

        if origin_time:
            value + "\x01" + str(origin_time)

        value + "x\00"

        self.redis.append(self.get_key(now), value)

    def decode_record(self, r):
        result = {}
        s = r.split("\x01")
        res['time'] = s[0]
        res['data'] = self.tsdecode(s[1])

        if s[2]:
            res['origin_time'] = self.tsdecode(s[2])
        else:
            res['origin_time'] = None

        return result

    def seek(self, time):
        best_start = None
        best_end = None
        range_len = 64
        key = self.get_key(time)
        length = self.redis.strlen(key)

        if length == 0:
            return 0

        min = 0
        max = length - 1

        while True:
            p = min + ((max - min) / 2)
            # puts "Min: #{min} Max: #{max} P: #{p}"
            # Seek the first complete record starting from position 'p'.
            # We need to search for two consecutive \x00 chars, and enlarnge
            # the range if needed as we don't know how big the record is.

            while True:
                range_end = p + range_len - 1

                if range_end > length:
                    range_end = length

                r = self.redis.getrange(key, p, range_end)

                if p == 0:
                    sep = -1
                else:
                    sep = r.index("\x00")

                if sep > -1:
                    sep2 = r.index("\x00", sep + 1)

                if sep and sep2:
                    record = r[sep+1:sep2]
                    record_start = p + sep + 1
                    record_end = p + sep2 - 1

                    dr = self.decode_record(record)

                    # Take track of the best sample, that is the sample
                    # that is greater than our sample, but with the smallest
                    # increment.

                    if dr['time'] >= time and (not best_time or best_time > dr['time']):
                        best_start = record_start
                        best_time = dr['time']

                    if max - min == 1:
                        return best_start

                    break

                if range_end == length:
                    return length + 1

                range_len *= 2

            if dr['time'] == time:
                record_start

            if dr['time'] > time:
                max = p
            else:
                min = p

    def produce_result(self, res, key, range_begin, range_end):
        r = self.redis.getrange(key, range_begin, range_end)
        if r:
            s = r.split("\x00")
            for r in s:
                record = self.decode_record(r)
                res.append(record)

        return res

    def fetch_range(self, begin_time, end_time):
        res = []
        begin_key = self.get_key(begin_time)
        end_key = self.get_key(end_time)
        begin_off = self.seek(begin_time)
        end_off = self.seek(end_time)

        if begin_key == end_key:
            self.produce_result(res, begin_key, begin_off, end_off - 1)
        else:
            t = self.normalize_time(begin_time)
            while True:
                t += self.timestep
                key = self.get_key(t)

                if key == end_key:
                    break

                self.produce_result(res, end_key, 0, end_off - 1)
        return res

    def fetch_timestep(self, time):
        res = []
        key = self.get_key(time)
        self.produce_result(res, key, 0, -1)
        return res
