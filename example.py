#!/usr/bin/python

from ts import TimeSeries
from time import sleep, time
import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)
t = TimeSeries("test", 1, r)

now = time()

for i in range(30):
    print i
    t.add(str(i))
    sleep(0.1)

begin_time = now + 1
end_time = now + 2

print "Get range from %s to %s" % (begin_time, end_time)

stamps = t.fetch_range(begin_time, end_time)
for s in stamps:
    print "Record time %s, data %s" % (s['time'], s['data'])

print "get a single timestamp near %s" % begin_time

stamps = t.fetch_timestep(begin_time)
for s in stamps:
    print "Record time %s, data %s" % (s['time'], s['data'])
