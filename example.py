#!/user/bin/env python
#coding=utf8

""" crawl weibo-user-tags
"""

import gevent.monkey
gevent.monkey.patch_all()
import gevent.queue
from gevent.coros import RLock

import os
import sys
import time
import json
import urllib2
from datetime import datetime, timedelta

from weiboapi import apiclient
from weiboapi.logger import get_logger
api = apiclient.APIClient()


# self-defined for proxies
PROXY_LIST = [({}, 8)]
N = sum([p[1] for p in PROXY_LIST])

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
# self-defined for OUTDIR & TASK_FILE
OUTDIR = os.path.join(CURRENT_PATH, 'data/')
TASK_FILE = os.path.join(CURRENT_PATH, 'task.txt')
LOG_FILE = os.path.join(CURRENT_PATH, 'error.log')
DONE_FILE = os.path.join(CURRENT_PATH, 'done.log')
STOP_FILE = os.path.join(CURRENT_PATH, 'stop.signal')

WORKER_PAUSE = 0.05
ERROR_PAUSE = 5
LINE_PER_FILE = 10000

# HTTPError 403: 代理时IP请求超过上限
# 10022   IP requests out of rate limit   IP请求频次超过上限
# 10023   User requests out of rate limit 用户请求频次超过上限
# 10024   User requests for (%s) out of rate limit    用户请求特殊接口 (%s) 频次超过上限
ERROR_NORMAL = 0
ERROR_API = -1
ERROR_RATE = -2

task_queue = gevent.queue.JoinableQueue(10000)
result_queue = gevent.queue.JoinableQueue(10000)
log_queue = gevent.queue.JoinableQueue(1000)

live_signal = 0
log_lock = RLock()
live_lock = RLock()
logger = get_logger(LOG_FILE)


def wait_time(proxy):
    try:
        rl = api.rate_limit(proxy=proxy)
    except Exception, e:
        rl = None

    if rl:
        if rl['remaining_ip_hits'] > 1 and rl['remaining_user_hits'] > 1:
            return 1
        return rl['reset_time_in_seconds'] + 1
    now = datetime.now()
    reset = now + timedelta(seconds=3600-now.minute*60-now.second)
    reset_ts = time.mktime( datetime.timetuple(reset) )
    return  reset_ts - time.time() + 1


# here is self-defined
def detail_work(uids, proxy):
    res = None
    error = None
    error_msg = None
    try:
        res = api.call('tags/tags_batch', uids=uids)
    except apiclient.APIError, e:
        error_msg = e
        if e.error_code in [10022, 10023, 10024]:
            error = ERROR_RATE
        else:
            error = ERROR_API
    except urllib2.HTTPError, e:
        error_msg = e
        if e.getcode() == 403:
            error = ERROR_RATE
        else:
            error = ERROR_NORMAL
    except Exception, e:
        error_msg = e
        error = ERROR_NORMAL

    if error is None:
        return res
    else:
        log_lock.acquire()
        logger.critical('@%s@: %s' % (uids, error_msg))
        log_lock.release()
        return error


def _worker_finish():
    live_lock.acquire()
    global live_signal
    live_signal -= 1
    print 'a worker quit!'
    print 'worker remained: %d' % live_signal
    live_lock.release()


def worker(proxy_index):
    proxy = PROXY_LIST[proxy_index]
    while True:
        t = task_queue.get()
        if t is None:
            task_queue.task_done()
            break
        res = detail_work(t, proxy)
        if res == ERROR_RATE:
            wt = wait_time(proxy)
            print '%s: A worker is falling asleep: %s' % (time.ctime(), wt)
            gevent.sleep(seconds=wt)
            print '%s: A worker is waking up.' % (time.ctime(),)
        elif res == ERROR_API or res == ERROR_NORMAL:
            gevent.sleep(seconds=ERROR_PAUSE)
        else:
            result_queue.put((t, res))

        task_queue.task_done()
        gevent.sleep(WORKER_PAUSE)

    # a worker quit
    _worker_finish()


def _init_fno():
    # init fno, do not cover existing files
    max_fno = -1
    for fn in os.listdir(OUTDIR):
        if not fn.isdigit(): continue
        fno = int(fn)
        if fno > max_fno: max_fno = fno
    fno = max_fno + 1
    return fno


# An independent worker: write to disk
def pipeline():
    fno = _init_fno()
    filename = os.path.join(OUTDIR, str(fno))
    fp = open(filename, 'w')
    counter = 0
    dfp = open(DONE_FILE, 'w')  # record tasks which are finished

    while True:
        r = result_queue.get()
        if r is None:
            result_queue.task_done()
            break
        if counter >= LINE_PER_FILE: # it's time to close the current file
            fp.close()
            counter = 0
            fno += 1
            filename = os.path.join(OUTDIR, str(fno))
            fp = open(filename, 'w')
            dfp.flush()

        # here is self-defined
        for r1 in r[1]:
            fp.write('%s\n' % json.dumps(r1))
        # here is self-defined
        dfp.write('%s: %s\n' % (fno, r[0]))
        counter += 1
        result_queue.task_done()

    fp.close()
    dfp.close()
    print 'pipeline done!'


# here is self-defined
def add_task():
    limit = 20
    uids = []
    with open(TASK_FILE) as f:
        for line in f:
            uids.append(line.strip())
            if len(uids) == limit:
                t = ','.join(uids)
                task_queue.put(t)
                uids = []

        if len(uids) > 0:
            t = ','.join(uids)
            task_queue.put(t)

    for i in xrange(N):
        task_queue.put(None)


# exit when workers all done, or STOP_FILE provided
def live():
    while True:
        if os.path.isfile(STOP_FILE):
            print 'I am killed!'
            break
        elif live_signal == 0:
            print 'I am dying!'
            break
        gevent.sleep(10)


def gevent_manager():
    global live_signal
    tasker = gevent.spawn(add_task)
    workers = []
    for i in xrange(len(PROXY_LIST)):
        for j in xrange(PROXY_LIST[i][1]):
            workers.append(gevent.spawn(worker, i))
            live_signal += 1

    piper = gevent.spawn(pipeline)
    liver = gevent.spawn(live)
    # liver control the time to exit
    gevent.joinall([liver,])
    result_queue.put(None)
    gevent.joinall([piper,])


def main():
    gevent_manager()


if __name__ == '__main__':
    main()
