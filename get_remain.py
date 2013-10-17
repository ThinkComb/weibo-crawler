#!/usr/bin/env python
#coding=utf8

"""Powered by python array.
    python array: http://docs.python.org/2/library/array.html
    最大值的一进制表示的位数，即为bitmap 需要的bit数量。
    使用bimap 可以快速查重、排序、做集合运算等.
"""

import os
import sys
import time
import json
import array
import random

ELE_BITS = 8
ELE_MAX = 128
# how many "1" for number n? n: 0~255, ie. 0b00000000 ~ 0b11111111
ONE_COUNTER = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8]


class BitMap():
    ''' array(0b00000000, 0b00000000, 0b00000000, ... )
    '''

    def __init__(self, n):
        self.arr = array.array('B', '\x00' * n)
        self.length = n

    def set_bit(self, i):
        no, offset = divmod(i, ELE_BITS)
        self.arr[no] |= (ELE_MAX >> offset)
        return True

    def get_bit(self, i):
        no, offset = divmod(i, ELE_BITS)
        if self.arr[no] & (ELE_MAX >> offset):
            return True
        return False

    # all too slow !!
    def count_ones(self):
        c = 0
        # for byte in self.arr:
        #    c += ONE_COUNTER[byte]
        # c = sum([ONE_COUNTER[byte] for byte in self.arr])
        # c = reduce(lambda x, y: x + ONE_COUNTER[y], self.arr)
        return c

    def display(self):
        for byte in self.arr:
            s = bin(byte)[2:]
            print "%s%s" % ('0' * (ELE_BITS - len(s)), s),
        print



M = 8000000000 - 1
N = 1000000000  # 10亿

# self-defined
def get_remain(task_file, done_file, outfile):
    bm = BitMap(N)
    print 'init bitmap done'

    # set bitmap for done task
    # take case of the format of done_file
    with open(done_file) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            recs = line.split(': ')
            uids = recs[1].split(',')
            for uid in uids:
                uid = int(uid)
                if uid > M: continue
                bm.set_bit(uid)
    print 'set done task done'


    # take care of the format of task_file
    with open(outfile, 'w') as outf:
        with open(task_file) as f:
            for line in f:
                uid = int(line)
                # invalid uid or already have
                if uid > M or bm.get_bit(uid): continue
                # get a remain, set the bit
                bm.set_bit(uid)
                outf.write("%d\n" % uid)
    print 'get remain done'


def test():
    bm =  BitMap(N)
    print 'BitMap Init done'
    nums = []
    for i in xrange(100):
        n = random.randint(1, 100000)
        nums.append(n)
        bm.set_bit(n)
    for n in nums:
        assert(bm.get_bit(n))

    # print 'count ones:'
    # print bm.count_ones()


def main():
    if len(sys.argv) < 4:
        sys.exit('Please provide task_file, done_file, outfile ')
    get_remain(sys.argv[1], sys.argv[2], sys.argv[3])


if __name__ == '__main__':
    # test()
    main()


