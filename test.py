#!/usr/bin/env python
#coding=utf8

from weiboapi.apiclient import APIClient, APIError
api = APIClient()

def get_user_by_id(uid):
    r = api.call('users/show', uid=uid)
    return r


def remain_ip_hits():
    try:
        r = api.rate_limit()
    except:
        return 0
    return r['remaining_ip_hits']

def main():
    h =  remain_ip_hits()
    uid = 1990786715
    if h > 0:
        u = get_user_by_id(uid)
        print u['screen_name']

if __name__ == '__main__':
    main()
