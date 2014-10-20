#!/usr/bin/env python

import urllib2
import json

'''
> for i in `seq 1 5`; do sed -e 's/storage_backend = bitcask/storage_backend = leveldb/' -i.back dev/dev$i/etc/riak.conf; done
>
>
'''


HOST='localhost:8098'
SCHEMA_BUCKET='__presto_schema'
SCHEMA_KEY='__schema'

'''
tables:

users:

 id | name    | army
----+---------+------------
  0 | Luke    | Rebellion
  1 | Anakin  | Imperial
  2 | Leia    | Rebellion
  3 | Sith    | Imperial
  4 | Fett    | Freelance
  5 | Solo    | Freelance

logs:

 timestamp           | method | status | accessor
---------------------+--------+--------+-----------
 2014-04-12-00:00:00 | GET    | 200    | 2
 2014-04-12-00:00:01 | GET    | 404    | 2
 2014-04-12-00:00:02 | GET    | 503    | 4
 2014-04-12-00:00:03 | PUT    | 204    | 5
 2014-04-12-00:01:00 | GET    | 200    | 5
 2014-04-12-00:03:00 | GET    | 200    | 0
 2014-04-15-00:04:00 | GET    | 301    | 1

'''

def get(bucket, key):
    url = 'http://%s/types/t/buckets/%s/keys/%s' % (HOST, bucket, key)
    c = urllib2.urlopen(url)
    return c.read()

def insert(bucket, key, data):
    url = 'http://%s/types/t/buckets/%s/keys/%s' % (HOST, bucket, key)
    req = urllib2.Request(url=url,
                          headers= {'Content-type': 'applicaiton/json'},
                          data =data)
    req.get_method = lambda: 'PUT'
    response = urllib2.urlopen(req)

def insert_with_index(bucket, key, data):
    url = 'http://%s/types/t/buckets/%s/keys/%s' % (HOST, bucket, key)
    j = json.loads(data)
    headers = {'Content-type': 'applicaiton/json'}
    for prop in j:
        if isinstance(j[prop], str) or isinstance(j[prop], unicode):
            headers['X-Riak-Index-%s_bin' % prop] = j[prop]
        elif isinstance(j[prop], int):
            headers['X-Riak-Index-%s_int' % prop] = j[prop]

    req = urllib2.Request(url=url, headers=headers, data=data)
    req.get_method = lambda: 'PUT'
    response = urllib2.urlopen(req)

def create_schema():
    url = 'http://%s/types/t/buckets/%s/keys/%s' % (HOST, SCHEMA_BUCKET, SCHEMA_KEY)

    req = urllib2.Request(url=url,
                          headers= {'Content-type': 'applicaiton/json'},
                          data = '{"tables": {"logs":["Access Logs"], "users":["Stars of Star Wars"]}}')
    req.get_method = lambda: 'PUT' #if I remove this line then the POST works fine.
    response = urllib2.urlopen(req)

    print response.read()


if __name__ == '__main__':
    # add table to schema if not exists
    create_schema()
    print(get(SCHEMA_BUCKET, SCHEMA_KEY))
    # create table 1
    insert(SCHEMA_BUCKET, 'logs',
           '''
{"name":"logs",
 "columns":[
  {"name":"timestamp", "type":"STRING", "index":false},
  {"name":"method", "type":"STRING", "index":true},
  {"name":"status", "type":"LONG", "index":true},
  {"name":"accessor", "type":"LONG", "index":true}]}
''')
    print(get(SCHEMA_BUCKET,  'logs'))

    # host, user, method, path, code, size, referer, agent, time, tag

    # create table 2
    insert(SCHEMA_BUCKET, 'users',
           '''
{"name":"users",
 "columns":[
  {"name":"id", "type":"LONG", "index":true},
  {"name":"name", "type":"STRING", "index":true},
  {"name":"army", "type":"STRING", "index":true}]}
''')
    print(get(SCHEMA_BUCKET, 'users'))

    # insert_with_index data to table 1
    insert_with_index('logs',  '2014-04-12-00:00:00',
           '{"timestamp":"2014-04-12-00:00:00", "method":"GET", "status":200, "accessor": 2}')
    insert_with_index('logs', '2014-04-12-00:00:01',
           '{"timestamp":"2014-04-12-00:00:01", "method":"GET", "status":404, "accessor": 2}')
    insert_with_index('logs', '2014-04-12-00:00:02',
           '{"timestamp":"2014-04-12-00:00:02", "method":"GET", "status":503, "accessor": 4}')
    insert_with_index('logs', '2014-04-12-00:00:03',
           '{"timestamp":"2014-04-12-00:00:03", "method":"PUT", "status":204, "accessor": 5}')
    insert_with_index('logs', '2014-04-12-00:01:00',
           '{"timestamp":"2014-04-12-00:01:00", "method":"GET", "status":200, "accessor": 5}')
    insert_with_index('logs', '2014-04-12-00:03:00',
           '{"timestamp":"2014-04-12-00:03:00", "method":"GET", "status":200, "accessor": 0}')
    insert_with_index('logs', '2014-04-12-00:04:00',
           '{"timestamp":"2014-04-15-00:04:00", "method":"GET", "status":301, "accessor": 1}')
    for k in ['2014-04-12-00:00:00',
              '2014-04-12-00:00:01',
              '2014-04-12-00:00:02',
              '2014-04-12-00:00:03',
              '2014-04-12-00:01:00',
              '2014-04-12-00:03:00',
              '2014-04-12-00:04:00']:
        print(get('logs', k))

    # insert_with_index data to table 2
    insert_with_index('users', '0', '{"id":0, "name":"Luke", "army":"Rebellion"}')
    insert_with_index('users', '1', '{"id":1, "name":"Anakin", "army":"Imperial"}')
    insert_with_index('users', '2', '{"id":2, "name":"Leia", "army":"Rebellion"}')
    insert_with_index('users', '3', '{"id":3, "name":"Sith", "army":"Imperial"}')
    insert_with_index('users', '4', '{"id":4, "name":"Fett", "army":"Freelance"}')
    insert_with_index('users', '5', '{"id":5, "name":"Solo", "army":"Freelance"}')
    for i in xrange(0, 6):
        print(get('users', str(i)))
