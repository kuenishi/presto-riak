#!/usr/bin/env python

import urllib2

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
  3 | Seth    | Imperial
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
    url = 'http://%s/buckets/%s/keys/%s' % (HOST, bucket, key)
    c = urllib2.urlopen(url)
    return c.read()

def insert(bucket, key, data):
    url = 'http://%s/buckets/%s/keys/%s' % (HOST, bucket, key)
    req = urllib2.Request(url=url,
                          headers= {'Content-type': 'applicaiton/json'},
                          data =data)
    req.get_method = lambda: 'PUT'
    response = urllib2.urlopen(req)

def create_schema():
    url = 'http://%s/buckets/%s/keys/%s' % (HOST, SCHEMA_BUCKET, SCHEMA_KEY)

    req = urllib2.Request(url=url,
                          headers= {'Content-type': 'applicaiton/json'},
                          data = '{"tables":["logs", "users"]}')
    req.get_method = lambda: 'PUT' #if I remove this line then the POST works fine.
    response = urllib2.urlopen(req)

    print response.read()


if __name__ == '__main__':
    # add table to schema if not exists
    create_schema()
    print(get(SCHEMA_BUCKET, SCHEMA_KEY))
    # create table 1
    insert(SCHEMA_BUCKET, 'default.logs',
           '''
{"name":"logs",
 "columns":[
  {"name":"timestamp", "type":"STRING"},
  {"name":"method", "type":"STRING"},
  {"name":"status", "type":"LONG"},
  {"name":"accessor", "type":"LONG"}]}
''')
    print(get(SCHEMA_BUCKET,  'default.logs'))

    # create table 2
    insert(SCHEMA_BUCKET, 'default.users',
           '''
{"name":"users",
 "columns":[
  {"name":"id", "type":"LONG"},
  {"name":"name", "type":"STRING"},
  {"name":"army", "type":"STRING"}]}
''')
    print(get(SCHEMA_BUCKET, 'default.users'))

    # insert data to table 1
    insert('default.logs',  '2014-04-12-00:00:00',
           '{"timestamp":"2014-04-12-00:00:00", "method":"GET", "status":200, "accessor": 2}')
    insert('default.logs', '2014-04-12-00:00:01',
           '{"timestamp":"2014-04-12-00:00:01", "method":"GET", "status":404, "accessor": 2}')
    insert('default.logs', '2014-04-12-00:00:02',
           '{"timestamp":"2014-04-12-00:00:02", "method":"GET", "status":503, "accessor": 4}')
    insert('default.logs', '2014-04-12-00:00:03',
           '{"timestamp":"2014-04-12-00:00:03", "method":"PUT", "status":204, "accessor": 5}')
    insert('default.logs', '2014-04-12-00:01:00',
           '{"timestamp":"2014-04-12-00:01:00", "method":"GET", "status":200, "accessor": 5}')
    insert('default.logs', '2014-04-12-00:03:00',
           '{"timestamp":"2014-04-12-00:03:00", "method":"GET", "status":200, "accessor": 0}')
    insert('default.logs', '2014-04-12-00:04:00',
           '{"timestamp":"2014-04-15-00:04:00", "method":"GET", "status":301, "accessor": 1}')
    for k in ['2014-04-12-00:00:00',
              '2014-04-12-00:00:01',
              '2014-04-12-00:00:02',
              '2014-04-12-00:00:03',
              '2014-04-12-00:01:00',
              '2014-04-12-00:03:00',
              '2014-04-12-00:04:00']:
        print(get('default.logs', k))

    # insert data to table 2
    insert('default.users', '0', '{"id":0, "name":"Luke", "army":"Rebellion"}')
    insert('default.users', '1', '{"id":1, "name":"Anakin", "army":"Imperial"}')
    insert('default.users', '2', '{"id":2, "name":"Leia", "army":"Rebellion"}')
    insert('default.users', '3', '{"id":3, "name":"Seth", "army":"Imperial"}')
    insert('default.users', '4', '{"id":4, "name":"Fett", "army":"Freelance"}')
    insert('default.users', '5', '{"id":5, "name":"Solo", "army":"Freelance"}')
    for i in xrange(0, 6):
        print(get('default.users', str(i)))
