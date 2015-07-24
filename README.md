# presto-riak

[![Build Status](https://api.travis-ci.org/kuenishi/presto-riak.png?branch=master)](https://travis-ci.org/kuenishi/presto-riak)
[![Build Status](https://drone.io/github.com/kuenishi/presto-riak/status.png)](https://drone.io/github.com/kuenishi/presto-riak/latest)

Riak connector for [prestodb](http://prestodb.io). [RP](https://github.com/kuenishi/rp) is for development environment. The latest information and examples are in RP repository.

## build and installation

You need `riak` directory in Presto plugin directory, with
hive-hadoop1 and so on.

```
$ mvn package
$ mv target/presto-riak-<version>.jar path/to/presto/plugin/presto-riak
```


## configuration

Use pb port.

```
$ cat riak.properies
connector.name=riak

riak.pb.host=localhost:8087
riak.erlang.node=riak@127.0.0.1

presto.erlang.node=presto@127.0.0.1
## uses default 'riak'
## presto.erlang.cookie=riak
$ cp riak.properties path/to/presto/etc/catalog
$ ./presto-cli --server localhost:8080 --catalog riak --schema t
```

test - the schema in Riak doesn't appear but it works (it's just because Riak does not have PB api related to bucket types).
That's why bucket type creation before inserting all data is required.

```
presto:t> show catalogs;
 Catalog
---------
 jmx
 riak
(2 rows)
presto:t> use riak.t;
presto:t> show schemas;
       Schema
--------------------
 default
 information_schema
 sys
 t
(3 rows)
```

Now Riak connector working.

run (this is a bit stale result)

```
presto:t> show tables;
    Table
-------------
 foobartable
(1 row)
presto:t> explain select * from foobartable;
                                                  Query Plan
---------------------------------------------------------------------------------------------------------------
 - Output[col1, col2, __pkey]
         __pkey :=
     - TableScan[riak:default:foobartable, original constraint=true] => [col1:varchar, col2:bigint, :varchar]
             col1 := RiakColumnHandle{connectorId=riak, columnName=col1, columnType=STRING, ordinalPosition=0}
             col2 := RiakColumnHandle{connectorId=riak, columnName=col2, columnType=LONG, ordinalPosition=1}
              := RiakColumnHandle{connectorId=riak, columnName=__pkey, columnType=STRING, ordinalPosition=2}

(1 row)
presto:default> select * from foobartable;
 col1 | col2 | __pkey
------+------+--------
 yey  |   34 | k
 yey  | NULL | k3
 yey  |   34 | k2
(3 rows)

presto:default> select * from logs l, users u where l.accessor = u.id;
      timestamp      | method | status | accessor | id | name |   army
---------------------+--------+--------+----------+----+------+-----------
 2014-04-12-00:03:00 | GET    |    200 |        0 |  0 | Solo | Freelance
 2014-04-12-00:03:00 | GET    |    204 |        5 |  5 | Solo | Freelance
 2014-04-12-00:03:00 | GET    |    503 |        4 |  4 | Fett | Freelance
 2014-04-12-00:03:00 | GET    |    404 |        2 |  2 | Solo | Freelance
 2014-04-15-00:04:00 | GET    |    301 |        1 |  1 | Fett | Freelance
 2014-04-15-00:04:00 | GET    |    200 |        5 |  5 | Solo | Freelance
 2014-04-15-00:04:00 | GET    |    200 |        2 |  2 | Solo | Freelance
(7 rows)

Query 20140517_073059_00004_hk6si, FINISHED, 1 node
Splits: 8 total, 8 done (100.00%)
0:00 [6 rows, 258B] [12 rows/s, 522B/s]

presto:default> explain select * from logs l, users u where l.accessor = u.id;
                                                                     Query Plan
----------------------------------------------------------------------------------------------------------------------------------------------------
 - Output[timestamp, method, status, accessor, id, name, army]
     - InnerJoin[("accessor" = "id")] => [timestamp:varchar, method:varchar, status:bigint, accessor:bigint, id:bigint, name:varchar, army:varchar]
         - TableScan[riak:riak:default:logs, original constraint=true] => [timestamp:varchar, method:varchar, status:bigint, accessor:bigint]
                 timestamp := riak:RiakColumnHandle{connectorId=riak, columnName=timestamp, type=varchar, index=false, ordinalPosition=0}
                 method := riak:RiakColumnHandle{connectorId=riak, columnName=method, type=varchar, index=false, ordinalPosition=1}
                 status := riak:RiakColumnHandle{connectorId=riak, columnName=status, type=bigint, index=false, ordinalPosition=2}
                 accessor := riak:RiakColumnHandle{connectorId=riak, columnName=accessor, type=bigint, index=false, ordinalPosition=3}
         - TableScan[riak:riak:default:users, original constraint=true] => [id:bigint, name:varchar, army:varchar]
                 id := riak:RiakColumnHandle{connectorId=riak, columnName=id, type=bigint, index=false, ordinalPosition=0}
                 name := riak:RiakColumnHandle{connectorId=riak, columnName=name, type=varchar, index=false, ordinalPosition=1}
                 army := riak:RiakColumnHandle{connectorId=riak, columnName=army, type=varchar, index=false, ordinalPosition=2}

(1 row)

```

## CLI

Presto doesn't create tables, schemas etc.

```
$ bin/presto-riak-ctl
presto-riak CLI. create table, create schema, drop... >
usage: ./presto-riak-cli <hostname> <port> [<commands>... ]
   list-tables <schema name>
   setup-schema <schema name>
   create-tabledef <schema name> <table definition json file>
   show-tabledef <schema name> <table name>
   clear-tabledef <schema name> <table name>
   check-tabledef <schema name> <table definition json file>
```

Where a table definition json file looks like this (users.json):

```
{"name":"users",
 "columns":[
  {"name":"id", "type":"bigint", "index":true},
  {"name":"name", "type":"varchar", "index":true, "pkey":true},
  {"name":"army", "type":"varchar", "index":true}]}
```

See also logs.json and bin/testdata.py for test data and schema.

# Design and Data layout

Concept correspondence:

- Schema: bucket types - default is default schema
- Table: buckets
- Special Bucket: `__presto_schema`
 - Special Key: `__tables` => list of tables
 - Keys: `(name of a table)` => list of columns

These metadatas are stored in special bucket `__presto_schema` with
special key `__schema` has a JSON like this:

```
{
    "tables": [ "table-a", "table-b" ]
}
```

Each table corresponds to bucket. Table definitions are stored in
special bucket `__presto_schema` with key `<table_name>`. in the key
there is a JSON like this:

```
{
    "columns": [
        {
            "name": "col1",
            "type": "varchar",
            "index": false,
            "pkey": true
        },
        {
            "name": "col2",
            "type": "bigint",
            "index": true
        }
    ],
    "format":"JSON" | "json" | "msgpack" (?) ...
    "name": "spam"
}
```

Any tool to create this style of schema?

## Primary keys

A property `pkey` is a boolean property that indicates the column is
primary key, namely a key in Riak. You can put the property into value
as JSON, but that will be overwritten when you run any query, by Riak
key. If `pkey` is not set in schema, is is just ignored, where you can
refer the Riak key with a column named `__key`. (`__vtag` is also a
column ... that will be implemented in the future)

## Subtables

Given nested JSON object like this.

```
{ "id": 100,
  "name": "Obi One Kenobi",
  "army": "Rebellion",
  "inventory" : [
    { "name" : "Light saber", "weight" : 1.0 },
    { "name" : "Sandal", "weight" : 0.5 }
  ]
}
```

To handle nested JSON objects, `subtables` can be included in a table
schema definition, with JSONpath specified. All nested objects that
match the json path spec are to be handled as single raw of a
subtable.


```
{"name":"users",
 "columns":[
  {"name":"id", "type":"bigint", "index":true},
  {"name":"name", "type":"varchar", "index":true, "pkey":true},
  {"name":"army", "type":"varchar", "index":true}],
  "subtables" :
   [{
     name: "inventory",
     path: "$.inventory.*",
     columns: [
      {"name":"name", "type":"varchar", "index":false},
      {"name":"weight", "type":"float", "index":false}
    ]}]
}
```

With this schema, you can query your inventory like `SELECT count(*)
FROM "users/inventory" GROUP BY __key` , or `SELECT count(*) FROM
"users/inventory" i, users u WHERE u.name = i.__key GROUP BY u.army`.
Please note that the latter runs join operation, where all keys are
read twice. This is something hard to optimize, a future work.

- [Goessner's JSONPATH spec](http://goessner.net/articles/JsonPath/)
- [JOSNPATH Expression Tester](http://jsonpath.curiousconcept.com)
- [Java port of JSONPATH](https://github.com/jayway/JsonPath)

## Types supported

Correspondence from JSON to SQL types,

- String as VARCHAR,
- Numeric (Integer) as BIGINT,
- Numeric (Double) as DOUBLE

and no support for timestamps, maps, arrays for now.

## Optimization

There can be several levels of optimization.

- 2i configuration
- Operator/predicate pushdown

Predicates are passed to `RiakSplitManager` (implements
`ConnectorSplitManager`) as `TupleDomain` . It includes all
information for selection. Normal connector generates splits in
`ConnectorSplitManager.getPartitions()` , splits are maybe with HDFS
location associated with predicates or partition keys.


## Notes and Future

- Correctness
 - grep TODO
 - docker testing with distributed setup, runs presto-verifier

- Performance
 - remove OTP's JInterface and replace with other fast IPC
 - columnar backend format other than leveldb

- Usability
 - introduce switch between pure-PB API mode

## dev setup

Riak

```sh
$ git clone git:github.com/basho/riak
$ cd riak
$ make stage
$ sed -e 's/storage_backend = bitcask/storage_backend = leveldb/' -i.back rel/riak/etc/riak.conf
$ sed -e 's/## ring_size = 64/ring_size = 8/' -i.back rel/riak/etc/riak.conf
$ cp ldna.beam rel/riak/lib/basho-patches
$ ulimit -n 4096
$ rel/riak/bin/riak start
$ rel/riak/bin/riak-admin bucket-type create <schemaname>
$ rel/riak/bin/riak-admin bucket-type activate <schemaname>
```


# Version compatibility

| Prestodb | presto-riak | Riak  |
|:---------|:------------|:------|
| 0.88     | 0.0.5       | 2.0.2 |
| 0.100    | 0.0.6       | 2.0.5 |
| 0.112    | 0.1.0       | 2.1.1 |

# rights

Distributed under Apached 2.0 license, (C) Kota UENISHI.
