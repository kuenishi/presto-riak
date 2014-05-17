# presto-riak

Riak connector for [prestodb](http://prestodb.io). [RP](https://github.com/kuenishi/rp) is for development environment.

## build and installation

You need `riak` directory in Presto plugin directory, with
hive-hadoop1 and so on.

```
$ mvn package assembly:assembly
$ cd target
$ unzip presto-riak-*.zip
$ mv *.jar path/to/presto/plugin/riak
```


## configuration

Use pb port.

```
$ cat riak.properies
connector.name=riak

riak.pb.host=localhost:8087
riak.erlang.node=riak@127.0.0.1

presto.erlang.node=presto@127.0.0.1
presto.erlang.cookie=riak
$ cp riak.properties path/to/presto/etc/catalog
$ ./presto-cli --server localhost:8008 --catalog riak
```

test

```
presto:default> show catalogs;
 Catalog
---------
 jmx
 riak
(2 rows)
presto:default> use catalog riak;
presto:default> show schemas;
       Schema
--------------------
 default
 information_schema
 sys
(3 rows)
```

Now Riak connector working.

run

```
presto:default> show tables;
    Table
-------------
 foobartable
(1 row)
presto:default> explain select * from foobartable;
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

presto:default> select * from logs cross join users where logs.accessor = users.id;
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

presto:default> explain select * from logs cross join users where logs.accessor = users.id;
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
$ mvn exec:java -Dexec.mainClass=com.basho.riak.presto.CLI
```

# Design

Concept correspondence:

- Schema: bucket types - default is default
- Table: buckets

These metadatas are stored in special bucket `__presto_schema` with
key `default` for default, and special key `__schema` includes all
schemas including default schema. Each schema key include table names
as JSON like this:

```
{
    "tables":{
        "table-a" : ["special properties here"],
        "table-b" : []
    }
}
```

Each table corresponds to bucket. Table definitions are stored in
special bucket `__presto_schema` with key
`<schema_name>.<table_name>}`. in the key there is a JSON

```
{
    "columns": [
        {
            "name": "col1",
            "type": "STRING",
            "index": false
        },
        {
            "name": "col2",
            "type": "LONG",
            "index": true
        },
        {
            "name": "__pkey",
            "type": "STRING",
            "index":false
        }
    ],
    "format":"JSON", | "json" | "msgpack" (?) ...
    "name": "spam"
}
```

Any tool to create this style of schema?

## Optimization

There can be several levels of optimization.

- 2i configuration
- Operator/predicate pushdown

Predicates are passed to `RiakSplitManager` (implements
`ConnectorSplitManager`) as `TupleDomain` . It includes all
information for selection. Normal connector generates splits in
`ConnectorSplitManager.getPartitions()` , splits are maybe with HDFS
location associated with predicates or partition keys.

## TODOs

- sibling handling: can they be handled as multiple tuples?
  or never use allow_mult, choose first, or whatever.
- optimization: pushing-down predicates would make it
  much faster; or changing index to find keys
  or custom mapreduce each time? Anyway, for now
  there are no means to know join keys or predicates
  in the backend.
   => maybe HyperLogLog in Riak, or eleveldb ????
- currently OtpConnection is protected with `synchronized`
  method in DirectConnection#call(). Needs change with
  request id control.
- nested JSON objects: handle it as a single column with
  concatinated name.
- modes: PB API mode and direct mode. Direct mode seems now
  working while PB API mode is cut off. Needs rewrite.
- custom backend: bitcask nor leveldb are not optimized
  for full scanning. A backend that supports columnar
  data format like parquet / ORCFile would make it faster

## DONE

- interface: how can we force users 2i properly set? - nothing.


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
```
