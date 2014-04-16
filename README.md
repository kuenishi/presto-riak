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
> show catalogs;
 Catalog
---------
 jmx
 riak
(2 rows)
> use catalog riak;
> show schemas;
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
> show tables;
    Table
-------------
 foobartable
(1 row)
> explain select * from foobartable;
                                                  Query Plan
---------------------------------------------------------------------------------------------------------------
 - Output[col1, col2, __pkey]
         __pkey :=
     - TableScan[riak:default:foobartable, original constraint=true] => [col1:varchar, col2:bigint, :varchar]
             col1 := RiakColumnHandle{connectorId=riak, columnName=col1, columnType=STRING, ordinalPosition=0}
             col2 := RiakColumnHandle{connectorId=riak, columnName=col2, columnType=LONG, ordinalPosition=1}
              := RiakColumnHandle{connectorId=riak, columnName=__pkey, columnType=STRING, ordinalPosition=2}

(1 row)
> select * from foobartable;
 col1 | col2 | __pkey
------+------+--------
 yey  |   34 | k
 yey  | NULL | k3
 yey  |   34 | k2
(3 rows)
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

- optimization: pushing-down predicates would make it
  much faster; or changing index to find keys
  or custom mapreduce each time? Anyway, for now
  there are no means to know join keys or predicates
  in the backend.
- interface: how can we force users 2i properly set?
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
