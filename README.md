# presto-riak

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
riak.hosts=localhost:8087
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
bucket properties.

```
{
  "props" :
  {
    "__presto_schema" : { ... }
    }
}
```

At startup time, presto-riak creates it all if not existing.

## TODOs

- modes: PB API mode and direct mode. Former is just a
  adaptor and no distributed processing over Presto.
  The latter uses direct access to Erlang node, then
  chooses a plan to run.
- locality: change Split, to use jinterface and design
  a coverage operation _in Java_.
- optimization: pushing-down predicates would make it
  much faster; or changing index to find keys
  or custom mapreduce each time? Anyway, for now
  there are no means to know join keys or predicates
  in the backend.
- custom backend: bitcask nor leveldb are not optimized
  for full scanning. A backend that supports columnar
  data format like parquet / ORCFile would make it faster
