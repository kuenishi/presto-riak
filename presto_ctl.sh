#!/bin/sh

set -e

RIAK=$1
CMD=$2

echo $1
echo $2

# ping
echo "ping:"
curl http://$RIAK/ping
echo ""

# init
# curl -i "http://$RIAK/buckets/__presto_schema/keys/__schema"
curl -XPUT "http://$RIAK/buckets/__presto_schema/keys/__schema" \
  -H "content-type: application/json" \
  -d '{"tables":["foobartable", "spam"]}'

curl "http://$RIAK/buckets/__presto_schema/keys/__schema"

curl -XPUT "http://$RIAK/buckets/__presto_schema/keys/default.foobartable" \
  -H "content-type: application/json" \
  -d '{"name":"foobartable", "columns":[{"name":"col1","type":"STRING"},{"name":"col2","type":"LONG"},{"name":"__pkey","type":"STRING"}]}'

curl -XPUT "http://$RIAK/buckets/__presto_schema/keys/default.spam" \
  -H "content-type: application/json" \
  -d '{"name":"spam", "columns":[{"name":"col1","type":"STRING"},{"name":"col2","type":"LONG"},{"name":"__pkey","type":"STRING"}]}'

curl "http://$RIAK/buckets/__presto_schema/keys/default.foobartable"
curl "http://$RIAK/buckets/__presto_schema/keys/default.spam"

echo "try 'select * from spam s cross join foobartable f where s.col1=f.col1;'"