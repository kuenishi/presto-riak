RIAK_HOME=../rp/riak/rel/riak
PRESTO_HOME=../rp
PRESTO_PLUGIN_DIR=$(PRESTO_HOME)/plugin/presto-riak/

all: deploy

list-deps:
	@mvn dependency:tree -Dverbose

deploy: package ldna.beam
	@cp ldna.beam $(RIAK_HOME)/lib/basho-patches
	@cd target && unzip -o presto-riak*.zip
	@mkdir -p $(PRESTO_PLUGIN_DIR)
	@cp target/*.jar $(PRESTO_PLUGIN_DIR)

package:
	@mvn package

ldna.beam: src/main/erlang/ldna.erl
	ERL_LIBS=$(RIAK_HOME)/lib $(RIAK_HOME)/erts-*/bin/erlc $<

start-test:
	$(RIAK_HOME)/bin/riak start
	$(PRESTO_HOME)/bin/launcher start

stop-test:
	$(RIAK_HOME)/bin/riak stop
	$(PRESTO_HOME)/bin/launcher stop
