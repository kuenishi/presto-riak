RIAK_HOME=../prc/riak/rel/riak

all: package ldna.beam
	@cp ldna.beam $(RIAK_HOME)/lib/basho-patches

package:
	@mvn package

ldna.beam: src/main/erlang/ldna.erl
	ERL_LIBS=$(RIAK_HOME)/lib $(RIAK_HOME)/erts-*/bin/erlc $<
