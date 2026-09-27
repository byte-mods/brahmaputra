# Builds with plain erlc so rebar3 is not required. rebar3 users can
# instead depend on this directory as an ordinary OTP application.
ERLC ?= erlc
ERLC_FLAGS = -Werror +warn_unused_vars +debug_info -I include

SRC  = $(wildcard src/*.erl)
BEAM = $(patsubst src/%.erl,ebin/%.beam,$(SRC))
TEST_SRC  = $(wildcard test/*.erl)
TEST_BEAM = $(patsubst test/%.erl,test/%.beam,$(TEST_SRC))
MODULES = $(shell ls src/*.erl | sed "s|src/||; s|\.erl||" | paste -sd, -)

HOST ?= 127.0.0.1
PORT ?= 9092

.PHONY: all test e2e clean

all: $(BEAM) ebin/brahmaputra.app

ebin:
	mkdir -p ebin

ebin/%.beam: src/%.erl include/brahmaputra.hrl | ebin
	$(ERLC) $(ERLC_FLAGS) -o ebin $<

ebin/brahmaputra.app: src/brahmaputra.app.src | ebin
	sed 's/{modules, \[\]}/{modules, [$(MODULES)]}/' $< > $@

test/%.beam: test/%.erl all
	$(ERLC) $(ERLC_FLAGS) -o test $<

test: $(TEST_BEAM)

e2e: test
	erl -noshell -pa ebin -pa test -eval 'brahmaputra_manual_test:main(["$(HOST)", "$(PORT)"])'

clean:
	rm -rf ebin test/*.beam
