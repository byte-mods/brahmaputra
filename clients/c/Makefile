# Brahmaputra C client.
#
#   make                 # libbrahmaputra.a + manual_test (gzip via zlib)
#   make WITH_ZLIB=0     # no zlib: gzip only if registered at runtime
#   make SANITIZE=1      # -fsanitize=address,undefined (separate build dir)
#   make test HOST=127.0.0.1 PORT=9092
#
CC        ?= cc
WITH_ZLIB ?= 1
SANITIZE  ?= 0

BUILD     ?= build
ifeq ($(SANITIZE),1)
BUILD     := build-asan
endif

CFLAGS  ?= -O2 -g
CFLAGS  += -std=c11 -Wall -Wextra -Werror -pedantic -Iinclude -pthread
LDLIBS  += -pthread

ifeq ($(WITH_ZLIB),1)
CFLAGS  += -DBRP_WITH_ZLIB
LDLIBS  += -lz
endif

ifeq ($(SANITIZE),1)
CFLAGS  += -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined
LDFLAGS += -fsanitize=address,undefined
endif

SRCS := src/protocol.c src/client.c src/producer.c src/consumer.c src/group.c
OBJS := $(SRCS:src/%.c=$(BUILD)/%.o)
LIB  := $(BUILD)/libbrahmaputra.a
TEST := $(BUILD)/manual_test

HOST ?= 127.0.0.1
PORT ?= 9092

.PHONY: all clean test stress
all: $(LIB) $(TEST)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/%.o: src/%.c src/internal.h include/brahmaputra.h | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(OBJS)
	$(AR) rcs $@ $^

$(TEST): test/manual_test.c $(LIB) include/brahmaputra.h
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS) $(LIB) $(LDLIBS)

test: $(TEST)
	./$(TEST) $(HOST) $(PORT)

# Repeats the two-member rebalance; see test/rebalance_stress.c.
STRESS := $(BUILD)/rebalance_stress
ITERATIONS ?= 50

$(STRESS): test/rebalance_stress.c $(LIB) include/brahmaputra.h
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS) $(LIB) $(LDLIBS)

stress: $(STRESS)
	./$(STRESS) $(HOST) $(PORT) $(ITERATIONS)

clean:
	rm -rf build build-asan build-cmake build-*
