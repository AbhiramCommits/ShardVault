CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11

.PHONY: test clean

test_block: test_block.c block.c block.h
	$(CC) $(CFLAGS) -o $@ test_block.c block.c

test: test_block
	./test_block

clean:
	rm -f test_block
