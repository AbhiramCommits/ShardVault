CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
SANFLAGS ?= -O1 -g -fsanitize=address,undefined

.PHONY: test test-san clean

test_block: test_block.c block.c block.h
	$(CC) $(CFLAGS) -o $@ test_block.c block.c

test: test_block
	./test_block

test_block_san: test_block.c block.c block.h
	$(CC) $(SANFLAGS) -Wall -Wextra -Werror -std=c11 -o $@ test_block.c block.c

test-san: test_block_san
	./test_block_san

clean:
	rm -f test_block test_block_san
