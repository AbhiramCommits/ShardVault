.PHONY: test test-c test-rust clean

test: test-c test-rust

test-c:
	$(MAKE) -C csrc test

test-rust:
	cargo test

clean:
	$(MAKE) -C csrc clean
	cargo clean
