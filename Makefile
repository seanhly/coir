build:
	(mkdir -p bin && cd src && g++ -O3 coir.cpp -o ../bin/coir -lcrypto -lm)
install: build
	mv bin/coir /usr/bin
run-daemon: build
	./bin/coir daemon
