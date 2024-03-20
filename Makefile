build:
	(mkdir -p bin && cd src && g++ -O3 librecoir.cpp -o ../bin/librecoir -lcrypto -lm)
install: build
	sudo ./src/install.sh
run-daemon: build
	./bin/librecoir daemon
