
all:
	rm -f *.log
	go build node.go utils.go
	go build throughput_tests.go utils.go