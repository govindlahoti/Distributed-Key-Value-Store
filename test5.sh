rm -rf log.txt
go build node.go utils.go
echo "master"
./node :8000 master > 8000.log &
sleep 5
echo "server1"
./node :8001 :8000 > 8001.log &
sleep 5
echo "server2"
./node :8002 :8001 > 8002.log &
sleep 5
echo "server3"
./node :8003 :8001 > 8003.log &
sleep 5
echo "server4"
./node :8004 :8002 > 8004.log &
sleep 5
echo "server5"
./node :8005 :8003 > 8005.log &
sleep 5
echo "test"
go build test5g.go
./test5g > test5g.log