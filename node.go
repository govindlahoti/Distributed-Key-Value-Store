package main

import (
	"fmt"
	// "errors"
	"net"
	// "time"
	// "net/rpc"
	"net/rpc/jsonrpc"
	"math"
	// "sort"
	"net/rpc"
	"os"
	"time"
	"log"
	"strings"
)


type Node struct {
	
	fingerTable []string
	nodeId uint64

	// addresses
	address string
	successors []string
	// predecessr string

	startRange uint64
	endRange uint64
	keyValueStore map[string]string

	//log *log.Logger
}


func (node *Node) lookUpFingerTable(key uint64) string {

	var target uint64
	if key > node.nodeId {
		target = uint64(math.Log2(float64(key - node.nodeId)))
	} else {
		target = uint64(math.Log2(float64(key + (uint64(1)<<32) - node.nodeId)))
	}
	fmt.Println(target,",",len(node.fingerTable),',',node.fingerTable[target])
	var targetLookUp string

	client,err := getClient(node.fingerTable[target])
	if err==nil {
		err=client.Call("Node.IpLookUp", key, &targetLookUp)
	}
	fmt.Println(target)

	for err != nil {
		fmt.Println(err)
		target = (target - 1 + 32) % 32
		client,err := getClient(node.fingerTable[target])
		if err==nil {
			err=client.Call("Node.IpLookUp", key, &targetLookUp)
		}
	}

	return targetLookUp
}

func (node *Node) IpLookUp (key uint64, addr *string) error {
	
	if key >= node.startRange && key <= node.endRange {
		fmt.Println("base case :)")
		*addr = node.address
	} else{
		*addr = node.lookUpFingerTable(key)
	}

	return nil
}

func (node *Node) LookUp(key string, value *string) error {
	
	var err error
	var client *rpc.Client
	err=nil
	hash := consistentHash(key)
	if hash >= node.startRange && hash <= node.endRange {
		*value = node.keyValueStore[key]
	} else {
		var targetIp string
		node.IpLookUp(hash, &targetIp)
		client,err=getClient(targetIp)
		if err==nil{
			err=client.Call("Node.LookUp", key, value)
		}
	}

	return err
}

func (node *Node) UpdateKey(key string, value *string) error {
	
	var err error
	var client *rpc.Client
	err=nil
	hash := consistentHash(key)
	if hash >= node.startRange && hash <= node.endRange {
		node.keyValueStore[key]=*value
	} else {
		var targetIp string
		node.IpLookUp(hash, &targetIp)
		client,err=getClient(targetIp)
		if err==nil{
			err=client.Call("Node.LookUp", key, value)
		}
	}

	return err
}

func (node *Node) updateFingerTable() {

	fmt.Println("here in updateFingerTable")
	node.fingerTable[0] = node.successors[0]
	fmt.Println("here in updateFingerTable first reference")
	for i := 1; i < 32; i++ {
		var target string
		node.IpLookUp(node.nodeId + (uint64(1)<<uint(i)), &target)
		fmt.Println("here in updateFingerTable reference")
		node.fingerTable[i] = target
		fmt.Println("here in updateFingerTable reference")
	}

}

func (node *Node) Join(addr string, newnode *Node) error {
	// TODO Search for node with most need
	var temp Node

	newnode.nodeId = node.nodeId
	newnode.startRange = (node.startRange + node.endRange)/2 + 1
	newnode.endRange = newnode.nodeId
	newnode.address = addr
	
	copy(newnode.successors, node.successors)
	copy(newnode.fingerTable, node.fingerTable)
	// hashes = make([]uint64)
	// for k,v := range keyValueStore {
	// 	hashes = append(hashes,consistentHash(k))
	// }
	// sort.Sort(hashes)
	// if(len(hashes)==0){
	// }
	// else{
	// 	nodeId=hashes[len(hashes)/2]
	// }

	/* pass finger table */
	/* start end range */
	temp.nodeId = (node.startRange + node.endRange)/2
	temp.endRange = temp.nodeId
	copy(temp.fingerTable, node.fingerTable)
	copy(temp.successors[1:], temp.successors[0:])
	temp.successors[0] = addr
	

	temp.fingerTable[0] = temp.successors[0]

	var i uint
	for i = 1; i < 32; i++ {
		if temp.nodeId + (uint64(1)<<i) <= newnode.nodeId {
			temp.fingerTable[i] = temp.successors[0]
		} else {
			break
		}
	}

	for ; i < 32; i++ {
		var target string
		temp.IpLookUp(temp.nodeId + (uint64(1)<<i), &target)
		temp.fingerTable[i] = target
	}

	
	/* key val store distributed */
	newnode.keyValueStore = make(map[string]string)
	for k, v := range node.keyValueStore {
		if consistentHash(k) > temp.nodeId {
			newnode.keyValueStore[k] = v
		} 
	}

	for k,_ := range newnode.keyValueStore {
		delete(node.keyValueStore, k)
	}

	node.nodeId = temp.nodeId
	node.startRange = temp.startRange
	node.endRange = temp.endRange
	node.fingerTable = temp.fingerTable
	node.successors = temp.successors

	return nil
}

func (node *Node) UpdateSuccessors(addr string, successors *[]string) error {
	// fmt.Println(*successors);
	// fmt.Println(node.successors);
	*successors = make([]string, len(node.successors))
	copy(*successors, node.successors);
	fmt.Println(*successors);
	// fmt.Println("here");
	copy((*successors)[1:], (*successors)[0:]);
	(*successors)[0] = node.address
	// fmt.Println("here after copy");
	return nil
}



func (node *Node) Leave(addr string, newnode *Node) error {
	// TODO send to successor
	node.startRange = newnode.startRange
	for k, v := range newnode.keyValueStore {
		node.keyValueStore[k] = v
	}
	return nil
}

func (node *Node) periodicUpdater() {
	fmt.Println("here in periodicUpdater")
	for true {

		for i := 0; i < len(node.successors); i++ {
			// fmt.Println("here in periodicUpdater i=",i)
			client,err:=getClient(node.successors[i])
			if err==nil{
				err=client.Call("Node.UpdateSuccessors", node.address, &node.successors)
			}
			if err == nil {
				break
			}
		}
		
		fmt.Println("here in periodicUpdater after successors update")
		node.updateFingerTable()
		fmt.Println("here in periodicUpdater finger table upto date update")
		time.Sleep(10000 * time.Millisecond)
	}
}

func tcpServer(port string){
	addr, err := net.ResolveTCPAddr("tcp", port)
	if err != nil {
		fmt.Println(err)
	}

	inbound, err := net.ListenTCP("tcp", addr)
	if err != nil {
		fmt.Println(err)
	}
	
	for true {
		conn, err := inbound.Accept()
		
		if err != nil {
			fmt.Println(err)
		}

		go jsonrpc.ServeConn(conn)

	}
}


func main() {
	// tcpServer()

	/* creating the log file */
	/*logfile, err := os.OpenFile(os.Args[1]+".log",  os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logg := log.New(logfile, "", log.Ltime)
	if err != nil {
	    fmt.Println("File does not exists or cannot be created and unable to create log")
	    os.Exit(1)
	}
	defer logfile.Close()*/
			
	/* setup master node */
	masterNode := new(Node)
	if(strings.Compare(os.Args[2],"master")==0){
		fmt.Println("here in master node creation")
		//masterNode.log = logg
		masterNode.fingerTable = make([]string, 32)
		masterNode.successors = make([]string,1)
		masterNode.successors[0]=os.Args[1]
		masterNode.startRange = 0
		masterNode.endRange = (uint64(1)<<32)-1
		masterNode.address=os.Args[1]
		masterNode.nodeId=(uint64(1)<<32)-1
		masterNode.keyValueStore = make(map[string]string)
		fmt.Println("here in master node creation done")
	} else {
		client,err:=getClient(os.Args[3])
		if err==nil{
			err=client.Call("Node.Join",os.Args[1],masterNode)
		}
		if err != nil {
			log.Fatal("Unable to join.");
		}
	}
	rpc.Register(masterNode)
	go masterNode.periodicUpdater()
	tcpServer(os.Args[1])
}