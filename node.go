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
	
	FingerTable []string
	NodeId uint64

	// addresses
	Address string
	Successors []string
	// predecessr string

	StartRange uint64
	EndRange uint64
	KeyValueStore map[string]string

	//log *log.Logger
}


func (node *Node) lookUpFingerTable(key uint64) string {

	// fmt.Println("lookUpFingerTable called with key =", key)
	var target uint64
	if key > node.NodeId {
		target = uint64(math.Log2(float64(key - node.NodeId)))
	} else {
		target = uint64(math.Log2(float64(key + (uint64(1)<<32) - node.NodeId)))
	}
	
	var targetLookUp string

	// fmt.Println("Found the location for next hop =", node.FingerTable[target])
	// fmt.Println("Will contact it")
	client, err := getClient(node.FingerTable[target])

	if err == nil {
		err = client.Call("Node.IpLookUp", key, &targetLookUp)
	}
	
	for err != nil {
		// fmt.Println("Problem contacting it")
		target = (target - 1 + 32) % 32
		// fmt.Println("Found the location for previous best hop =", node.FingerTable[target])
		// fmt.Println("Will contact it")
		client, err := getClient(node.FingerTable[target])
		if err == nil {
			err = client.Call("Node.IpLookUp", key, &targetLookUp)
		}
	}

	// fmt.Println("Final destination for", key, "was found to be at", targetLookUp)
	return targetLookUp
}

func (node *Node) IpLookUp (key uint64, addr *string) error {
	
	// fmt.Println("IpLookUp called with key =", key)
	
	if key >= node.StartRange && key <= node.EndRange {
		*addr = node.Address 
	} else{
		*addr = node.lookUpFingerTable(key)
	}

	// fmt.Println("IpLookUp returns the destination =", *addr)
	return nil
}

func (node *Node) LookUp(key string, value *string) error {
	
	fmt.Println("LookUp called for key =", key)
	var err error
	var client *rpc.Client
	err = nil
	hash := consistentHash(key)
	if hash >= node.StartRange && hash <= node.EndRange {
		*value = node.KeyValueStore[key]
	} else {
		var targetIp string
		node.IpLookUp(hash, &targetIp)
		client,err=getClient(targetIp)
		if err == nil{
			err=client.Call("Node.LookUp", key, value)
		}
	}

	fmt.Println("Lookup resulted into value =", *value)
	return err
}

func (node *Node) UpdateKey(keyValue []string, dummy *int) error {
	
	fmt.Println("Add request came for key =", keyValue[0], "value =", keyValue[1])

	var err error
	var client *rpc.Client
	err = nil

	hash := consistentHash(keyValue[0])

	if hash >= node.StartRange && hash <= node.EndRange {
		node.KeyValueStore[keyValue[0]] = keyValue[1]
		fmt.Println("Added - ", keyValue[0], ": ", keyValue[1])
	} else {
		var targetIp string
		node.IpLookUp(hash, &targetIp)
		client,err = getClient(targetIp)
		if err == nil{
			err = client.Call("Node.UpdateKey", keyValue, dummy)
		}
	}

	return err
}

func (node *Node) updateFingerTable() {

	// fmt.Println("Periodic Fingure Table Update")
	node.FingerTable[0] = node.Successors[0]
	
	for i := 1; i < 32; i++ {
		var target string
		node.IpLookUp((node.NodeId + power2(i)) % power2(32), &target)
		node.FingerTable[i] = target
	}

}

func (node *Node) init() {
	node.FingerTable = make([]string, 32)
	node.Successors = make([]string, 1)
	node.KeyValueStore = make(map[string]string)
}

func (node *Node) Join(addr string, newnode *Node) error {
	// TODO Search for node with most need

	newnode.init()
	fmt.Println(newnode.Successors)
	fmt.Println(newnode.FingerTable)
	newnode.NodeId = node.NodeId
	newnode.StartRange = (node.StartRange + node.EndRange)/2 + 1
	newnode.EndRange = newnode.NodeId
	newnode.Address = addr
	
	copy(newnode.Successors, node.Successors)
	copy(newnode.FingerTable, node.FingerTable)

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
	
	var temp Node
	temp.init()
	temp.NodeId = (node.StartRange + node.EndRange)/2
	temp.StartRange = node.StartRange
	temp.EndRange = temp.NodeId
	copy(temp.FingerTable, node.FingerTable)
	copy(temp.Successors[1:], temp.Successors[0:])
	temp.Successors[0] = addr

	// create a new fingure table
	var i int
	for i = 0; i < 32; i++ {
		if temp.NodeId + power2(i) <= newnode.NodeId {
			temp.FingerTable[i] = temp.Successors[0]
			// fmt.Println(i,temp.NodeId,temp.NodeId + power2(i),newnode.NodeId)
		} else {
			break
		}
	}

	for ; i < 32; i++ {
		var target string
		temp.IpLookUp((temp.NodeId + power2(i)) % power2(32), &target)
		temp.FingerTable[i] = target
	}

	
	/* key val store distributed */
	newnode.KeyValueStore = make(map[string]string)
	for k, v := range node.KeyValueStore {
		if consistentHash(k) > temp.NodeId {
			newnode.KeyValueStore[k] = v
		} 
	}

	for k,_ := range newnode.KeyValueStore {
		delete(node.KeyValueStore, k)
	}

	fmt.Println(temp)
	fmt.Println(newnode)
	node.NodeId = temp.NodeId
	node.StartRange = temp.StartRange
	node.EndRange = temp.EndRange
	node.FingerTable = temp.FingerTable
	node.Successors = temp.Successors

	return nil
}

func (node *Node) UpdateSuccessors(addr string, successors *[]string) error {

	*successors = make([]string, len(node.Successors))
	copy(*successors, node.Successors);
	fmt.Println(*successors);
	copy((*successors)[1:], (*successors)[0:]);
	(*successors)[0] = node.Address
	return nil
}



func (node *Node) Leave(addr string, newnode *Node) error {
	// TODO send to successor
	node.StartRange = newnode.StartRange
	for k, v := range newnode.KeyValueStore {
		node.KeyValueStore[k] = v
	}
	return nil
}

func (node *Node) periodicUpdater() {

	for true {

		for i := 0; i < len(node.Successors); i++ {
			client, err := getClient(node.Successors[i])
			if err == nil{
				err = client.Call("Node.UpdateSuccessors", node.Address, &node.Successors)
			}
			if err == nil {
				break
			}
		}
		node.updateFingerTable()
		node.printFingerTable()
		node.printDetails()
		time.Sleep(10000 * time.Millisecond)
	}
}

func (node *Node) printDetails() {
	fmt.Println("Range = [", node.StartRange, ",", node.EndRange, "]")
}

func (node *Node) printFingerTable(){
	fmt.Println("------Finger Table------")
	for i:=0;i<32;i++ {
		fmt.Println((node.NodeId+power2(i))%power2(32),node.FingerTable[i])
	}
	fmt.Println("-------------------------")
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
	
	node := new(Node)
	node.init()
	fmt.Println(node)	
	if(strings.Compare(os.Args[2], "master") == 0) {
		// fmt.Println("here in master node creation")
		//node.log = logg
		node.Successors[0] = os.Args[1]
		node.StartRange = 0
		node.EndRange = power2(32) - 1
		node.Address = os.Args[1]
		node.NodeId = power2(32) - 1
		fmt.Println("here in master node creation done")
	} else {
		client,err:=getClient(os.Args[2])
		var newnode Node
		newnode.init()
		if err == nil {
			err=client.Call("Node.Join", os.Args[1], &newnode)
			*node=newnode
		} else {
			log.Fatal("Unable to join.");
		}
		fmt.Println(node)
	}
	
	rpc.Register(node)
	go node.periodicUpdater()
	tcpServer(os.Args[1])
}