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
	"sync"
)


type Node struct {
	
	FingerTable []string
	NodeId uint64

	Address string
	Successors []string
	
	StartRange uint64
	EndRange uint64
	KeyValueStore map[string]string

}

var nodelock sync.Mutex

func (node *Node) lookUpFingerTable(key uint64) string {

	// fmt.Println("lookUpFingerTable called with key =", key)
	var target uint64

	nodelock.Lock()
	if key > node.NodeId {
		target = uint64(math.Log2(float64(key - node.NodeId)))
	} else {
		target = uint64(math.Log2(float64(key + power2(32) - node.NodeId)))
	}
	
	var targetLookUp string

	// fmt.Println("Found the location for next hop =", node.FingerTable[target])
	// fmt.Println("Will contact it")
	finger := node.FingerTable[target]
	nodelock.Unlock()
	// node.printFingerTable()
	// node.printDetails()

	client, err := getClient(finger)

	if err == nil {
		err = client.Call("Node.IpLookUp", key, &targetLookUp)
		client.Close()
	}
	
	if err != nil {
		// fmt.Println("Problem contacting it")
		target = (target - 1 + 32) % 32
		// fmt.Println("Found the location for previous best hop =", node.FingerTable[target])
		// fmt.Println("Will contact it")
		node.printFingerTable()
		nodelock.Lock()
		finger = node.FingerTable[target]
		nodelock.Unlock()
		
		client, err := getClient(finger)
		if err == nil {
			err = client.Call("Node.IpLookUp", key, &targetLookUp)
			client.Close()
		}
	}

	// fmt.Println("Final destination for", key, "was found to be at", targetLookUp)
	return targetLookUp
}

func (node *Node) IpLookUp (key uint64, addr *string) error {
	
	// fmt.Println("IpLookUp called with key =", key)
	nodelock.Lock()
	if node.inRange(key) {
		nodelock.Unlock()
		*addr = node.Address 
	} else{
		nodelock.Unlock()
		*addr = node.lookUpFingerTable(key)
	}

	// fmt.Println("IpLookUp returns the destination =", *addr)
	return nil
}

func (node *Node) LookUp(key string, value *string) error {
	
	// fmt.Println("LookUp called for key =", key)
	var err error
	var client *rpc.Client
	err = nil
	hash := consistentHash(key)

	nodelock.Lock()
	if node.inRange(hash) {
		*value = node.KeyValueStore[key]
		nodelock.Unlock()
		time.Sleep(1000 * time.Millisecond)
	} else {
		nodelock.Unlock()
		var targetIp string
		node.IpLookUp(hash, &targetIp)
		client, err = getClient(targetIp)
		if err == nil{
			err = client.Call("Node.LookUp", key, value)
			client.Close()
		}
	}

	// fmt.Println("Lookup resulted into value =", *value)
	return err
}

func (node *Node) UpdateKey(keyValue []string, dummy *int) error {
	
	fmt.Println("Add request came for key =", keyValue[0], "value =", keyValue[1])

	var err error
	var client *rpc.Client
	err = nil

	hash := consistentHash(keyValue[0])

	nodelock.Lock()
	if node.inRange(hash) {
		node.KeyValueStore[keyValue[0]] = keyValue[1]
		nodelock.Unlock()
		time.Sleep(1000 * time.Millisecond)
		fmt.Println("Added - ", keyValue[0], ": ", keyValue[1])
	} else {
		nodelock.Unlock()
		var targetIp string
		node.IpLookUp(hash, &targetIp)
		client, err = getClient(targetIp)
		if err == nil{
			err = client.Call("Node.UpdateKey", keyValue, dummy)
			client.Close()
		}
	}

	return err
}

func (node *Node) updateFingerTable() {

	// fmt.Println("Periodic Fingure Table Update")
	nodelock.Lock()
	node.FingerTable[0] = node.Successors[0]
	nodelock.Unlock()
	for i := 1; i < 32; i++ {
		var target string
		node.IpLookUp((node.NodeId + power2(i)) % power2(32), &target)
		nodelock.Lock()
		node.FingerTable[i] = target
		nodelock.Unlock()
	}

}

func (node *Node) init() {
	node.FingerTable = make([]string, 32)
	node.Successors = make([]string, 10)
	node.KeyValueStore = make(map[string]string)
}

func (node *Node) Join(addr string, newnode *Node) error {
	// TODO Search for node with most need

	newnode.init()
	fmt.Println(newnode.Successors)
	fmt.Println(newnode.FingerTable)

	nodelock.Lock()
	newnode.NodeId = node.NodeId
	newnode.StartRange = (node.StartRange + (node.EndRange - node.StartRange + power2(32))%power2(32) /2 + 1) % power2(32)
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
	temp.NodeId = (node.StartRange + (node.EndRange - node.StartRange + power2(32))%power2(32) /2 ) % power2(32)
	temp.StartRange = node.StartRange
	temp.EndRange = temp.NodeId
	copy(temp.FingerTable, node.FingerTable)
	copy(temp.Successors[1:], node.Successors[0:])
	temp.Successors[0] = addr
	nodelock.Unlock()

	// create a new finger table
	var i int
	for i = 0; i < 32; i++ {
		if newnode.inRange((temp.NodeId + power2(i))%power2(32)) {
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

	
	nodelock.Lock()

	/* key val store distributed */
	newnode.KeyValueStore = make(map[string]string)
	for k, v := range node.KeyValueStore {
		if newnode.inRange(consistentHash(k)) {
			newnode.KeyValueStore[k] = v
		} 
	}

	for k,_ := range newnode.KeyValueStore {
		delete(node.KeyValueStore, k)
	}


	fmt.Println("In join node is temp Successors=",temp.Successors,"FingerTable=",temp.FingerTable)
	fmt.Println("In join node is temp Successors=",newnode.Successors,"FingerTable=",newnode.FingerTable)
	node.NodeId = temp.NodeId
	node.StartRange = temp.StartRange
	node.EndRange = temp.EndRange
	node.FingerTable = temp.FingerTable
	node.Successors = temp.Successors
	nodelock.Unlock()
	return nil
}

func (node *Node) UpdateSuccessors(end uint64, successors *[]string) error {

	nodelock.Lock()
	
	*successors = make([]string, len(node.Successors))
	copy(*successors, node.Successors);
	fmt.Println(*successors);
	copy((*successors)[1:], (*successors)[0:]);
	(*successors)[0] = node.Address

	if end < power2(32) {
		node.StartRange = (end+1)%power2(32)
	}
	
	nodelock.Unlock()

	return nil
}



func (node *Node) Leave(addr string, newnode *Node) error {
	// TODO send to successor
	nodelock.Lock()
	node.StartRange = newnode.StartRange
	for k, v := range newnode.KeyValueStore {
		node.KeyValueStore[k] = v
	}
	nodelock.Unlock()
	return nil
}

func (node *Node) periodicUpdater() {

	for true {

		nodelock.Lock()
		r := len(node.Successors)
		nodelock.Unlock()

		for i := 0; i < r; i++ {
			nodelock.Lock()
			succ := node.Successors[i]
			nodelock.Unlock()

			client, err := getClient(succ)
			if err == nil{
				
				temp := make([]string, r)
				
				var end uint64

				if i > 0 {
					nodelock.Lock()
					end = node.EndRange
					nodelock.Unlock()
				} else {
					end = power2(32)
				}

				err = client.Call("Node.UpdateSuccessors", end, &temp)
				client.Close()

				if err == nil {
					nodelock.Lock()
					copy(node.Successors, temp)
					nodelock.Unlock()
					break
				}
			}
		}

		node.updateFingerTable()
		// node.printFingerTable()
		// node.printDetails()
		time.Sleep(1000 * time.Millisecond)
	}
}

func (node *Node) inRange(key uint64) bool {
	if node.StartRange <= node.EndRange {
		return node.StartRange <= key && key <= node.EndRange
	} else {
		return node.StartRange <= key || key <= node.EndRange   
	}
}

func (node *Node) printDetails() {
	nodelock.Lock()
	fmt.Println("Range = [", node.StartRange, ",", node.EndRange, "]")
	nodelock.Unlock()
}

func (node *Node) printFingerTable(){
	nodelock.Lock()
	fmt.Println("------Finger Table------")
	for i:=0;i<32;i++ {
		fmt.Println((node.NodeId+power2(i)) % power2(32), node.FingerTable[i])
	}
	fmt.Println("-------------------------")
	nodelock.Unlock()
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
	nodelock = sync.Mutex{}
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
		client, err := getClient(os.Args[2])
		var newnode Node
		newnode.init()

		if err == nil {
			err = client.Call("Node.Join", os.Args[1], &newnode)
			client.Close()
			*node = newnode
		} else {
			log.Fatal("Unable to join.");
		}
		fmt.Println(node)
	}
	
	rpc.Register(node)
	go node.periodicUpdater()
	tcpServer(os.Args[1])
}