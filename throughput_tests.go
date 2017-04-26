package main
	
import (
	"fmt"
	"net/rpc/jsonrpc"
	// "strings"
	// "bufio"
	"time"
	"os"
)


func main() {
	client, err := jsonrpc.Dial("tcp", os.Args[1])
	
	if err != nil {
		fmt.Println(err)
	}

	t := time.Now()
	nanos := t.UnixNano()

	var cnt int64
	cnt = 0

	fmt.Println(t)

	for true {
	
    		var dummy int

    		err = client.Call("Node.UpdateKey", []string{randomString(), randomString()}, &dummy)

    		if err != nil {
    			fmt.Println(err)
    		} else {
    			// fmt.Println("Successful Insert")
    		}

    		cnt++

    		t1 := time.Now()
    		nanos1 := t1.UnixNano()

    		fmt.Println(cnt * 1000000000.0 / (nanos1 - nanos) )
	}
}