package main
	
import (
	"fmt"
	"net/rpc/jsonrpc"
	// "strings"
	// "bufio"
	"time"
	"os"
	"sync"
)

var cnt int64
var lock sync.Mutex

func user() {
	client, err := jsonrpc.Dial("tcp", os.Args[1])
	
	if err != nil {
		fmt.Println(err)
	}

	for true {
	
    		var dummy int

    		err = client.Call("Node.UpdateKey", []string{randomString(), randomString()}, &dummy)

    		if err != nil {
    			fmt.Println(err)
    		} else {
    			// fmt.Println("Successful Insert")
    		}

    		lock.Lock()
    		cnt++
    		lock.Unlock()

    		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	cnt = 0
	lock = sync.Mutex{}

	for i := 0; i < 100 ; i++ {
		go user()
	}

	t := time.Now()
	nanos := t.UnixNano()
	t1 := time.Now()
	nanos1 := t1.UnixNano()
	
	for true {
		t1 = time.Now()
		nanos1 = t1.UnixNano()

		lock.Lock()
		fmt.Println(cnt * 1000000000.0 / (nanos1 - nanos) )

		if cnt > 1000 {
			cnt = 0
			t = time.Now()
			nanos = t.UnixNano()
		}

		lock.Unlock()
		time.Sleep(1000 * time.Millisecond)
	}
}