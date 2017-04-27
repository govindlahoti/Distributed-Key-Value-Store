package main
	
import (
	"fmt"
	"net/rpc"
	// "strings"
	// "bufio"
	"time"
	"os"
	"sync"
)

var cnt int64
var lock sync.Mutex

func user(addr string) {
	client, err := rpc.DialHTTP("tcp", addr)
	
	if err != nil {
		fmt.Println(err)
	}

	for true {
	
    		var dummy string

    		err = client.Call("Node.LookUp", randomString(), &dummy)

    		if err != nil {
    			// fmt.Println(err)
    			continue
    		} else {
    			// fmt.Println("Successful Insert")
	    		lock.Lock()
	    		cnt++
	    		lock.Unlock()
    		}

    		// time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	cnt = 0
	lock = sync.Mutex{}

	for i := 0; i < 100 ; i++ {
		go user(os.Args[1+(i%(len(os.Args)-1))])
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

		if cnt > 100 {
			cnt = 0
			t = time.Now()
			nanos = t.UnixNano()
		}

		lock.Unlock()
		time.Sleep(1000 * time.Millisecond)
	}
}