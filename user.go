package main
	
import (
	"fmt"
	"net/rpc/jsonrpc"
	"strings"
	"bufio"
	"os"
)

func main() {
	client, err := jsonrpc.Dial("tcp", os.Args[1])
	if err != nil {
		fmt.Println(err)
	}

	for true {
		fmt.Print(">>>> ")
	    scanner := bufio.NewScanner(os.Stdin)
	    
	    for scanner.Scan() {
	    	s := scanner.Text()
	    	cmds := strings.Fields(s)

		    switch cmds[0] {
		    	case "add":

		    		if len(cmds) != 3 {
		    			fmt.Println("Wrong format")
		    			continue
		    		}

		    		var dummy int
		    		err = client.Call("Node.UpdateKey", cmds[1:3], &dummy)

		    		if err != nil {
		    			fmt.Println(err)
		    		} else {
		    			fmt.Println("Successful Insert")
		    		}

		    	case "look":

		    		if len(cmds) != 2 {
		    			fmt.Println("Wrong format")
		    			continue
		    		}

		    		var value string 
					err = client.Call("Node.LookUp", cmds[1], &value)

		    		if err != nil {
		    			fmt.Println(err)
		    		} else {
		    			fmt.Println(cmds[1] + ": " +value)
		    		}
		    }
		    fmt.Print(">>>> ")
	    }
	}
}