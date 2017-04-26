package main

import (
	"fmt"
	"net/rpc"
	"strings"
	"bufio"
	"os"
)

func main() {
	for true {
		fmt.Print(">>>> ")
	    scanner := bufio.NewScanner(os.Stdin)
	    
	    for scanner.Scan() {
	    	s := scanner.Text()
	    	cmds := strings.Fields(s)

    		client, err := rpc.DialHTTP("tcp", cmds[0])
			if err != nil {
				fmt.Println(err)
				continue
			}

		  	switch cmds[1] {
		    	case "add":

		    		if len(cmds) != 4 {
		    			fmt.Println("Wrong format")
		    			continue
		    		}

		    		var dummy int
		    		err = client.Call("Node.UpdateKey", cmds[2:4], &dummy)

		    		if err != nil {
		    			fmt.Println(err)
		    		} else {
		    			fmt.Println("Successful Insert")
		    		}

		    	case "look":

		    		if len(cmds) != 3 {
		    			fmt.Println("Wrong format")
		    			continue
		    		}

		    		var value string 
					err = client.Call("Node.LookUp", cmds[2], &value)

		    		if err != nil {
		    			fmt.Println(err)
		    		} else {
		    			fmt.Println(cmds[2] + ": " +value)
		    		}
	    		case "leave":

		    		if len(cmds) != 2 {
		    			fmt.Println("Wrong format")
		    			continue
		    		}

		    		var dummy string 
					err = client.Call("Node.DoLeave", dummy, &dummy)

		    		if err != nil {
		    			fmt.Println(err)
		    		} else {
		    			fmt.Println("Node Leave Successful")
		    		}
	    		case "del":

		    		if len(cmds) != 3 {
		    			fmt.Println("Wrong format")
		    			continue
		    		}

		    		var dummy int 
					err = client.Call("Node.DeleteKey", cmds[2], &dummy)

		    		if err != nil {
		    			fmt.Println(err)
		    		} else {
		    			fmt.Println("Delete Successful")
		    		}

		    }
		    fmt.Print(">>>> ")
	    }
	}
}