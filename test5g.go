package main

import (
    "fmt"
    "time"
    "strconv"
    "net/rpc"
    "log"
    "os"
    "io/ioutil"
    "strings"
)

func main() {
	logfile, err := os.OpenFile("log.txt",  os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	Logg := log.New(logfile, "", log.Ltime)
	if err != nil {
	    fmt.Println("File does not exists or cannot be created and unable to create log")
	    os.Exit(1)
	}
	defer logfile.Close()

	for j := 0;j < 10; j++ {
		go start_user(0,j,Logg);
	}

	time.Sleep(10*time.Second)
	
	if !detect_cycle() {
		fmt.Println("No Cycle Detected")
	}

	if check_total_order() {
		fmt.Println("Following total order")
	}
}


func start_user(i int,j int, Logg *log.Logger) {
	client, err := rpc.DialHTTP("tcp", ":8000")
	if err != nil {
		fmt.Println(err)
	}
	cmds := make([]string,2)
	cmds[0] = strconv.Itoa(i)
	cmds[1] = strconv.Itoa(j)
	// fmt.Println(cmds[0])
	// fmt.Println(cmds[1])
	var dummy int
	err = client.Call("Node.UpdateKey",cmds, &dummy)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Successful Insert",cmds[0],cmds[1])
	}

	var value string 
	err = client.Call("Node.LookUp", cmds[0], &value)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(cmds[0] + ": " +value)
	}
	fmt.Println(value)
	Logg.Println(strconv.Itoa(j) + " " + value)
}

func detect_cycle() bool{
	b, err := ioutil.ReadFile("log.txt")
    if err != nil {
        fmt.Print(err)
    }
    str := string(b)
    lines := strings.Split(str,"\n")


    edges := make([]int, len(lines)-1)

    for i:=0;i<len(lines)-1;i++ {
    	line := strings.Split(lines[i]," ")
    	x,_ := strconv.Atoi(line[1])
    	y,_ := strconv.Atoi(line[2])

    	if line[1] != line[2] {
    		edges[x] = y
    	} else {
    		edges[x] = -1
    	}
    }

    for i:=0;i<len(lines)-1;i++ {
    	visited := make([]bool, len(lines))
    	for j := 0;j<len(lines)-1;j++ {
    		visited[j] = false;
    	}

    	temp := i
		for edges[temp] != -1 && visited[temp] == false{
			visited[temp] = true
			if visited[edges[temp]] {
				fmt.Println("Cycle Detected",i)
				return true
			}
			temp = edges[temp]
		}
    }

    return false
}

func check_total_order() bool {
	values := make([]int,10)
	b, err := ioutil.ReadFile("8000.log")
    if err != nil {
        fmt.Print(err)
    }
    str := string(b)
    lines := strings.Split(str,"\n")
    k := 0
    for i:=0;i<len(lines)-1;i++ {
    	line := strings.Split(lines[i]," ")
    	if strings.Compare(line[0], "Added") == 0 {
    		x,_ := strconv.Atoi(line[7])
    		values[k] = x;
    		k = k + 1
    	}
    }

    fmt.Println("---------------------")
    for i:=0;i<10;i++ {
    	fmt.Println(values[i])
    }
    fmt.Println("---------------------")

	for j:=1;j<6;j++ {
		b, err := ioutil.ReadFile(strconv.Itoa(8000+j)+".log")
		if err != nil {
		    fmt.Print(err)
		}
		str := string(b)
		lines := strings.Split(str,"\n")
    	k = 0
		for i:=0;i<len(lines)-1;i++ {
			line := strings.Split(lines[i]," ")
			if strings.Compare(line[0], "(Replication)") == 0{
				x,_ := strconv.Atoi(line[8])
				if values[k] != x {
					fmt.Println("Total order missed",j)
					return false
				}
				k = k + 1
			}
		}
	}
	return true
}
