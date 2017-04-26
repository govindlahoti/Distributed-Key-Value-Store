package main

import (
	"crypto/sha256"
	"net/rpc/jsonrpc"
	"net/rpc"
	"fmt"
)

func consistentHash(s string) uint64 {

	sum := sha256.Sum256([]byte(s))
	
	var hash uint64
	hash = 0
	for _, i := range sum[len(sum)-4:] {
		hash *= 256
		hash += uint64(i)
	}

	return hash
}

func getClient(addr string) (*rpc.Client,error) {
	client, err := jsonrpc.Dial("tcp", addr)
	
	if err != nil {
		fmt.Println("error in rpc call", err)
		client = nil
	}

	return client, err
}

func power2(power int) uint64 {
	return (uint64(1) << uint(power))
}