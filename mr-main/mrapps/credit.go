package main

//
// a MR application you need to develop
// go build -buildmode=plugin credit.go
//

import (
	"cs350/mr"
	"strconv"
	"strings"
)

// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
func Map(filename string, contents string) []mr.KeyValue {
	// your inplementation here
	kva := []mr.KeyValue{}
	//fmt.Println(contents)
	temp := strings.Split(contents, "\n")
	for _, thing := range temp {
		temp := strings.Split(thing, ",")
		//fmt.Println(temp)
		if len(temp) == 4 {
			d, _ := strconv.Atoi(temp[3])
			//xd, _ := strconv.Atoi(temp[2])
			if d > 400 && temp[2] == "2023" {
				kva = append(kva, mr.KeyValue{temp[1], "1"})
			}
		}
	}
	return kva
}

// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
func Reduce(key string, values []string) string {
	// your inplementation here
	return strconv.Itoa(len(values))
}
