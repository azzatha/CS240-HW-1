package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os" // Interface to operating system functionality
	"sort"
	//"bytes"
	//"fmt"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//---------------------------------------------------------------------------
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//---------------------------------------------------------------------------

	//Solution:
	//---------

	//struct keyVal to for the Key/value pairs
	var keyVal KeyValue

	//1- Create map of string slices. every key point to list of values
	listKeyValues := make(map[string][]string)

	//2- Iterate through all the nMaps tasks,
	for mapTask := 0; mapTask < nMap; mapTask++ {
		nameOfFile := reduceName(jobName, mapTask, reduceTaskNumber)
		intermfile, e := os.OpenFile(nameOfFile, os.O_RDWR|os.O_CREATE, 0755)
		Checkr(e)

		//3- use josn to decode the intermediate files
		decodeFiles := json.NewDecoder(intermfile) //NewDecoder returns a new decoder that read from rfile

		// for loop until break out of the loop (when reach the end of file)
		for {
			//repeatedly calling Decode() until Decode() returns an error.
			err := decodeFiles.Decode(&keyVal)
			if err == io.EOF {
				break
			}
			Checkr(err)
			// Appends the items to the listKeyValues slice.
			listKeyValues[keyVal.Key] = append(listKeyValues[keyVal.Key], keyVal.Value)
		}
		//close the file when we’re done, using defer
		defer intermfile.Close()
	}

	//4- Sort by keys in increasing order
	var sortedKey []string
	for keys := range listKeyValues {
		sortedKey = append(sortedKey, keys)
	}
	sort.Strings(sortedKey)

	//5- create a file for the reduced output
	reducedOutput := mergeName(jobName, reduceTaskNumber)
	var mergeFile, err = os.Create(reducedOutput)
	Checkr(err)

	//6- encode the contents of the file
	enc := json.NewEncoder(mergeFile)

	//7- write the reduced output as JSON encoded KeyValue
	for _, key := range sortedKey {
		reduceFun := reduceF(key, listKeyValues[key]) //add all the ones value for each key
		KV := KeyValue{key, reduceFun}
		e := enc.Encode(KV)
		Checkr(e)
	}
	mergeFile.Close()
}

func Checkr(e error) {
	if e != nil {
		log.Fatal("The problem: ", e)
	}
}
