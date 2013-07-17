//
// GoMR Is a toy MapReduce framework written for experimenting (and for the heck of it).
// It is not distributed, and does the parallel stuff only on the host machine, using goroutines as its parallel execution engine.
// To run a MapReduce job, you have to define a struct that implements GoMR's MapReducer interface that has:
//
// 1. A Mapper function of the form:
//		Map(input interface{}, out chan Record)
// (Record is simply a key,value pair, where the value can be anything).
// The mapper takes one piece of input, and maps to to N key/value pairs, pushing them to the out channel
//
// 2. A Reducer function of the form:
//		Reduce(key string, values []interface{}, out chan Record)
// The reducer takes a key, and a slice of values that are created by the mapper, and pushes back K {key,value} records to the output channel
//
// 3. An input reader function that returns one item to be processed (e.g. a document) per each call
//		Read() (record interface{}, err error)
//	if it returns an error, the reading stops
//
//	4. An output handler function of the form
//		HandleOutput(output chan Record)
//	This function should consume the output of the job from the output channel completely
//
package gomr

import (
	"github.com/dvirsky/go-pylog/logging"
	"sync"
	"time"
)

type Record struct {
	Key string
	Val interface{}
}

//This is what you need to implement in order to run a mapreduce job
type MapReducer interface {

	//mapper interface
	Map(input interface{}, out chan Record)

	//reducer interface
	Reduce(key string, values []interface{}, out chan Record)

	//input reader - reads one record per call
	Read() (record interface{}, err error)

	//output handler - do whatever you want with the output of the job
	HandleOutput(output chan Record)
}

//this is the map reduce runner struct. it contains the channels to sync everything, a custom Reader, a mapper function and a reducer function
type MapReduceJob struct {
	inputChan         chan interface{}
	mappersOutputChan chan Record
	reducersInputChan chan Record
	OutputChan        chan Record
	mapReducer        *MapReducer

	aggregate map[string][]interface{}

	numReducers int
	numMappers  int
}

//Create a new mapreduce job. Pass a mapreducer object and the number of mappers and reducers in the pools
func NewMapReduceJob(mr MapReducer, numReducers int, numMappers int) *MapReduceJob {

	job := &MapReduceJob{
		mappersOutputChan: make(chan Record),
		OutputChan:        make(chan Record),
		reducersInputChan: make(chan Record),
		inputChan:         make(chan interface{}),
		mapReducer:        &mr,
		aggregate:         make(map[string][]interface{}),
		numReducers:       numReducers,
		numMappers:        numMappers,
	}
	logging.Info("Created job with MapReducer %s, %d mappers, %d reducers", mr, numMappers, numReducers)
	return job
}

//Read the input from the user's reader and push it to the input chan
func (mr *MapReduceJob) readInput() {

	tracker := progressTracker(2, "docs")
	logging.Info("Reading input")
	for {
		record, err := (*mr.mapReducer).Read()

		if err != nil {
			logging.Error("Error: Aborting read: %s", err)
			break
		}
		mr.inputChan <- record
		tracker <- 1

	}
	close(tracker)
	close(mr.inputChan)
	logging.Info("Finished reading input!")
}

//run all the mappers in parallel
func (mr *MapReduceJob) runMappers() {
	mappersGroup := sync.WaitGroup{}

	//open up N mappers in parallel go routines
	for i := 0; i < mr.numMappers; i++ {

		//we add each mapper to the waitgroup before starting, then when it finishes it calls Done
		//this way we can wait for the mappers to finish safely
		mappersGroup.Add(1)
		logging.Info("Starting mapper %d", i)
		go func() {

			for record := range mr.inputChan {

				(*mr.mapReducer).Map(record, mr.mappersOutputChan)
			}

			mappersGroup.Done()
			logging.Info("Mapper %d done", i)
		}()
	}

	//wait for the mappers to finish up and then close their output channel
	logging.Info("Waiting for mappers to finish")
	mappersGroup.Wait()
	logging.Info("All mappers finished...")

	close(mr.mappersOutputChan)
}

//collect the mappers' output into the aggregate dictionary
func (mr *MapReduceJob) collectMappersOutput() {

	logging.Info("Collecting mappers output")

	tracker := progressTracker(5, "items")
	for kv := range mr.mappersOutputChan {
		mr.aggregate[kv.Key] = append(mr.aggregate[kv.Key], kv.Val)
		tracker <- 1
	}

	close(tracker)
	logging.Info("FINISHED Collecting mappers output")

}

//generic progress tracker -returns a channel that we push progress count on.
// NOTE: you must close the channel on finish
func progressTracker(interval int, itemName string) chan int {

	ret := make(chan int)

	go func() {
		p, i := 0, 0
		st := time.Now()
		for x := range ret {
			i += x

			if time.Since(st) > time.Duration(interval)*time.Second {
				logging.Info("Mapped %d %s, Progress rate: %.02f %s/sec", i, itemName, float32(1000000000*(i-p))/float32(time.Since(st)), itemName)
				st = time.Now()
				p = i
			}
		}

	}()

	return ret
}

//run the reducers
func (mr *MapReduceJob) runReducers() {
	reducersGroup := sync.WaitGroup{}

	logging.Info("Runnign reducers!")

	//Run N reducers in parallel
	for i := 0; i < mr.numReducers; i++ {

		reducersGroup.Add(1)
		go func() {
			for record := range mr.reducersInputChan {
				(*mr.mapReducer).Reduce(record.Key, record.Val.([]interface{}), mr.OutputChan)

			}
			reducersGroup.Done()
		}()
	}

	tracker := progressTracker(5, "items")
	//push the output from the aggregate dictionary to the reducers' input channel
	for k := range mr.aggregate {

		mr.reducersInputChan <- Record{k, mr.aggregate[k]}
		tracker <- 1
	}
	close(tracker)

	//we close the input channel, causing all reduce loops to exit
	close(mr.reducersInputChan)
	//let's wait for them to actually finish
	reducersGroup.Wait()
	logging.Info("FINISHED Runnign reducers!")

	//we now close the output channel
	close(mr.OutputChan)

}

//run all steps
func (mr *MapReduceJob) Run() {

	//read the input in a seaparate goroutine
	go mr.readInput()

	//aggregate results in a separate goroutine while the mappers are working
	go mr.collectMappersOutput()

	//start mappers and block until they are all finished
	mr.runMappers()

	//run the output collector in a different goroutine

	//now run the reducers. For this to advance, some other gorooutine needs to collect output
	go mr.runReducers()
	outputHandler := sync.WaitGroup{}
	outputHandler.Add(1)
	go func() {
		(*mr.mapReducer).HandleOutput(mr.OutputChan)
		outputHandler.Done()
	}()

	outputHandler.Wait()

	logging.Info("Finished!")

}
