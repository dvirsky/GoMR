#GoMR - Goroutine based MapReduce



GoMR is toy-ish map reduce framework that can run MapReduce jobs **locally** across goroutines. It was written as an expriment in go, but put out here since a co-worker had asked to use it for some *SmallData* crunching, and I hope it might be useful to other people.

In `src/example.go` you can find the obligatory word counting example (that reads from stdin and counts words), but here is the gist of it:


To run a MapReduce job, you have to define a struct that implements GoMR's MapReducer interface that has:

### 1. A Mapper function of the form:

```go
Map(input interface{}, out chan Record)
```

(Record is simply a key,value pair, where the value can be anything). The mapper takes one piece of input, and maps to to N key/value pairs, pushing them to the out channel


### 2. A Reducer function of the form:

```go
Reduce(key string, values []interface{}, out chan Record)
```

The reducer takes a key, and a slice of values that are created by the mapper, and pushes back K {key,value} records to the output channel

### 3. An input reader function that returns one item to be processed (e.g. a document) per each call
```go
Read() (record interface{}, err error)
```

if it returns an error, the reading stops

###4. An output handler function of the form
```go
HandleOutput(output chan Record)
```

This function should consume the output of the job from the output channel completely


