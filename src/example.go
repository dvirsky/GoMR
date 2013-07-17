//Example GoMR implementation of counting words from an stdin line stream
//
package main

import (
	"./gomr"
	"bufio"
	"fmt"
	"github.com/dvirsky/go-pylog/logging"
	"os"
	"runtime"
	"strings"
	"time"
)

type MyMapReducer struct {
	scanner bufio.Scanner
	offset  int
}

//iterate over the dummy docs
func (m *MyMapReducer) Read() (record interface{}, err error) {

	ok := m.scanner.Scan()
	m.offset++
	if !ok {
		return nil, fmt.Errorf("End reaced...")
	}
	if m.offset%1000 == 0 {
		_, _ = os.Stdout.WriteString(".")

	}

	//we copy so we won't use the same memory buffer for all goroutines

	dst := make([]byte, len(m.scanner.Bytes()))
	copy(dst, m.scanner.Bytes())

	return dst, nil

}

// The mapper function - simply emits 1 for every word in the doc
func (m *MyMapReducer) Map(input interface{}, out chan gomr.Record) {

	str := string(input.([]byte))
	tokens := strings.Fields(str)

	for i := range tokens {

		word := strings.Trim(strings.ToLower(tokens[i]), ",_-.:/*!;-=() \"'[]{}+")
		if len(word) > 2 {
			out <- gomr.Record{word, 1}
		}

	}

}

//reducer - the len of the values is the number of times the values were counted as 1
func (m *MyMapReducer) Reduce(key string, values []interface{}, reduceChan chan gomr.Record) {

	reduceChan <- gomr.Record{key, len(values)}

}

func (m *MyMapReducer) HandleOutput(outputChan chan gomr.Record) {

	n := 0
	w := 0
	mx := 0
	mxw := ""

	for kv := range outputChan {
		n++
		count := kv.Val.(int)
		w += count
		if count > mx {
			mxw = kv.Key
			mx = count
		}

		//logging.Info("Reduced output: ", kv)
	}

	logging.Info("keys %d, words %d, top word: %s (%d)", n, w, mxw, mx)

}

func main() {

	runtime.GOMAXPROCS(8)

	my := MyMapReducer{scanner: *bufio.NewScanner(os.Stdin)}

	mr := gomr.NewMapReduceJob(&my, 80, 80)

	st := time.Now()
	mr.Run()

	logging.Info("Finished in %s", time.Since(st))
}
