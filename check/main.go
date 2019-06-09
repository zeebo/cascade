package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/zeebo/cascade"
	"github.com/zeebo/errs"
	"github.com/zeebo/mon"
	"github.com/zeebo/mon/monhandler"
	"github.com/zeebo/pcg"
)

var (
	nodes           = flag.Int("nodes", 1, "number of nodes")
	pointers        = flag.Int("pointers", 10000, "number of data pointers")
	nodesPerPointer = flag.Int("nodes_per_pointer", 1, "number of nodes per pointer")

	rng pcg.T
)

func intn(n int) int { return int(rng.Uint32n(uint32(n))) }

func stats() {
	defer fmt.Println()

	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	defer tw.Flush()

	mon.Times(func(name string, state *mon.State) bool {
		sum, avg := state.Average()
		fmt.Fprintf(tw, "%s\t%v\t%v\t%v\n",
			name, state.Total(), time.Duration(sum), time.Duration(avg))
		return true
	})
}

func main() {
	flag.Parse()

	defer stats()
	go http.ListenAndServe(":8080", monhandler.Handler{})

	if err := run(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func run() error {
	if err := os.Mkdir("data", 0755); err != nil {
		return errs.Wrap(err)
	}

	const (
		bits = 25
		mask = 1<<bits - 1
	)

	fs := make([]*cascade.Filter, *nodes)
	for i := range fs {
		fh, err := os.Create(fmt.Sprintf("data/node-%d", i))
		if err != nil {
			return errs.Wrap(err)
		}
		defer fh.Close()

		fs[i] = cascade.New(fh, bits)
	}

	var node0 []uint64
	for i := 0; i < *pointers; i++ {
		if i > 0 && i%(*pointers/10) == 0 {
			fmt.Printf("progress: %0.2f\n", 100*float64(i)/float64(*pointers))
			stats()
		}

		for n := 0; n < *nodesPerPointer; n++ {
			node, hash := intn(*nodes), rng.Uint64()
			if node == 0 {
				node0 = append(node0, hash)
			}

			if err := fs[node].Add(hash); err != nil {
				return errs.Wrap(err)
			}
		}
	}

	fmt.Printf("NODE0: rem: %d quo: %d len: %d\n",
		fs[0].RemainderBits(), fs[0].QuotientBits(), fs[0].Len())
	fmt.Printf("NODE0: auditing %d values\n", len(node0))
	for _, v := range node0 {
		if !fs[0].Lookup(v) {
			return errs.New("false negative: 0x%08x\n", v&mask)
		}
	}

	count, total := 0, 100*len(node0)
	for i := 0; i < total; i++ {
		if fs[0].Lookup(rng.Uint64()) {
			count++
		}
	}
	fmt.Printf("NODE0: got %d/%d == %0.4f%%\n", count, total, 100*float64(count)/float64(total))

	fmt.Println("done. waiting for ctrl+c...")
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT)
	<-ch
	fmt.Println()

	return nil
}
