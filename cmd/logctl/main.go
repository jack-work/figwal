package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"figwal/log"
	"figwal/segment"
)

func main() {
	segSize := flag.Int64("seg-size", 0, "segment size in bytes (0=default)")
	codec := flag.String("codec", "binary", "codec: binary|jsonl")
	verbose := flag.Bool("verbose", false, "enable debug-level logging")
	flag.Usage = usage
	flag.Parse()

	setupLogging(*verbose)

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}
	dir, cmd := args[0], args[1]
	cmdArgs := args[2:]

	var c segment.SegmentCodec
	switch *codec {
	case "binary":
		c = segment.BinaryCodec{}
	case "jsonl":
		c = segment.JSONLCodec{}
	default:
		fmt.Fprintf(os.Stderr, "unknown codec: %s\n", *codec)
		os.Exit(2)
	}

	l, err := log.Open(dir, log.Options{SegmentSize: *segSize, Codec: c})
	check(err)
	defer l.Close()

	switch cmd {
	case "write":
		if len(cmdArgs) != 2 {
			usage()
		}
		idx, err := strconv.ParseUint(cmdArgs[0], 10, 64)
		check(err)
		check(l.Write(idx, []byte(cmdArgs[1])))
		fmt.Println("ok")
	case "read":
		if len(cmdArgs) != 1 {
			usage()
		}
		idx, err := strconv.ParseUint(cmdArgs[0], 10, 64)
		check(err)
		data, err := l.Read(idx)
		check(err)
		fmt.Printf("%s\n", data)
	case "range":
		fmt.Printf("%d..%d\n", l.FirstIndex(), l.LastIndex())
	default:
		usage()
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func setupLogging(verbose bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: logctl [--seg-size N] [--codec binary|jsonl] [--verbose] <dir> <write <idx> <data>|read <idx>|range>")
	os.Exit(2)
}
