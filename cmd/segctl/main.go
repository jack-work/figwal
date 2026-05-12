package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jack-work/figwal/segment"
)

func main() {
	codec := flag.String("codec", "", "codec: binary|jsonl (default: detect from path ext, else binary)")
	verbose := flag.Bool("verbose", false, "enable debug-level logging")
	flag.Usage = usage
	flag.Parse()

	setupLogging(*verbose)
	args := flag.Args()

	if len(args) < 2 {
		usage()
	}
	path, cmd := args[0], args[1]
	cmdArgs := args[2:]

	var c segment.SegmentCodec
	switch *codec {
	case "":
		switch filepath.Ext(path) {
		case ".jsonl":
			c = segment.JSONLCodec{}
		default:
			c = segment.BinaryCodec{}
		}
	case "binary":
		c = segment.BinaryCodec{}
	case "jsonl":
		c = segment.JSONLCodec{}
	default:
		fmt.Fprintf(os.Stderr, "unknown codec: %s\n", *codec)
		os.Exit(2)
	}

	switch cmd {
	case "create":
		maxLength := getMaxLength(cmdArgs)
		s, err := segment.Create(path, c, 1, maxLength)
		check(err)
		s.Close()
		fmt.Println("created", path)

	case "append":
		if len(cmdArgs) != 1 && len(cmdArgs) != 2 {
			usage()
		}
		maxLength := getMaxLength(cmdArgs)
		s, err := segment.Open(path, c, 1, maxLength)
		check(err)
		off, err := s.Append([]byte(cmdArgs[0]))
		check(err)
		check(s.Sync())
		check(s.Close())
		fmt.Println("offset:", off)

	case "read":
		if len(cmdArgs) != 1 {
			usage()
		}
		off, err := strconv.ParseInt(cmdArgs[0], 10, 64)
		check(err)
		s, err := segment.Open(path, c, 1, 0)
		check(err)
		data, err := s.ReadAt(off)
		check(err)
		check(s.Close())
		fmt.Printf("%s\n", data)
	case "readi":
		if len(cmdArgs) != 1 {
			usage()
		}
		i, err := strconv.ParseUint(cmdArgs[0], 10, 64)
		check(err)
		s, err := segment.Open(path, c, 1, 0)
		check(err)
		data, err := s.ReadIndex(i)
		check(err)
		check(s.Close())
		fmt.Printf("%s\n", data)
	case "size":
		maxLength := getMaxLength(cmdArgs)
		s, err := segment.Open(path, c, 1, maxLength)
		check(err)
		fmt.Println(s.Size())
		s.Close()

	default:
		usage()
	}
}

func getMaxLength(args []string) int64 {
	if len(args) > 1 {
		n, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			usage()
		}
		return n
	}
	return 0
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
	fmt.Fprintln(os.Stderr, "usage: segctl [--codec binary|jsonl] [--verbose] <path> <create <max_size>|append <data> [max_size]|read <offset>|readi <i>|size [max_size]>")
	os.Exit(2)
}
