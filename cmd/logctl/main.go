package main

import (
	"bufio"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"figwal/log"
	"figwal/segment"
)

func main() {
	segSize := flag.Int64("seg-size", 0, "segment size in bytes (0=default)")
	codec := flag.String("codec", "", "codec: binary|jsonl (default: detect from dir, else binary)")
	verbose := flag.Bool("verbose", false, "enable debug-level logging")
	yes := flag.Bool("yes", false, "skip interactive confirmation for delete")
	flag.Usage = usage
	flag.Parse()

	setupLogging(*verbose)

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}
	dir, cmd := args[0], args[1]
	cmdArgs := args[2:]

	// delete short-circuits: it operates on the directory tree, not on
	// an opened Log instance.
	if cmd == "delete" {
		runDelete(dir, *yes)
		return
	}

	var c segment.SegmentCodec
	switch *codec {
	case "":
		c = nil // log.Open will auto-detect, else default to binary
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
	case "fork":
		if len(cmdArgs) != 2 {
			usage()
		}
		atIdx, err := strconv.ParseUint(cmdArgs[0], 10, 64)
		check(err)
		child, err := l.Fork(atIdx, cmdArgs[1])
		check(err)
		fmt.Printf("forked %s at %d -> %s\n", dir, atIdx, filepath.Join(dir, cmdArgs[1]))
		child.Close()
	default:
		usage()
	}
}

// runDelete walks dir for descendant forks, prints the subtree, and
// prompts for y/N confirmation before removing everything. Deletion is
// recursive and cascades into child forks per the design.
func runDelete(dir string, yes bool) {
	info, err := os.Stat(dir)
	check(err)
	if !info.IsDir() {
		fmt.Fprintln(os.Stderr, "not a directory:", dir)
		os.Exit(1)
	}
	forks := descendantForks(dir)
	fmt.Println("will delete:", dir)
	for _, f := range forks {
		fmt.Println("  ", f)
	}
	if !yes {
		fmt.Print("continue? [y/N] ")
		reader := bufio.NewReader(os.Stdin)
		ans, _ := reader.ReadString('\n')
		ans = strings.ToLower(strings.TrimSpace(ans))
		if ans != "y" && ans != "yes" {
			fmt.Println("aborted")
			os.Exit(1)
		}
	}
	check(os.RemoveAll(dir))
	fmt.Println("deleted")
}

// descendantForks returns the relative paths of every subdirectory
// under root (depth-first). Hidden subdirs (dot-prefixed) are skipped.
func descendantForks(root string) []string {
	var out []string
	var walk func(string, string)
	walk = func(abs, rel string) {
		entries, err := os.ReadDir(abs)
		if err != nil {
			return
		}
		for _, e := range entries {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			child := filepath.Join(rel, e.Name())
			out = append(out, child)
			walk(filepath.Join(abs, e.Name()), child)
		}
	}
	walk(root, "")
	return out
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
	fmt.Fprintln(os.Stderr, "usage: logctl [--seg-size N] [--codec binary|jsonl] [--verbose] [--yes] <dir> <command> [args]")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  write <idx> <data>   append an entry (idx must be LastIndex+1)")
	fmt.Fprintln(os.Stderr, "  read <idx>           read the entry at idx")
	fmt.Fprintln(os.Stderr, "  range                print FirstIndex..LastIndex")
	fmt.Fprintln(os.Stderr, "  fork <atIdx> <name>  fork at atIdx into subdir <name>")
	fmt.Fprintln(os.Stderr, "  delete               recursively delete dir and all child forks")
	os.Exit(2)
}
