// Command figwal is a hands-on CLI over figwal: raw segmented logs
// (with forking and reducible watermarks) and xwal triunes (a main
// timeline plus related timelines, forked as a unit).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/segment"
	"github.com/jack-work/figwal/xwal"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
	}
	switch args[0] {
	case "log":
		runLog(args[1:])
	case "xwal":
		runXWAL(args[1:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown group %q\n\n", args[0])
		usage()
	}
}

// ---- reducer registry (shared by xwal reducible channels) ----

func registry() map[string]xwal.Reducer {
	return map[string]xwal.Reducer{
		"jsonmerge": {Reduce: jsonMerge, Initial: []byte("{}")},
	}
}

// jsonMerge applies {"set":{...},"remove":[...]} to a flat JSON object.
func jsonMerge(state, patch []byte) ([]byte, error) {
	m := map[string]json.RawMessage{}
	if len(state) > 0 {
		if err := json.Unmarshal(state, &m); err != nil {
			return nil, err
		}
	}
	var p struct {
		Set    map[string]json.RawMessage `json:"set"`
		Remove []string                   `json:"remove"`
	}
	if err := json.Unmarshal(patch, &p); err != nil {
		return nil, fmt.Errorf("patch must be {\"set\":{...},\"remove\":[...]}: %w", err)
	}
	for k, v := range p.Set {
		m[k] = v
	}
	for _, k := range p.Remove {
		delete(m, k)
	}
	return json.Marshal(m)
}

// ---- raw log group ----

func runLog(args []string) {
	if len(args) < 2 {
		usageLog()
	}
	cmd, dir := args[0], args[1]
	rest := args[2:]

	switch cmd {
	case "fork":
		fs := flag.NewFlagSet("log fork", flag.ExitOnError)
		segSize := fs.Int64("seg-size", 0, "segment size")
		fs.Parse(rest)
		if fs.NArg() != 2 {
			usageLog()
		}
		at := mustU64(fs.Arg(0))
		l := openLog(dir, *segSize)
		defer l.Close()
		child, err := l.Fork(at, fs.Arg(1))
		check(err)
		child.Close()
		fmt.Printf("forked %s @%d -> %s/%s\n", dir, at, dir, fs.Arg(1))
		return
	}

	fs := flag.NewFlagSet("log", flag.ExitOnError)
	segSize := fs.Int64("seg-size", 0, "segment size in bytes (0=default)")
	fs.Parse(rest)
	pos := fs.Args()

	l := openLog(dir, *segSize)
	defer l.Close()

	switch cmd {
	case "info":
		printLogInfo(dir, l)
	case "append":
		if len(pos) != 1 {
			usageLog()
		}
		next := l.LastIndex() + 1
		check(l.Write(next, []byte(pos[0])))
		fmt.Printf("appended @%d\n", next)
	case "write":
		if len(pos) != 2 {
			usageLog()
		}
		idx := mustU64(pos[0])
		check(l.Write(idx, []byte(pos[1])))
		fmt.Printf("wrote @%d\n", idx)
	case "read":
		if len(pos) != 1 {
			usageLog()
		}
		b, err := l.Read(mustU64(pos[0]))
		check(err)
		os.Stdout.Write(b)
		fmt.Println()
	case "dump":
		from := l.FirstIndex()
		if len(pos) == 1 {
			from = mustU64(pos[0])
		}
		if from == 0 {
			fmt.Println("(empty)")
			return
		}
		check(l.Range(from, func(idx uint64, payload []byte) error {
			fmt.Printf("%6d  %s\n", idx, payload)
			return nil
		}))
	default:
		usageLog()
	}
}

func printLogInfo(dir string, l *disk.Log) {
	fmt.Printf("log %s\n", dir)
	fmt.Printf("  range    %d..%d\n", l.FirstIndex(), l.LastIndex())
	if fb := l.ForkBase(); fb > 0 {
		fmt.Printf("  forkBase %d (child of %s)\n", fb, dirOf(l))
	}
	bases := l.SegmentBaseIndexes()
	fmt.Printf("  segments %d  bases=%v\n", len(bases), bases)
}

func dirOf(l *disk.Log) string {
	if p := l.Parent(); p != nil {
		return "parent"
	}
	return "-"
}

func openLog(dir string, segSize int64) *disk.Log {
	l, err := disk.Open(dir, disk.Options{Codec: segment.BinaryCodec{}, SegmentSize: segSize})
	check(err)
	return l
}

// ---- xwal group ----

func runXWAL(args []string) {
	if len(args) < 2 {
		usageXWAL()
	}
	cmd, dir := args[0], args[1]
	rest := args[2:]

	if cmd == "init" {
		// figwal xwal init <dir> <main> <ch[:reducer]>... [--codec jsonl|binary]
		var codec string
		rest = extractFlags(rest, map[string]*string{"codec": &codec})
		if len(rest) < 1 {
			usageXWAL()
		}
		main := rest[0]
		cfg := xwal.Config{Main: main, Registry: registry(), Codec: codec}
		seen := false
		for _, spec := range rest[1:] {
			name, reducer, isRed := strings.Cut(spec, ":")
			ch := xwal.ChannelSpec{Name: name, Kind: xwal.ChannelLog}
			if isRed {
				ch.Kind = xwal.ChannelReducible
				ch.Reducer = reducer
			}
			if name == main {
				seen = true
			}
			cfg.Channels = append(cfg.Channels, ch)
		}
		if !seen {
			cfg.Channels = append([]xwal.ChannelSpec{{Name: main, Kind: xwal.ChannelLog}}, cfg.Channels...)
		}
		x, err := xwal.Open(dir, cfg)
		check(err)
		x.Close()
		fmt.Printf("initialized xwal %s (main=%s)\n", dir, main)
		return
	}

	var branch, mainLTStr, segStr string
	pos := extractFlags(rest, map[string]*string{
		"branch":   &branch,
		"main-lt":  &mainLTStr,
		"seg-size": &segStr,
	})
	mainLT := int64(-1)
	if mainLTStr != "" {
		mainLT = int64(mustU64(mainLTStr))
	}
	var segSize int64
	if segStr != "" {
		segSize = int64(mustU64(segStr))
	}

	cfg := xwal.Config{Registry: registry(), SegmentSize: segSize}
	x, err := xwal.Open(dir, cfg, branchParts(branch)...)
	check(err)
	defer x.Close()

	switch cmd {
	case "info", "channels":
		printXWALInfo(x)
	case "appendmain":
		if len(pos) != 1 {
			usageXWAL()
		}
		lt, err := x.AppendMain([]byte(pos[0]), nil)
		check(err)
		fmt.Printf("main @%d\n", lt)
	case "append":
		if len(pos) != 2 {
			usageXWAL()
		}
		ch := pos[0]
		if ch == x.Main() {
			lt, err := x.AppendMain([]byte(pos[1]), nil)
			check(err)
			fmt.Printf("main @%d\n", lt)
			return
		}
		m := uint64(mainLT)
		if mainLT < 0 {
			m = defaultMainLT(x, ch)
		}
		lt, err := x.Append(ch, m, []byte(pos[1]), nil)
		check(err)
		fmt.Printf("%s @%d (main-lt %d)\n", ch, lt, m)
	case "read":
		if len(pos) != 2 {
			usageXWAL()
		}
		m, payload, err := x.Read(pos[0], mustU64(pos[1]))
		check(err)
		fmt.Printf("main-lt %d  %s\n", m, displayPayload(payload))
	case "state":
		if len(pos) != 2 {
			usageXWAL()
		}
		st, err := x.StateAt(pos[0], mustU64(pos[1]))
		check(err)
		os.Stdout.Write(st)
		fmt.Println()
	case "fork":
		if len(pos) != 3 {
			usageXWAL()
		}
		at := mustU64(pos[0])
		child, err := x.Fork(at, pos[1], pos[2])
		check(err)
		child.Close()
		fmt.Printf("forked @main-lt %d -> new branch %q, original continuation %q\n", at, pos[1], pos[2])
	default:
		usageXWAL()
	}
}

func printXWALInfo(x *xwal.XWAL) {
	br := branchJoin(x.Branch())
	if br == "" {
		br = "(trunk)"
	}
	fmt.Printf("xwal branch=%s main=%s\n", br, x.Main())
	fmt.Printf("  %-14s %-10s %-10s %8s %8s %s\n", "channel", "kind", "reducer", "first", "last", "segs")
	for _, c := range x.Channels() {
		marker := ""
		if c.Name == x.Main() {
			marker = " *"
		}
		fmt.Printf("  %-14s %-10s %-10s %8d %8d %4d%s\n",
			c.Name, c.Kind, c.Reducer, c.First, c.Last, c.Segments, marker)
	}
}

func mainTail(x *xwal.XWAL) uint64 {
	for _, c := range x.Channels() {
		if c.Name == x.Main() {
			return c.Last
		}
	}
	return 0
}

// defaultMainLT picks the main-lt to tag an entry with when none is
// given. A reducible channel (e.g. chalkboard) records state for the
// turn about to happen, so it defaults one ahead (mainTail+1). A log
// channel (e.g. translations) is a projection of an entry that already
// exists, so it defaults to the current tail. Always overridable with
// --main-lt.
func defaultMainLT(x *xwal.XWAL, channel string) uint64 {
	for _, c := range x.Channels() {
		if c.Name == channel && c.Kind == xwal.ChannelReducible {
			return mainTail(x) + 1
		}
	}
	return mainTail(x)
}

func branchParts(s string) []string {
	s = strings.Trim(s, "/")
	if s == "" {
		return nil
	}
	return strings.Split(s, "/")
}

func branchJoin(parts []string) string { return strings.Join(parts, "/") }

// ---- shared ----

// extractFlags pulls "--name value" and "--name=value" pairs for the
// named flags out of args from any position, returning the remaining
// positional arguments. Keeps flag placement forgiving for interactive
// use.
func extractFlags(args []string, vals map[string]*string) []string {
	var pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "--") {
			key := a[2:]
			if name, val, ok := strings.Cut(key, "="); ok {
				if p, known := vals[name]; known {
					*p = val
					continue
				}
			} else if p, known := vals[key]; known {
				if i+1 < len(args) {
					*p = args[i+1]
					i++
				}
				continue
			}
		}
		pos = append(pos, a)
	}
	return pos
}

// displayPayload renders a stored payload: a JSON string is unquoted
// back to its text; anything else (objects, numbers) prints as-is.
func displayPayload(p []byte) string {
	if len(p) > 0 && p[0] == '"' {
		var s string
		if err := json.Unmarshal(p, &s); err == nil {
			return s
		}
	}
	return string(p)
}

func mustU64(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	check(err)
	return n
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `figwal - segmented WAL with forking and reducible watermarks

usage:
  figwal log  <command> <dir> [args]   raw segmented log
  figwal xwal <command> <dir> [args]   multi-channel triune

run "figwal log" or "figwal xwal" with no command for group help`)
	os.Exit(2)
}

func usageLog() {
	fmt.Fprintln(os.Stderr, `figwal log <command> <dir> [args]
  info                       range, fork base, segment watermark bases
  append <data>              append at the tail
  write  <idx> <data>        write at an explicit index
  read   <idx>               read one entry
  dump   [from]              list entries from an index (default: first)
  fork   <atIdx> <name> [--seg-size N]   split at atIdx into subdir <name>`)
	os.Exit(2)
}

func usageXWAL() {
	fmt.Fprintln(os.Stderr, `figwal xwal <command> <dir> [args]   [--branch p] [--main-lt N] [--seg-size N]
  init <main> <ch[:reducer]>... [--codec jsonl|binary]
                                  create; e.g. init ./c ir translations chalkboard:jsonmerge
                                  (jsonl, the default, stores readable frames + watermarks)
  info                            channels, kinds, bounds (alias: channels)
  appendmain <data>               append a main-timeline entry
  append <channel> <data>         append to a channel (main-lt defaults to main tail)
  read   <channel> <channelLT>    read one entry (shows its main-lt)
  state  <channel> <channelLT>    fold a reducible channel to channelLT
  fork   <atMainLT> <newName> <origName>   joint-fork every channel; <newName>
                                  is the new branch, <origName> the original
                                  continuation. Address either with --branch.
reducers: jsonmerge ({"set":{...},"remove":[...]} over a flat JSON object)`)
	os.Exit(2)
}
