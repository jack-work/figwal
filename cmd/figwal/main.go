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
	"path/filepath"
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
		"map":       xwal.MapReducer(),
	}
}

// cliReducers binds the built-in reducers under both their registry names
// and the conventional channel names, so legacy and new-style manifests
// both resolve without extra flags.
func cliReducers() map[string]xwal.Reducer {
	out := map[string]xwal.Reducer{}
	for name, r := range registry() {
		out[name] = r
	}
	out["chalkboard"] = xwal.MapReducer()
	out["state"] = xwal.MapReducer()
	return out
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
		opts := xwal.StoreOptions{Main: main, Codec: codec, Reducers: map[string]xwal.Reducer{}}
		for _, spec := range rest[1:] {
			name, reducer, isRed := strings.Cut(spec, ":")
			if !isRed {
				continue // plain log channels auto-create on first append
			}
			r, ok := registry()[reducer]
			if !ok {
				r = xwal.MapReducer()
			}
			opts.Reducers[name] = r
		}
		s, err := xwal.OpenStore(dir, opts)
		check(err)
		check(s.Close())
		fmt.Printf("initialized xwal %s (main=%s); root is markerless — create stumps with `xwal stump`\n", dir, main)
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
	sopts := xwal.StoreOptions{SegmentSize: segSize, Reducers: cliReducers()}

	// Trunk-level verbs — these mirror figaro (a trunk is the addressable
	// handle; appending at an interior LT forks). They operate on the joint
	// forest, never on a raw branch.
	switch cmd {
	case "stump":
		// stump <dir> <name> [birth-data]  — create a markerless stump; if
		// birth-data is given, write it as the stump's first IR entry.
		if len(pos) < 1 || len(pos) > 2 {
			usageXWAL()
		}
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		check(f.CreateStump(pos[0]))
		if len(pos) == 2 {
			sx, err := f.StumpHead(pos[0])
			check(err)
			_, err = sx.AppendMain([]byte(pos[1]), nil)
			sx.Close()
			check(err)
		}
		fmt.Printf("created stump %q under root\n", pos[0])
		return
	case "spawn":
		// spawn <dir> <stump-name>  — mint a top-level trunk under a stump.
		if len(pos) != 1 {
			usageXWAL()
		}
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		id, err := f.SpawnUnderStump(pos[0])
		check(err)
		fmt.Printf("spawned trunk %s under stump %q\n", id, pos[0])
		return
	case "spawn-root":
		// spawn-root <dir>  — mint a top-level trunk directly under the root.
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		id, err := f.SpawnUnderRoot()
		check(err)
		fmt.Printf("spawned trunk %s under root\n", id)
		return
	case "promote":
		// promote <dir> <trunk> [levels]
		if len(pos) < 1 || len(pos) > 2 {
			usageXWAL()
		}
		levels := 1
		if len(pos) == 2 {
			levels = int(mustU64(pos[1]))
		}
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		climbed, err := f.Promote(pos[0], levels)
		if err == xwal.ErrAtStump {
			fmt.Printf("trunk %s is rooted at a stump — cannot promote further\n", pos[0])
			os.Exit(1)
		}
		check(err)
		fmt.Printf("promoted trunk %s by %d level(s)\n", pos[0], climbed)
		return
	case "stumps":
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		for _, s := range f.Stumps() {
			fmt.Printf("  %-24s  children=%v\n", s.Name, s.Children)
		}
		return
	case "send":
		if len(pos) != 2 {
			usageXWAL()
		}
		trunk, at := parseTrunkLT(pos[0])
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		target := trunk
		if at > 0 {
			target, err = f.Fork(trunk, at)
			check(err)
		}
		_, lt, err := f.Trunks.Append(target, 0, []byte(pos[1]), nil)
		check(err)
		if target == trunk {
			fmt.Printf("appended to trunk %s @ main-lt %d\n", trunk, lt)
		} else {
			fmt.Printf("forked trunk %s -> new trunk %s @ main-lt %d (existing trunk retained)\n", trunk, target, lt)
		}
		return
	case "fork":
		if len(pos) != 1 {
			usageXWAL()
		}
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		alt, err := f.ForkTail(pos[0])
		check(err)
		fmt.Printf("fork-tail %s -> trunk %s continues (new head), new alternative trunk %s\n", pos[0], pos[0], alt)
		return
	case "set":
		// set <trunk> <dot.path> <value>  — value is JSON, or a bare string
		// is auto-quoted. Builds a native nested-map patch (validated).
		if len(pos) != 3 {
			usageXWAL()
		}
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		ch := reducibleChannel(f.Trunks, pos[0])
		patch, err := xwal.MapSetPatch(splitPath(pos[1]), jsonOrString(pos[2]))
		check(err)
		lt, err := f.AppendChannel(pos[0], ch, setMainLT(mainLT), patch, nil)
		check(err)
		fmt.Printf("set %s on trunk %s -> %s @%d\n", pos[1], pos[0], ch, lt)
		return
	case "unset":
		if len(pos) != 2 {
			usageXWAL()
		}
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		ch := reducibleChannel(f.Trunks, pos[0])
		patch, err := xwal.MapRemovePatch(splitPath(pos[1]))
		check(err)
		lt, err := f.AppendChannel(pos[0], ch, setMainLT(mainLT), patch, nil)
		check(err)
		fmt.Printf("unset %s on trunk %s -> %s @%d\n", pos[1], pos[0], ch, lt)
		return
	case "state":
		if len(pos) == 1 { // trunk-level folded reducible state
			f, err := xwal.OpenStore(dir, sopts)
			check(err)
			defer f.Close()
			x, err := f.Head(pos[0])
			check(err)
			defer x.Close()
			ch, last := reducibleBounds(x)
			st, err := x.StateAt(ch, last)
			check(err)
			os.Stdout.Write(st)
			fmt.Println()
			return
		}
	case "trunks":
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		printTrunks(f.Trunks)
		return
	case "nodes":
		f, err := xwal.OpenStore(dir, sopts)
		check(err)
		defer f.Close()
		printNodes(f.Trunks)
		return
	case "dump":
		if len(pos) == 1 { // dump a trunk (by id)
			f, err := xwal.OpenStore(dir, sopts)
			check(err)
			defer f.Close()
			x, err := f.Head(pos[0])
			check(err)
			defer x.Close()
			fmt.Printf("trunk %s (head node):\n", pos[0])
			dumpXWAL(x)
			return
		}
	}

	// Raw branch-level verbs — low-level poking, addressed by --branch.
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
	case "dump":
		dumpXWAL(x)
	case "branches", "tree":
		printBranches(dir, x.Main())
	default:
		usageXWAL()
	}
}

// parseTrunkLT splits "trunk" or "trunk:LT". No ":" means LT 0 (append at
// the trunk tail).
func parseTrunkLT(s string) (string, uint64) {
	trunk, ltStr, ok := strings.Cut(s, ":")
	if !ok || ltStr == "" {
		return trunk, 0
	}
	return trunk, mustU64(ltStr)
}

func printTrunks(f *xwal.Trunks) {
	if ss := f.Stumps(); len(ss) > 0 {
		for _, s := range ss {
			fmt.Printf("  stump %-20s children=%v\n", s.Name, s.Children)
		}
	}
	fmt.Printf("  %-8s %-8s %-12s %5s %-16s %s\n", "TRUNK", "PARENT", "STUMP", "TIP", "HEAD", "BRANCHED")
	for _, t := range f.List() {
		fmt.Printf("  %-8s %-8s %-12s %5d %-16s %s\n",
			t.ID, dash(t.Parent), dash(t.Stump), t.Tip, branchJoin(t.Head), branchedAt(t.BranchedLT))
	}
}

func printNodes(f *xwal.Trunks) {
	fmt.Printf("  %-20s %-8s %-7s %-6s %s\n", "NODE(branch)", "TRUNK", "FROZEN", "CLASS", "CHILDREN")
	for _, n := range f.Nodes() {
		fr := ""
		if n.Frozen {
			fr = "frozen"
		}
		id := branchJoin(n.Branch)
		class := "trunk"
		if id == "" {
			id, class = "(root)", "root"
		} else if n.Trunk == "" {
			class = "stump"
		}
		fmt.Printf("  %-20s %-8s %-7s %-6s %d\n", id, n.Trunk, fr, class, len(n.Children))
	}
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func branchedAt(lt uint64) string {
	if lt == 0 {
		return "-"
	}
	return "@" + strconv.FormatUint(lt, 10)
}

// splitPath turns a dot path ("system.tags.42") into segments.
func splitPath(s string) []string { return strings.Split(s, ".") }

// jsonOrString returns the arg as raw JSON if it parses, else as a quoted
// JSON string (so `set t0 mantra root` works without manual quoting).
func jsonOrString(s string) json.RawMessage {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	b, _ := json.Marshal(s)
	return b
}

// setMainLT maps the --main-lt flag (or its absence) to the AppendChannel
// main-LT arg: 0 means "default one ahead" (the reducible convention).
func setMainLT(flag int64) uint64 {
	if flag >= 0 {
		return uint64(flag)
	}
	return 0
}

// reducibleChannel returns the trunk's (first) reducible channel name.
func reducibleChannel(f *xwal.Trunks, trunk string) string {
	x, err := f.Head(trunk)
	check(err)
	defer x.Close()
	if name, _ := reducibleBounds(x); name != "" {
		return name
	}
	check(fmt.Errorf("trunk %s has no reducible channel", trunk))
	return ""
}

// reducibleBounds returns the first reducible channel's name and last LT.
func reducibleBounds(x *xwal.XWAL) (string, uint64) {
	for _, c := range x.Channels() {
		if c.Kind == xwal.ChannelReducible {
			return c.Name, c.Last
		}
	}
	return "", 0
}

// dumpXWAL prints every channel's full contents for this branch — the
// joint view of all the trees at a glance. Log channels list each entry
// (channelLT, its main-lt, payload); a reducible channel also shows its
// folded state at the tail.
func dumpXWAL(x *xwal.XWAL) {
	br := branchJoin(x.Branch())
	if br == "" {
		br = "(trunk)"
	}
	fmt.Printf("xwal branch=%s main=%s\n", br, x.Main())
	for _, c := range x.Channels() {
		fmt.Printf("\n== %s (%s)  first=%d last=%d ==\n", c.Name, c.Kind, c.First, c.Last)
		if c.Last == 0 {
			fmt.Println("  (empty — inherited from ancestor)")
			continue
		}
		if c.Kind == xwal.ChannelReducible {
			if st, err := x.StateAt(c.Name, c.Last); err == nil {
				fmt.Printf("  state@%d = %s\n", c.Last, string(st))
			}
		}
		// c.First reflects the first index visible up the parent chain, which
		// is 0 when ancestors are empty even though this node has own entries;
		// start at 1 and skip the inherited-but-absent low indices.
		lo := c.First
		if lo == 0 {
			lo = 1
		}
		for lt := lo; lt <= c.Last; lt++ {
			m, p, err := x.Read(c.Name, lt)
			if err != nil {
				continue // below this node's own range (inherited prefix is empty)
			}
			fmt.Printf("  [%d] main=%d  %s\n", lt, m, displayPayload(p))
		}
	}
}

// printBranches walks the fork tree under the main channel and prints
// every branch path (a directory chain of fork names). Branch points —
// dirs that have fork children — are flagged. Address any branch with
// --branch <path>.
func printBranches(dir, main string) {
	fmt.Printf("fork tree under main channel %q (address with --branch):\n", main)
	var walk func(d string, chain []string)
	walk = func(d string, chain []string) {
		ents, err := os.ReadDir(d)
		if err != nil {
			return
		}
		var kids []string
		for _, e := range ents {
			if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
				kids = append(kids, e.Name())
			}
		}
		label := "(trunk)"
		if len(chain) > 0 {
			label = strings.Repeat("  ", len(chain)) + branchJoin(chain)
		}
		marker := ""
		if len(kids) > 0 {
			marker = "  [branch point, read-only]"
		}
		fmt.Printf("  %s%s\n", label, marker)
		for _, k := range kids {
			walk(filepath.Join(d, k), append(append([]string(nil), chain...), k))
		}
	}
	walk(filepath.Join(dir, main), nil)
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
	fmt.Fprintln(os.Stderr, `figwal xwal <command> <dir> [args]

trunk verbs (mirror figaro — a trunk is the addressable handle; no attendance):
  init <main> <ch[:reducer]>...   create the forest (markerless root holding
                                  genesis); e.g. init ./c ir translations chalkboard:jsonmerge
                                  [--codec jsonl|binary]
  stump <name> [birth-data]       create a markerless stump under the root (the
                                  cauterization boundary), optional birth IR entry
  spawn <stump-name>              mint a top-level trunk under a stump
  spawn-root                      mint a top-level trunk directly under the root
  stumps                          list stumps and their trunk children
  promote <trunk> [levels]        climb a trunk up N stump-bounded levels by
                                  relabeling .trunk markers (stops at a stump)
  send  <trunk>[:<LT>] <data>     append to a trunk's tail; with <LT>, fork a new
                                  trunk there first and append to it (channels
                                  auto-create on first append)
  fork  <trunk>                   tail-only: bisect the present (trunk continues on a
                                  new head; a new alternative trunk is founded)
  set   <trunk> <dot.path> <val>  set a nested value in the trunk's reducible map;
                                  <val> is JSON, or a bare word is auto-quoted
                                  (e.g. set t0 system.tags.42 '{"cache":"x"}') [--main-lt N]
  unset <trunk> <dot.path>        remove a nested value [--main-lt N]
  state <trunk>                   the folded reducible-map state of a trunk
  trunks                          list trunks (TRUNK VECTOR PARENT TIP HEAD BRANCHED)
  dump  <trunk>                   full contents of a trunk's head across all channels
  nodes                           the raw node tree (debug)

raw branch verbs (low-level, addressed by --branch [--main-lt N] [--seg-size N]):
  info | appendmain <data> | append <channel> <data> | read <channel> <chLT> |
  state <channel> <chLT> | dump | branches
reducers: jsonmerge ({"set":{...},"remove":[...]} over a flat JSON object)`)
	os.Exit(2)
}
