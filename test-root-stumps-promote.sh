#!/usr/bin/env bash
# Hands-on test for root/stumps + channel backfill + promote, all via the
# figwal CLI. Builds the binary, drives an isolated store, prints results.
set -euo pipefail
cd "$(dirname "$0")"
go build -o /tmp/figwal-test ./cmd/figwal
FW=/tmp/figwal-test
D=$(mktemp -d)/store
echo "store: $D"
run(){ echo; echo "\$ figwal xwal $*"; "$FW" xwal "$@"; }

echo "######## TASK 2: root & stumps ########"
run init "$D" ir chalkboard:jsonmerge
run stump "$D" 'config@d880aef2' 'loadout-birth'   # a loadout (markerless stump)
run spawn "$D" 'config@d880aef2'                    # conversation t0
run spawn "$D" 'config@d880aef2'                    # conversation t1
run send  "$D" t0 hello
run send  "$D" t0 world
run fork  "$D" t0                                    # branch t0 -> new alt
run stumps "$D"
run trunks "$D"
echo "--- the root and the stumps are markerless (no .trunk): ---"
ls -a "$D"/ir | grep -q '^\.trunk$' && echo "FAIL: root has .trunk" || echo "ok: root markerless"
ls -a "$D"/ir/'config@d880aef2' | grep -q '^\.trunk$' && echo "FAIL: stump has .trunk" || echo "ok: stump markerless"

echo
echo "######## TASK 1: add-channel + backfill (slashed name) ########"
run add-channel "$D" translations/anthropic
echo "--- ir node dirs vs translations/anthropic node dirs (must match) ---"
diff <(cd "$D"/ir && find . -type d | sort) \
     <(cd "$D"/translations/anthropic && find . -type d | sort) \
  && echo "ok: channel tree mirrors ir" || echo "FAIL: trees differ"
echo "--- no stray anthropic/anthropic (nested inside the channel) ---"
test -z "$(find "$D"/translations/anthropic -mindepth 1 -type d -name anthropic)" \
  && echo "ok: no stray dir" || echo "FAIL: stray dir"
echo "--- write a translation to t1 and read it back ---"
"$FW" xwal append "$D" translations/anthropic '["hi from t1"]' --branch 'config@d880aef2/n1'
run dump "$D" t1   # the translations/anthropic section shows [1] ["hi from t1"]

echo
echo "######## TASK 3: promote ########"
# Build a lineage under t1: t1 -> interior-fork -> B -> interior-fork -> A
"$FW" xwal send "$D" t1 x1 >/dev/null
"$FW" xwal send "$D" t1 x2 >/dev/null
"$FW" xwal send "$D" t1 x3 >/dev/null
B=$("$FW" xwal send "$D" t1:4 fromB | grep -oE 'new trunk t[0-9]+' | grep -oE 't[0-9]+')
"$FW" xwal send "$D" "$B" b2 >/dev/null
A=$("$FW" xwal send "$D" "$B":5 fromA | grep -oE 'new trunk t[0-9]+' | grep -oE 't[0-9]+')
echo "lineage: A=$A is an alt of B=$B is an alt of t1"
markers(){ find "$D"/ir/'config@d880aef2'/n1 -name .trunk | sort \
  | while read -r f; do d=${f%/.trunk}; echo "  ${d#"$D"/ir/}: $(cat "$f")"; done; }
echo "--- .trunk markers before promote (A=$A leaf, B=$B, t1) ---"; markers
run promote "$D" "$A"          # A absorbs B's run
echo "--- after promote $A x1 ---"; markers
run promote "$D" "$A" 5        # climbs t1, stops at stump (excess no-op)
echo "--- after promote $A x5 ---"; markers
echo; echo "\$ figwal xwal promote $D $A   # expect ErrAtStump"
"$FW" xwal promote "$D" "$A" || echo "  (exit 1 as expected — rooted at the stump)"
echo
echo "ALL DONE. store left at: $D"
