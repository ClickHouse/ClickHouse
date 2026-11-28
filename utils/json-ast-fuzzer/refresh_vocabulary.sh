#!/bin/bash
# Teach the fuzzer new syntax: diff a live server's functions & settings against the proto vocabulary, and (optionally)
# append the missing ones to json_ast.proto. Then rebuild (proto change -> build_variants.sh) so the mutator uses them.
# Usage: refresh_vocabulary.sh [--apply]   (default: dry-run, just lists the gaps)
. "$(dirname "$(readlink -f "$0")")/env.sh"
APPLY="${1:-}"; CH="$REF_CLICKHOUSE"; P="$REPO/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast.proto"
"$CH" local -q "SELECT name FROM system.functions ORDER BY name" | sort -u > /tmp/_fns.txt
"$CH" local -q "SELECT name FROM system.settings  ORDER BY name" | sort -u > /tmp/_sets.txt
grep -oE 'FN_[A-Za-z0-9_]* = [0-9]+ \[\(json_text\) = "[^"]*"\]' "$P" | sed -E 's/.*= "([^"]*)"\]/\1/' | sort -u > /tmp/_pfns.txt
awk '/enum SettingName/,/^}/' "$P" | grep -oE '= "([^"]*)"\]' | sed -E 's/= "([^"]*)"\]/\1/' | sort -u > /tmp/_psets.txt
echo "missing functions: $(comm -23 /tmp/_fns.txt /tmp/_pfns.txt | wc -l); missing settings: $(comm -23 /tmp/_sets.txt /tmp/_psets.txt | wc -l)"
[ "$APPLY" != "--apply" ] && { echo "(dry run; comm -23 /tmp/_fns.txt /tmp/_pfns.txt  to list. Re-run with --apply to patch the proto.)"; exit 0; }
REPO="$REPO" python3 -I - <<'PY'
import os,re
P=os.environ['REPO']+'/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast.proto'
lines=open(P).read().split('\n')
def rng(n):
    s=next(i for i,l in enumerate(lines) if l.strip()=='enum %s'%n); e=next(i for i in range(s,len(lines)) if lines[i].strip()=='}'); return e
miss=lambda a,b: [x for x in open(a).read().split('\n') if x and x not in set(open(b).read().split('\n'))]
existing=set(l.strip().split(' ',1)[0] for l in lines if l.strip().startswith(('FN_','SET_')))
def add(enum,pref,af,bf,start):
    e=rng(enum); n=start; out=[]
    for nm in miss(af,bf):
        i=pref+nm
        if i in existing: continue
        existing.add(i); out.append('    %s = %d [(json_text) = "%s"];'%(i,n,nm)); n+=1
    lines[e:e]=out; return len(out)
def maxnum(pref):
    import re as _r
    return max([int(m.group(1)) for l in lines for m in [_r.search(r'\b%s\w* = (\d+)'%pref,l)] if m]+[0])
af=add('FunctionName','FN_','/tmp/_fns.txt','/tmp/_pfns.txt',maxnum('FN_')+1)
as_=add('SettingName','SET_','/tmp/_sets.txt','/tmp/_psets.txt',maxnum('SET_')+1)
open(P,'w').write('\n'.join(lines)); print('appended %d functions, %d settings to json_ast.proto'%(af,as_))
PY
echo "now rebuild: build_variants.sh asan_ubsan   (and other variants in use)"
