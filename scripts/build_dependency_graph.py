"""パッケージのモジュール・関数の依存グラフ（インタラクティブな HTML）と指標（JSON）を作る。

    uv run python scripts/build_dependency_graph.py
    # リファクタリング前（main ブランチ）のグラフ
    git archive main edinet_xbrl_prep | tar -x -C /tmp/before
    uv run python scripts/build_dependency_graph.py --src /tmp/before/edinet_xbrl_prep \
        --out docs/dependency_graph_before.html --label before --title "リファクタリング前（main）"

ソースコードを構文解析（ast）して、次を辺にする。
- モジュール → モジュール：パッケージ内の import
- 関数 → 関数：関数の本体で呼ぶ（または引数として渡す）パッケージ内の関数・クラス

指標：モジュール数・関数数・辺の数・モジュール間の循環・import * の数・同じ本体の重複定義・使われていない関数・最大入次数/出次数。
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import hashlib
import json
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "edinet_xbrl_prep"
VIS_NETWORK = "https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.9/standalone/umd/vis-network.min.js"
# 層（数字が大きいほど上）。上の層から下の層への依存だけを許す
LAYERS = {
    "schemas": 1, "lineage": 1,
    "linkbase": 2, "archive": 2, "text": 2, "llm": 2, "numeric": 2, "edinet_api": 2, "taxonomy": 2,
    "facts": 3,
    "fs_tbl": 4,
    "__init__": 5,
}
MIN_BODY = 120  # 重複定義とみなす本体の大きさ（ast.dump の文字数）の下限


@dataclass
class Func:
    id: str  # module.name
    module: str
    name: str
    kind: str  # function / class / method
    lineno: int
    doc: str
    body_hash: str
    refs: set[str] = field(default_factory=set)  # 本体で参照する名前（a, a.b）


@dataclass
class Module:
    name: str
    path: Path
    imports: dict[str, str] = field(default_factory=dict)  # ローカル名 → module または module.attr
    internal_imports: set[str] = field(default_factory=set)
    star_imports: list[str] = field(default_factory=list)
    funcs: dict[str, Func] = field(default_factory=dict)


def _body_hash(node: ast.AST) -> str:
    body = list(getattr(node, "body", []))
    if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant):
        body = body[1:]
    dumped = "".join(ast.dump(s, annotate_fields=False) for s in body)
    return hashlib.sha1(dumped.encode()).hexdigest()[:12] if len(dumped) >= MIN_BODY else ""


def _refs(node: ast.AST) -> set[str]:
    refs = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name):
            refs.add(f"{sub.value.id}.{sub.attr}")
        elif isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
            refs.add(sub.id)
    return refs


def parse_module(path: Path, src: Path) -> Module:
    name = path.stem
    module = Module(name, path)
    with warnings.catch_warnings():  # 旧版のコードの不正なエスケープの警告は出さない
        warnings.simplefilter("ignore", SyntaxWarning)
        tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            target = node.module or ""
            if node.level:  # 相対 import（from .x import y）
                target = f"{PACKAGE}.{target}" if target else PACKAGE
            internal = target.startswith(PACKAGE + ".")
            short = target.removeprefix(PACKAGE + ".") if internal else target
            if internal:
                module.internal_imports.add(short)
            for alias in node.names:
                if alias.name == "*":
                    module.star_imports.append(short)
                elif internal:
                    module.imports[alias.asname or alias.name] = f"{short}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(PACKAGE + "."):
                    short = alias.name.removeprefix(PACKAGE + ".")
                    module.internal_imports.add(short)
                    module.imports[alias.asname or short] = short

    def add(node, qual: str, kind: str) -> None:
        doc = (ast.get_docstring(node) or "").strip().splitlines()
        module.funcs[qual] = Func(
            f"{name}.{qual}", name, qual, kind, node.lineno, doc[0] if doc else "", _body_hash(node), _refs(node)
        )

    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            add(node, node.name, "function")
        elif isinstance(node, ast.ClassDef):
            add(node, node.name, "class")
            for item in node.body:
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                    add(item, f"{node.name}.{item.name}", "method")
    return module


def collect(src: Path) -> dict[str, Module]:
    return {p.stem: parse_module(p, src) for p in sorted(src.glob("*.py"))}


def resolve_edges(modules: dict[str, Module]) -> set[tuple[str, str]]:
    """関数 → 関数の辺。"""
    edges = set()
    for module in modules.values():
        # star import で入る名前も解決する（旧版の from .x import *）
        star_names = {
            fname: f"{star}.{fname}"
            for star in module.star_imports if star in modules
            for fname in modules[star].funcs if "." not in fname
        }
        for func in module.funcs.values():
            cls = func.name.split(".")[0] if func.kind == "method" else None
            for ref in func.refs:
                head, _, attr = ref.partition(".")
                target = None
                if head in ("self", "cls") and cls and attr and f"{cls}.{attr}" in module.funcs:
                    target = f"{module.name}.{cls}.{attr}"
                elif head in module.funcs and not attr and "." not in head:
                    target = f"{module.name}.{head}"
                elif head in module.imports:
                    resolved = module.imports[head] + (f".{attr}" if attr else "")
                    mod, _, rest = resolved.partition(".")
                    if mod in modules and rest in modules[mod].funcs:
                        target = f"{mod}.{rest}"
                    elif mod in modules and rest.split(".")[0] in modules[mod].funcs:
                        target = f"{mod}.{rest.split('.')[0]}"
                elif head in star_names and not attr:
                    target = star_names[head]
                if target and target != func.id:
                    edges.add((func.id, target))
    return edges


def module_cycles(modules: dict[str, Module]) -> list[list[str]]:
    graph = {m: {i for i in mod.internal_imports if i in modules and i != m} for m, mod in modules.items()}
    cycles, seen = [], set()

    def dfs(node, stack):
        for nxt in sorted(graph[node]):
            if nxt in stack:
                cycle = stack[stack.index(nxt):]
                key = frozenset(cycle)
                if key not in seen:
                    seen.add(key)
                    cycles.append(cycle + [nxt])
            elif len(stack) < 12:
                dfs(nxt, stack + [nxt])

    for start in sorted(graph):
        dfs(start, [start])
    return cycles


def metrics(modules: dict[str, Module], edges: set[tuple[str, str]], label: str) -> dict:
    funcs = [f for m in modules.values() for f in m.funcs.values()]
    by_hash = defaultdict(list)
    for f in funcs:
        if f.body_hash:
            by_hash[f.body_hash].append(f.id)
    duplicates = [ids for ids in by_hash.values() if len(ids) > 1]
    indeg, outdeg = defaultdict(int), defaultdict(int)
    for a, b in edges:
        outdeg[a] += 1
        indeg[b] += 1
    exported = set()
    if "__init__" in modules:
        exported = {v for v in modules["__init__"].imports.values()}
    unused = sorted(
        f.id for f in funcs
        if indeg[f.id] == 0 and not f.name.startswith("_") and f.kind != "method"
        and f"{f.module}.{f.name}" not in exported and f.module != "__init__"
    )
    reverse = []
    for m, mod in modules.items():
        for i in mod.internal_imports:
            if i in modules and LAYERS.get(i, 0) > LAYERS.get(m, 0):
                reverse.append(f"{m} → {i}")
    return {
        "label": label,
        "created_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "modules": len(modules),
        "functions": len(funcs),
        "function_edges": len(edges),
        "module_edges": sum(len({i for i in m.internal_imports if i in modules}) for m in modules.values()),
        "module_cycles": module_cycles(modules),
        "star_imports": sum(len(m.star_imports) for m in modules.values()),
        "duplicate_bodies": duplicates,
        "reverse_layer_imports": sorted(reverse),
        "unreferenced_public_functions": unused,
        "max_in_degree": max(indeg.values(), default=0),
        "max_out_degree": max(outdeg.values(), default=0),
        "lines_of_code": sum(len(m.path.read_text(encoding="utf-8").splitlines()) for m in modules.values()),
    }


PALETTE = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#76b7b2", "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac", "#86bcb6", "#d37295", "#8cd17d"]


def render(modules: dict[str, Module], edges: set[tuple[str, str]], stats: dict, title: str, src: Path) -> str:
    colors = {m: PALETTE[i % len(PALETTE)] for i, m in enumerate(sorted(modules))}
    nodes, links = [], []
    for m, mod in sorted(modules.items()):
        nodes.append({"id": f"mod:{m}", "label": f"{m}.py", "group": m, "shape": "box", "level": LAYERS.get(m, 0),
                      "color": colors[m], "title": f"{m}.py（{len(mod.funcs)} 関数・クラス）", "kind": "module", "module": m})
        for f in mod.funcs.values():
            nodes.append({"id": f.id, "label": f.name, "group": m, "shape": "dot" if f.kind != "class" else "diamond",
                          "size": 9 if f.kind == "method" else 13, "color": colors[m],
                          "title": f"{f.id}（{f.kind}, L{f.lineno}）\n{f.doc}", "kind": f.kind, "module": m,
                          "doc": f.doc, "lineno": f.lineno})
            links.append({"from": f"mod:{m}", "to": f.id, "kind": "contains"})
        for i in sorted(mod.internal_imports):
            if i in modules and i != m:
                links.append({"from": f"mod:{m}", "to": f"mod:{i}", "kind": "import"})
    for a, b in sorted(edges):
        links.append({"from": a, "to": b, "kind": "call"})
    data = json.dumps({"nodes": nodes, "edges": links, "stats": stats, "modules": sorted(modules), "colors": colors}, ensure_ascii=False)
    return HTML.replace("__TITLE__", title).replace("__DATA__", data).replace("__VIS__", VIS_NETWORK).replace("__SRC__", str(src.relative_to(ROOT) if src.is_relative_to(ROOT) else src.name))


HTML = r"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>依存関係グラフ</title>
<script src="__VIS__"></script>
<style>
:root{--bg:#fff;--fg:#1f2328;--muted:#656d76;--line:#d0d7de;--panel:#f6f8fa}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;--muted:#8d96a0;--line:#30363d;--panel:#161b22}}
body{margin:0;font-family:system-ui,-apple-system,"Hiragino Sans",sans-serif;background:var(--bg);color:var(--fg)}
header{padding:12px 16px;border-bottom:1px solid var(--line)}h1{font-size:18px;margin:0 0 4px}
.sub{color:var(--muted);font-size:13px}
main{display:flex;height:calc(100vh - 64px)}#graph{flex:1;min-width:0}
aside{width:340px;max-width:40vw;overflow:auto;border-left:1px solid var(--line);background:var(--panel);padding:12px;font-size:13px}
aside h2{font-size:14px;margin:12px 0 6px}table{border-collapse:collapse;width:100%}td{padding:2px 4px;border-bottom:1px solid var(--line)}
label{display:block;margin:2px 0}code{font-size:12px}.mods label span{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px}
ul{padding-left:18px;margin:4px 0}button{margin:2px 4px 2px 0}
@media (max-width:700px){main{flex-direction:column;height:auto}#graph{height:70vh}aside{width:auto;max-width:none;border-left:0;border-top:1px solid var(--line)}}
</style></head><body>
<header><h1>__TITLE__</h1><div class="sub">対象：<code>__SRC__</code>　ノードを選ぶと、呼び出し元（橙）・呼び出し先（青）を強調します。四角＝モジュール、丸＝関数、ひし形＝クラス。</div></header>
<main><div id="graph"></div><aside>
<h2>表示</h2>
<label><input type="checkbox" id="showImports" checked> モジュール間の import</label>
<label><input type="checkbox" id="showMethods" checked> メソッド</label>
<label><input type="checkbox" id="showContains" checked> モジュール → 関数の所属線</label>
<button id="fit">全体を表示</button>
<h2>モジュール</h2><div class="mods" id="mods"></div>
<h2>選択中</h2><div id="info">（ノードを選んでください）</div>
<h2>指標</h2><table id="stats"></table>
</aside></main>
<script>
const DATA = __DATA__;
const nodes = new vis.DataSet(DATA.nodes.map(n => ({...n, font:{color: getComputedStyle(document.body).color, size: n.kind==='module'?16:12}})));
const allEdges = DATA.edges.map((e,i) => ({id:i, ...e, arrows: e.kind==='contains'?'':'to',
  dashes: e.kind==='import', color:{color: e.kind==='call'?'#8b949e':(e.kind==='import'?'#6e7781':'#d0d7de'), opacity: e.kind==='contains'?0.3:0.8},
  width: e.kind==='import'?2:1}));
const edges = new vis.DataSet([]);
const hiddenModules = new Set();
function refresh(){
  const showImports = document.getElementById('showImports').checked, showMethods = document.getElementById('showMethods').checked, showContains = document.getElementById('showContains').checked;
  nodes.update(DATA.nodes.map(n => ({id:n.id, hidden: hiddenModules.has(n.module) || (!showMethods && n.kind==='method')})));
  edges.clear();
  edges.add(allEdges.filter(e => (e.kind!=='import'||showImports) && (e.kind!=='contains'||showContains)));
}
const net = new vis.Network(document.getElementById('graph'), {nodes, edges}, {
  physics:{solver:'forceAtlas2Based', forceAtlas2Based:{gravitationalConstant:-45, springLength:60, springConstant:0.12}, stabilization:{iterations:300}},
  interaction:{hover:true, multiselect:false}, nodes:{borderWidth:1}, edges:{smooth:{type:'continuous'}}});
const box = document.getElementById('mods');
DATA.modules.forEach(m => { const l = document.createElement('label');
  l.innerHTML = `<input type="checkbox" checked data-m="${m}"> <span style="background:${DATA.colors[m]}"></span>${m}`; box.appendChild(l); });
box.addEventListener('change', e => { const m = e.target.dataset.m; e.target.checked ? hiddenModules.delete(m) : hiddenModules.add(m); refresh(); });
['showImports','showMethods','showContains'].forEach(id => document.getElementById(id).addEventListener('change', refresh));
document.getElementById('fit').onclick = () => net.fit();
const esc = s => String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
net.on('click', p => {
  const id = p.nodes[0];
  const color = n => DATA.nodes.find(x => x.id===n).color;
  if(!id){ nodes.update(DATA.nodes.map(n => ({id:n.id, color:n.color, opacity:1}))); document.getElementById('info').textContent='（ノードを選んでください）'; return; }
  const calls = allEdges.filter(e => e.kind!=='contains');
  const out = calls.filter(e => e.from===id).map(e => e.to), inn = calls.filter(e => e.to===id).map(e => e.from);
  const keep = new Set([id, ...out, ...inn]);
  nodes.update(DATA.nodes.map(n => ({id:n.id, opacity: keep.has(n.id)?1:0.15,
    color: n.id===id ? n.color : (out.includes(n.id) ? '#1f6feb' : (inn.includes(n.id) ? '#fb8500' : n.color))})));
  const n = DATA.nodes.find(x => x.id===id);
  const li = a => a.length ? '<ul>'+a.map(x => `<li><code>${esc(x)}</code></li>`).join('')+'</ul>' : '（なし）';
  document.getElementById('info').innerHTML = `<b><code>${esc(id)}</code></b><br>${esc(n.doc||'')}<br><br>呼び出し元・import 元（橙）${li(inn)}呼び出し先・import 先（青）${li(out)}`;
});
const s = DATA.stats, rows = [['モジュール', s.modules], ['関数・クラス・メソッド', s.functions], ['関数間の辺', s.function_edges],
  ['モジュール間の import', s.module_edges], ['モジュール間の循環', s.module_cycles.length], ['import *', s.star_imports],
  ['同じ本体の重複定義', s.duplicate_bodies.length], ['層の逆向きの import', s.reverse_layer_imports.length],
  ['どこからも使われない公開関数', s.unreferenced_public_functions.length], ['最大入次数 / 出次数', s.max_in_degree+' / '+s.max_out_degree], ['行数', s.lines_of_code]];
document.getElementById('stats').innerHTML = rows.map(r => `<tr><td>${r[0]}</td><td style="text-align:right">${r[1]}</td></tr>`).join('');
refresh();
</script></body></html>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=Path, default=ROOT / PACKAGE)
    parser.add_argument("--out", type=Path, default=ROOT / "docs" / "dependency_graph.html")
    parser.add_argument("--metrics", type=Path, default=ROOT / "docs" / "graph_metrics.json")
    parser.add_argument("--label", default="after")
    parser.add_argument("--title", default="edinet_xbrl_prep の依存関係グラフ")
    args = parser.parse_args()
    modules = collect(args.src)
    edges = resolve_edges(modules)
    stats = metrics(modules, edges, args.label)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(modules, edges, stats, args.title, args.src), encoding="utf-8")
    history = json.loads(args.metrics.read_text(encoding="utf-8")) if args.metrics.exists() else {}
    history[args.label] = stats
    args.metrics.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in stats.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
