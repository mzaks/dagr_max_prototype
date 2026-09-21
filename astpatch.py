"""Locate code in MAX by structure, not by quoting it.

The patch scripts rewrite files inside a locally installed MAX. Anchoring those edits on
verbatim excerpts would mean carrying MAX's source in this repository, which its licence does
not allow. These helpers find the same places through the AST — by class, function and
identifier — and, where a replacement has to keep the original code, take it from the file
being patched rather than from a literal here.

Everything works on (source, tree) pairs and returns edits as line-range replacements, so the
untouched parts of the file keep their exact formatting.
"""

from __future__ import annotations

import ast


class NotFound(Exception):
    """Raised when MAX's shape changed: better a clear failure than a silent no-op."""


# -- finding ----------------------------------------------------------------------

def parse(src: str) -> ast.Module:
    return ast.parse(src)


def find_class(tree: ast.AST, name: str) -> ast.ClassDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise NotFound(f"class {name}")


def find_function(tree: ast.AST, name: str, *, in_class: str | None = None) -> ast.FunctionDef:
    scope = find_class(tree, in_class) if in_class else tree
    for node in ast.walk(scope):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise NotFound(f"function {name}" + (f" in {in_class}" if in_class else ""))


def find_stmt(scope: ast.AST, predicate) -> ast.stmt:
    """The one statement under `scope` satisfying `predicate` (ambiguity is an error)."""
    hits = [n for n in ast.walk(scope) if isinstance(n, ast.stmt) and predicate(n)]
    if len(hits) != 1:
        raise NotFound(f"expected exactly one statement, found {len(hits)}")
    return hits[0]


def calls(node: ast.AST, dotted: str) -> bool:
    """Does this node contain a call to `dotted` (e.g. 'BatchMetrics.create', 'print')?"""
    want = dotted.split(".")
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and name_of(sub.func) == want:
            return True
    return False


def name_of(node: ast.AST) -> list[str] | None:
    """['METRICS', 'transaction'] for METRICS.transaction, ['foo'] for foo, else None."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return parts[::-1]


def assigns_to(node: ast.AST, target: str) -> bool:
    return (
        isinstance(node, ast.Assign)
        and any(name_of(t) == [target] for t in node.targets)
    )


def compares_to(node: ast.AST, dotted: str) -> bool:
    """An `if x == Some.CONSTANT:` test against the given dotted name."""
    want = dotted.split(".")
    if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
        return False
    return any(name_of(c) == want for c in node.test.comparators)


# -- editing ----------------------------------------------------------------------

def segment(src: str, node: ast.AST) -> str:
    """The exact source of a node, taken from the file being patched."""
    out = ast.get_source_segment(src, node)
    if out is None:
        raise NotFound(f"no source for {type(node).__name__}")
    return out


def lines_of(src: str, node: ast.stmt) -> tuple[int, int]:
    """Half-open line range [start, end) of a statement, decorators included."""
    start = min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])])
    return start - 1, node.end_lineno


def indent_of(src: str, node: ast.stmt) -> str:
    return " " * node.col_offset


def replace_lines(src: str, start: int, end: int, new_text: str) -> str:
    lines = src.splitlines(keepends=True)
    if new_text and not new_text.endswith("\n"):
        new_text += "\n"
    return "".join(lines[:start]) + new_text + "".join(lines[end:])


def replace_stmt(src: str, node: ast.stmt, new_text: str) -> str:
    start, end = lines_of(src, node)
    return replace_lines(src, start, end, new_text)


def insert_before(src: str, node: ast.stmt, new_text: str) -> str:
    start, _ = lines_of(src, node)
    return replace_lines(src, start, start, new_text)


def insert_at_body_start(src: str, node: ast.stmt, new_text: str) -> str:
    """Insert as the first statement of a block (an `if`, a `with`, a function body)."""
    first = node.body[0]
    start, _ = lines_of(src, first)
    return replace_lines(src, start, start, new_text)


def replace_body(src: str, func: ast.FunctionDef, new_body: str) -> str:
    """Replace a function's body, keeping its signature, decorators and docstring line."""
    first = func.body[0]
    start, _ = lines_of(src, first)
    return replace_lines(src, start, func.body[-1].end_lineno, new_body)


def reindent(text: str, indent: str) -> str:
    return "".join(indent + line if line.strip() else line
                   for line in text.splitlines(keepends=True))
