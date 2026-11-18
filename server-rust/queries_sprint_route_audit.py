#!/usr/bin/env python3
"""Registration inventory for the frozen Go query routes.

This is NOT an HTTP parity, middleware-reachability or production-registration
proof: it statically resolves chi registrations in the canonical Go router and
only claims that each frozen matrix route is registered once, with the same
method, handler and leaf path literal, under the single root router. Rust
entries are static registration candidates, not evidence of behavior.

The v52 scanner expected one ``zbs.Register(r,`` mount in ``cmd/server/router.go``
and the rest of the routes in ``internal/zbs/routes.go``. The canonical tree
registers everything inline through nested ``r.Route``/``r.Group`` blocks, so
this version follows that shape instead of loosening the assertions.
"""
import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

METHODS = "Get|Post|Put|Patch|Delete|Head|Options"
CALL = re.compile(r"\.(" + METHODS + r")\s*\(")
ROUTE = re.compile(r"\.Route\s*\(")
ROOT_ROUTER = re.compile(r"\bchi\.NewRouter\s*\(\s*\)")
ROUTER_FUNC = re.compile(r"func NewRouterWithOptions\s*\(")


def masked(text):
    """Blank out comments and string/rune literals, preserving offsets."""
    pattern = r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|`[^`]*`|\'(?:\\.|[^\'\\])\''
    return re.sub(pattern, lambda m: ''.join('\n' if c == '\n' else ' ' for c in m[0]), text)


def match_close(text, open_at, opener, closer):
    depth = 0
    for i in range(open_at, len(text)):
        if text[i] == opener:
            depth += 1
        elif text[i] == closer:
            depth -= 1
            if depth == 0:
                return i
    raise AssertionError(f"unbalanced {opener} at {open_at}")


def split_args(text, open_at, close_at):
    """Split the arguments of a call into top-level (start, end) spans."""
    spans, start, depth = [], open_at + 1, 1
    for i in range(open_at + 1, close_at):
        c = text[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == ',' and depth == 1:
            spans.append((start, i))
            start = i + 1
    if text[start:close_at].strip():
        spans.append((start, close_at))
    return spans


def unquote(raw):
    raw = raw.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in '`"':
        return raw[1:-1], True
    return raw, False


def go_routes(router):
    text = router.read_text()
    clean = masked(text)
    line_of = lambda offset: text.count('\n', 0, offset) + 1

    roots = [m.start() for m in ROOT_ROUTER.finditer(clean)]
    assert len(roots) == 1, f"expected one root chi.NewRouter(), got {len(roots)}"
    router_funcs = [m.start() for m in ROUTER_FUNC.finditer(clean)]
    assert len(router_funcs) == 1, f"expected one NewRouterWithOptions, got {len(router_funcs)}"
    func_open = clean.index('{', router_funcs[0])
    func_span = (func_open, match_close(clean, func_open, '{', '}'))

    blocks = []
    for m in ROUTE.finditer(clean):
        open_at = clean.index('(', m.start())
        close_at = match_close(clean, open_at, '(', ')')
        args = split_args(clean, open_at, close_at)
        assert len(args) == 2, (line_of(m.start()), 'Route expects path and handler')
        prefix, literal = unquote(text[args[0][0]:args[0][1]])
        # The chi subtree is the func literal passed as the second argument, so
        # its body brace sits inside the call, not after it.
        brace = clean.find('{', args[1][0])
        assert brace != -1 and brace < close_at, (line_of(m.start()), 'Route handler is not a block')
        end = match_close(clean, brace, '{', '}')
        assert not clean[end + 1:close_at].strip(), (line_of(m.start()), 'Route call continues after the block')
        blocks.append({'start': brace, 'end': end,
                       'prefix': prefix, 'literal': literal, 'line': line_of(m.start())})
    assert blocks, "no r.Route blocks found in the canonical router"

    def chain(offset):
        outer = [b for b in blocks if b['start'] < offset < b['end']]
        outer.sort(key=lambda b: b['start'])
        for inner, parent in zip(outer[1:], outer):
            assert parent['start'] < inner['start'] < inner['end'] < parent['end'], \
                'route blocks must nest'
        return outer

    rows = []
    for m in CALL.finditer(clean):
        method = m.group(1)
        receiver = clean[max(0, m.start() - 220):m.start()]
        if not re.search(r"(?:^|[^\w.])r(?:\.With\s*\([^()]*\))?$", receiver):
            continue
        open_at = clean.index('(', m.start())
        close_at = match_close(clean, open_at, '(', ')')
        args = split_args(clean, open_at, close_at)
        assert len(args) == 2, (line_of(m.start()), f'{method} expects path and handler')
        declared, literal = unquote(text[args[0][0]:args[0][1]])
        handler_raw = text[args[1][0]:args[1][1]].strip()
        handler = re.search(r'(?:^|\.)(\w+)$', handler_raw)
        enclosing = chain(m.start())
        prefix = ''.join(b['prefix'] for b in enclosing)
        rows.append({
            'method': method.upper(), 'path': prefix + declared,
            'leaf': declared, 'handler': handler.group(1) if handler else handler_raw,
            'handler_expression': handler_raw, 'path_literal': literal,
            'route_prefixes': [b['prefix'] for b in enclosing],
            'prefix_literals': [b['literal'] for b in enclosing],
            'go_file': str(router), 'go_line': line_of(m.start()),
            'wrapped_middleware': '.With(' in receiver[-60:],
            'inside_router_function': func_span[0] < m.start() < func_span[1],
        })
    return rows, {'root_routers': len(roots), 'router_functions': len(router_funcs),
                  'route_blocks': len(blocks), 'route_blocks_outside_registerer':
                      sum(1 for b in blocks if not func_span[0] < b['start'] < func_span[1])}


def normalized(path):
    return re.sub(r'\{[^}]+\}', '{}', path.rstrip('/') or '/')


def rust_routes(root):
    result = {}
    files = [root / 'src/api.rs', *sorted((root / 'src/api').glob('*.rs'))]
    for path in files:
        text = path.read_text()
        clean = masked(text)
        for match in re.finditer(r'\.route\(\s*"([^"\n]+)"', text):
            start = text.index('(', match.start())
            depth, end = 1, start + 1
            while end < len(clean) and depth:
                depth += (clean[end] == '(') - (clean[end] == ')')
                end += 1
            assert depth == 0, (path, match[1])
            for method in set(re.findall(r'\b(get|post|put|patch|delete|head|options)\s*\(', clean[start:end])):
                key = method.upper(), normalized(match[1])
                result.setdefault(key, []).append({'file': str(path.relative_to(root)),
                                                   'line': text.count('\n', 0, match.start()) + 1,
                                                   'path': match[1]})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go-root', type=Path, required=True,
                        help='canonical Go server directory (contains cmd/server/router.go)')
    parser.add_argument('--rust-root', type=Path, default=Path('.'))
    parser.add_argument('--matrix', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    router = args.go_root / 'cmd/server/router.go'
    rows, structure = go_routes(router)
    assert rows, "no route registrations parsed"
    assert structure['root_routers'] == 1 and structure['router_functions'] == 1, structure
    assert structure['route_blocks_outside_registerer'] == 0, structure

    matrix = json.loads(args.matrix.read_text())['routes']
    owned = [row for row in matrix if row['sprint_owner'] == 'queries']
    rust = rust_routes(args.rust_root)
    inventory, claimed = [], set()
    for row in owned:
        leaf, _ = unquote(row['path_expression'])
        matches = [r for r in rows if r['method'] == row['method']
                   and r['handler'] == row['handler'] and r['leaf'] == leaf]
        assert len(matches) == 1, (row['handler'], row['method'], leaf, len(matches))
        item = dict(matches[0])
        assert item['inside_router_function'], item
        assert item['path_literal'], f"unresolved path expression: {item}"
        assert all(item['prefix_literals']), f"unresolved route prefix: {item}"
        key = (item['go_line'], item['method'], item['handler'], item['path'])
        assert key not in claimed, f"matrix rows share a registration: {key}"
        claimed.add(key)
        item['matrix_line'] = row['line']
        item['matrix_state'] = row.get('state')
        item['rust_registration_candidates'] = rust.get((item['method'], normalized(item['path'])), [])
        item['declaration_status'] = ('present_not_parity_proven'
                                      if item['rust_registration_candidates'] else 'absent')
        inventory.append(item)
    assert len(inventory) == 76, len(inventory)
    assert all(r['path_literal'] for r in inventory), 'query route path is not a literal'
    assert len({(r['method'], normalized(r['path'])) for r in inventory}) == 76, 'duplicate query route'

    extras = [r for r in rows if (r['go_line'], r['method'], r['handler'], r['path']) not in claimed]
    for row in extras:
        row['owner_note'] = (
            'automation: rule definition, coordinator must confirm' if row['handler'] == 'GetGatewayDailyRule'
            else 'channels: installation/guest read, coordinator must confirm'
            if 'Guest' in row['handler'] or 'ZaloInstall' in row['handler']
            else 'issue mutation preview; coordinator must confirm'
            if row['handler'] == 'PreviewCommentSubIssue'
            else 'outside query scope; not reassigned by this audit')
    result = {
        'scope': ('Frozen Go query route REGISTRATION INVENTORY only. Static Rust candidates are not '
                  'proof of reachability, response/auth/event parity, middleware coverage or production '
                  'registration. Path parameters and trailing slashes normalized for matching.'),
        'go_router_structure': structure,
        'sources_sha256': {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                           for p in [router, args.matrix]},
        'query_count': len(inventory),
        'query_methods': dict(Counter(r['method'] for r in inventory)),
        'query_route_prefixes': sorted({tuple(r['route_prefixes']) for r in inventory}),
        'query_absent': [{'handler': r['handler'], 'method': r['method'], 'path': r['path']}
                         for r in inventory if r['declaration_status'] == 'absent'],
        'query_inventory': inventory,
        'omitted_registrations': extras,
        'warning': ('Matrix also has non-literal public API constants, not enumerated by this literal-route '
                    'scanner. No owner was changed; extra route assignments require coordinator '
                    'acknowledgement.'),
    }
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps({'query_count': len(inventory),
                      'absent_handlers': [r['handler'] for r in result['query_absent']],
                      'extra_literal_registrations': len(extras),
                      'go_router_structure': structure}, ensure_ascii=False))


if __name__ == '__main__':
    main()