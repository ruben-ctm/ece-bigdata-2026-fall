import ast, io, json, sys, contextlib, traceback, os
import pandas as pd
pd.set_option("display.width", 220); pd.set_option("display.max_columns", 30)
pd.set_option("display.max_colwidth", 70)
ns = {}
log = []
def cell(section, code):
    code = code.strip("\n")
    buf = io.StringIO()
    tree = ast.parse(code)
    last = None
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        last = ast.Expression(tree.body.pop().value)
    err = None
    with contextlib.redirect_stdout(buf):
        try:
            exec(compile(tree, "<cell>", "exec"), ns)
            if last is not None:
                v = eval(compile(last, "<cell>", "eval"), ns)
                if v is not None: print(repr(v) if not isinstance(v, pd.DataFrame) else v.to_string())
        except Exception:
            err = traceback.format_exc(limit=2)
            print(err.strip().splitlines()[-1])
    out = buf.getvalue().rstrip("\n")
    log.append({"section": section, "kind": "python", "code": code, "output": out, "error": bool(err)})
    print(f"[{section}] {'ERR' if err else 'ok'}: {code.splitlines()[0][:70]}", file=sys.stderr)
def bash(section, cmd):
    import subprocess
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, executable="/bin/bash")
    out = (r.stdout + r.stderr).rstrip("\n")
    log.append({"section": section, "kind": "bash", "code": cmd.strip(), "output": out, "error": r.returncode != 0})
    print(f"[{section}] bash rc={r.returncode}: {cmd.strip()[:70]}", file=sys.stderr)
def save(path): json.dump(log, open(path, "w"), indent=1)
