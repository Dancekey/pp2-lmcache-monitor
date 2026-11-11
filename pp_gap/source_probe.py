"""List observation seams in explicitly supplied source files; no framework imports."""
import argparse
import ast
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'observer'))
from ks_pp_trace import transform


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',action='append',required=True,help='MODULE=/actual/source/file.py')
    parser.add_argument('--out',type=Path,required=True)
    args = parser.parse_args()
    results = []
    for value in args.source:
        module,filename = value.split('=',1)
        path = Path(filename).resolve()
        source = path.read_text(encoding='utf-8')
        tree,sites = transform(source,str(path),module)
        compile(tree,str(path),'exec')
        methods = [{'name':n.name,'line':n.lineno,'end_line':n.end_lineno}
                   for n in ast.walk(ast.parse(source)) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))
                   and n.name in ('execute_model','schedule','sample_tokens','step','step_with_batch_queue','check_lookup_result')]
        results.append({'module':module,'path':str(path),'sites':sites,'methods':methods,
                        'status':'SOURCE_CHECKED_RUNTIME_LOADING_NOT_VERIFIED'})
    args.out.write_bytes((json.dumps(results,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))
    print(json.dumps({'modules':len(results),'statement_sites':sum(len(r['sites']) for r in results),'device_imports':0}))
