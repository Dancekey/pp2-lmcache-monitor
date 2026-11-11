"""Prepare PP-gap configuration and run CPU checks. Never start services."""
import argparse
import json
import os
from pathlib import Path
import py_compile
import shlex
import subprocess
import sys

HERE = Path(__file__).resolve().parent


def write_new(path, content):
    if not path.exists():
        path.write_bytes(content.encode('utf-8'))


def deploy(out, run_id):
    if sys.version_info < (3, 10):
        raise RuntimeError('Use the existing Python 3.10+ environment')
    out = out.resolve()
    if out == HERE or out == HERE.parent or out in HERE.parents:
        raise ValueError('--out must be a separate private output directory')
    marker = out/'deployment.json'
    if out.exists() and any(out.iterdir()):
        if not marker.is_file() or json.loads(marker.read_text(encoding='utf-8')).get('tool') != 'pp_gap':
            raise ValueError('Output is not an existing PP-gap deployment; choose a new private directory')
        if json.loads(marker.read_text(encoding='utf-8')).get('run_id') != run_id:
            raise ValueError('A different run-id requires a new output directory')
    out.mkdir(parents=True, exist_ok=True)
    scratch = out/'cpu-tmp'
    scratch.mkdir(exist_ok=True)
    sources = sorted(HERE.glob('*.py'))+sorted((HERE/'observer').glob('*.py'))
    for path in sources:
        py_compile.compile(str(path),doraise=True)
    env = dict(os.environ)
    for key in ('PYTHONPATH','PYTHONHOME','KS_WAIT_TRACE_DIR','KS_SAFE_FAULT_DIR'):
        env.pop(key,None)
    env.update(KS_PP_DIAG='0',TORCH_DEVICE_BACKEND_AUTOLOAD='0',VLLM_PLUGINS='',
               TMPDIR=str(scratch),TEMP=str(scratch),TMP=str(scratch))
    subprocess.run([sys.executable,'-I','-S',str(HERE/'cpu_check.py'),
                    '--out',str(out/'cpu-check-result.json')],
                   env=env,cwd=str(out),check=True)
    checks = json.loads((out/'cpu-check-result.json').read_text(encoding='utf-8'))
    if checks['count'] != 30 or checks['serving_framework_modules_imported']:
        raise RuntimeError('CPU-only validation did not meet the expected contract')
    for group in ('OFF','ON'):
        write_new(out/('runtime-'+group+'.json'),(HERE/'runtime.template.json').read_text(encoding='utf-8'))
        for node in ('pp0','pp1'):
            values = {'KS_PP_DIAG':'1','KS_PP_DIAG_PP':'2','KS_PP_DIAG_RUN':run_id,
                      'KS_PP_DIAG_GROUP':group,'KS_PP_DIAG_NODE':node,
                      'KS_PP_DIAG_DIR':str(out/'traces'/group/node),'KS_PP_DIAG_RANGES':'1'}
            content = '# Source only before the next authorized service launch.\n'
            content += '\n'.join('export '+key+'='+shlex.quote(value) for key,value in values.items())+'\n'
            write_new(out/('env-'+group+'-'+node+'.sh'),content)
    receipt = {'tool':'pp_gap','state':'DEPLOYED_CPU_CHECKED_NO_SERVICES',
               'code_directory':str(HERE),'output_directory':str(out),'run_id':run_id,
               'python':sys.executable,'cpu_checks_passed':checks['count'],
               'python_files_compiled':len(sources),'serving_framework_modules_imported':[],
               'model_cache_start_actions':0,'network_requests':0,'observer_enabled':False,
               'customer_prompt_data_included':False,
               'note':'Existing runtime/environment files are preserved; review them before a later device run'}
    marker.write_bytes((json.dumps(receipt,indent=2)+'\n').encode('utf-8'))
    print('CPU_CHECKS_PASSED=30',flush=True)
    print('DEPLOYED_CPU_CHECKED_NO_SERVICES',flush=True)
    print('DEPLOYMENT_RECEIPT='+str(marker),flush=True)
    print('OBSERVATION_BOOTSTRAP='+str(HERE/'observer/ks_pp_startup.py'),flush=True)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True,help='Confirmed private container output directory')
    parser.add_argument('--run-id',default='pp-gap-001',help='Use the same ID on both PP nodes')
    args = parser.parse_args()
    deploy(args.out,args.run_id)
