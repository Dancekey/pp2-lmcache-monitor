"""Adapt old comparison client: explicit new runtime/window, native DS replay only."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import time
import urllib.request

HERE = Path(__file__).resolve().parent


def argv_for(plan, runtime, group, phase):
    argv = list(plan['commands'][group+'/'+phase]['argv'])
    for key, value in runtime['variables'].items():
        argv = [x.replace('${'+key+'}',str(value)) for x in argv]
    if any('${' in x for x in argv):
        raise ValueError('Unresolved runtime argument')
    return argv


def check_window(runtime):
    window = runtime['authorization']
    if not window or not window['machines']:
        raise ValueError('No new device window recorded')
    start, end = (datetime.fromisoformat(window[k]) for k in ('start','end'))
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError('Window must contain explicit timezone offsets')
    if not start.timestamp() <= time.time() < end.timestamp():
        raise ValueError('Outside the new authorized window')
    return end.timestamp()


def identity(pid):
    fields = (Path('/proc')/str(pid)/'stat').read_text().rsplit(')',1)[1].split()
    return {'pid':pid,'state':fields[0],'session':int(fields[3]),'start_ticks':int(fields[19])}


def run(args):
    runtime = json.loads(args.runtime.read_text(encoding='utf-8'))
    plan = json.loads(args.plan.read_text(encoding='utf-8'))
    argv = argv_for(plan,runtime,args.group,args.phase)
    if not args.execute:
        print(shlex.join(argv))
        return
    if os.name != 'posix':
        raise ValueError('Execute only in the new private Linux container')
    deadline = check_window(runtime)
    root = Path(runtime['variables']['PRIVATE_RUN_DIR']).resolve()
    if root == Path('/') or str(root).startswith('/root/'):
        raise ValueError('Expected a confirmed private container directory')
    model = identity(runtime['model_pid'])
    if model['state'] == 'Z' or model['start_ticks'] != runtime['model_start_ticks']:
        raise ValueError('Model identity no longer matches new runtime receipt')
    folder = root/'results'/args.group/args.phase
    folder.mkdir(parents=True,exist_ok=False)
    url = runtime['variables']['P_API_URL'].rstrip('/')
    def metrics(label):
        with urllib.request.urlopen(url+'/metrics',timeout=3) as response:
            payload = response.read().decode()
        (folder/(label+'.prom')).write_text(payload,encoding='utf-8')
    with urllib.request.urlopen(url+'/health',timeout=3) as response:
        if response.status != 200:
            raise ValueError('P API not ready')
    metrics('before')
    env = dict(os.environ,TORCH_DEVICE_BACKEND_AUTOLOAD='0',VLLM_PLUGINS='',TOKENIZERS_PARALLELISM='false',
               KS_PP_DIAG='1',KS_PP_DIAG_PP='2',KS_PP_DIAG_RUN=plan['run_id'],KS_PP_DIAG_GROUP=args.group,
               KS_PP_DIAG_NODE=runtime['node'],KS_PP_DIAG_DIR=str(root/'raw'/args.group/'client'),
               PYTHONPATH=str(HERE/'observer')+os.pathsep+os.environ.get('PYTHONPATH',''))
    env.pop('KS_WAIT_TRACE_DIR',None)
    env.pop('KS_SAFE_FAULT_DIR',None)
    started_ns = time.monotonic_ns()
    with (folder/'benchmark.log').open('xb',buffering=0) as log:
        client = subprocess.Popen(argv,cwd=root,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        owned = identity(client.pid)
        receipt = {'state':'ACTUALLY_STARTED','argv':argv,'client':owned,'model':model,
                   'start_mono_ns':started_ns,'start_wall_ns':time.time_ns(),'deadline':deadline,
                   'authorization':runtime['authorization'],'model_cache_stop_actions':'none'}
        (folder/'client-start.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
        timed_out = False
        next_metrics = time.monotonic()+10
        while client.poll() is None:
            if time.time() >= deadline:
                timed_out = True
                current = identity(client.pid)
                if current['start_ticks'] != owned['start_ticks']:
                    raise ValueError('Client PID identity changed; no signal sent')
                os.killpg(owned['session'],signal.SIGTERM)
                try:
                    client.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(owned['session'],signal.SIGKILL)
                    client.wait(timeout=5)
                break
            if time.monotonic() >= next_metrics:
                try:
                    metrics('sample-'+str(time.monotonic_ns()))
                except Exception as exc:
                    with (folder/'metrics-errors.jsonl').open('a',encoding='utf-8') as f:
                        f.write(json.dumps({'error':str(exc),'mono_ns':time.monotonic_ns()})+'\n')
                next_metrics = time.monotonic()+10
            time.sleep(min(1,max(0,deadline-time.time())))
    native_path = folder/'benchmark.json'
    native = json.loads(native_path.read_text(encoding='utf-8')) if native_path.exists() else None
    result = {'state':'CLIENT_COMPLETED' if client.returncode == 0 and native else 'CLIENT_FAILED_OR_INCOMPLETE',
              'returncode':client.returncode,'timed_out':timed_out,'native':native,
              'end_mono_ns':time.monotonic_ns(),'start':receipt,'service_stop_actions':'none'}
    (folder/'client-result.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    try:
        metrics('after')
    except Exception as exc:
        (folder/'after-metrics-error.txt').write_text(str(exc),encoding='utf-8')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan',type=Path,default=HERE/'prepared/benchmark-plan.json')
    p.add_argument('--runtime',type=Path,required=True)
    p.add_argument('--group',choices=['OFF','ON'],required=True)
    p.add_argument('--phase',choices=['cold','warm'],required=True)
    p.add_argument('--execute',action='store_true')
    run(p.parse_args())
