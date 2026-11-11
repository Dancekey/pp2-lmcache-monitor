"""Local contract checks only; synthetic inputs are not model/device evidence."""
import ast
import collections
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / 'observer'))
import ks_pp_trace as trace
from analyze import union, gaps, analyze
from import_device import convert
from align_timeline import align, calibration
from comparison_client import argv_for, check_window


def checks():
    done = []
    def check(name, condition):
        if not condition:
            raise AssertionError(name)
        done.append(name)

    check('overlapping compute counted once', union([(0,10),(5,15),(20,25)]) == [[0,15],[20,25]])
    check('gaps are clipped to capture window', gaps([(-5,5),(10,20)],0,15) == [[5,10]])
    check('empty evidence preserves whole unknown window', gaps([],0,10) == [[0,10]])
    output = types.SimpleNamespace(num_scheduled_tokens={'r1': 4}, scheduled_new_reqs=[
        types.SimpleNamespace(req_id='r1', num_computed_tokens=0, prompt_token_ids=[1,2,3,4])],
        scheduled_cached_reqs=types.SimpleNamespace(req_ids=[],num_computed_tokens=[]))
    trace._counts.clear()
    a, b = trace.new_batch(output,'executor'), trace.new_batch(output,'worker')
    check('independent executor worker association', a['batch'] == b['batch'])
    check('same request in a new step differs', trace.new_batch(output,'worker')['batch'] != b['batch'])
    output.num_scheduled_tokens['r1'] = 3
    check('actual token shape enters identity', trace.batch_shape(output)['fingerprint'] != a['fingerprint'])
    received = []
    class P:
        vllm_config = types.SimpleNamespace(parallel_config=types.SimpleNamespace(pipeline_parallel_size=1))
        def execute_model(self, value):
            received.append(value)
            return value
    old = P.execute_model
    P.execute_model = trace._method('worker.execute',check_pp=True,make_batch='worker')(old)
    trace.emit = lambda event, **fields: received.append(event)
    marker = object()
    check('PP1 returns original object and no events', P().execute_model(marker) is marker and received == [marker])
    P.vllm_config.parallel_config.pipeline_parallel_size = 2
    check('PP2 malformed observation cannot block business', P().execute_model(marker) is marker)
    class Broken(P):
        def go(self):
            raise ValueError('original')
    wrapped = trace._method('test',check_pp=True)(Broken.go)
    try:
        wrapped(Broken())
    except ValueError as exc:
        check('original exceptions propagated', str(exc) == 'original')
    check('thread context restored after exception', not getattr(trace._tls,'active',False))
    class F:
        pass
    class RPC:
        def lookup(self, *args, **kwargs):
            return marker_future
    marker_future = F()
    proxy = trace._Client(RPC(),'server')
    check('RPC returns same Future without query', proxy.lookup('r1') is marker_future)
    source = '''from __future__ import annotations
class Worker:
    def execute_model(self, scheduler_output):
        for handle in self._pp_send_work:
            handle.wait()
        output = self.model_runner.execute_model(scheduler_output, None)
        self._pp_send_work = self.pp.isend_tensor_dict(output)
        return output
'''
    tree, sites = trace.transform(source,'fixture.py','vllm_ascend.worker.worker')
    order = []
    class Handle:
        def wait(self):
            order.append('wait')
    class Runner:
        def execute_model(self, *args):
            order.append('model')
            return marker
    class PP:
        def isend_tensor_dict(self,value):
            check('activation unchanged', value is marker)
            order.append('send')
            return []
    env = {}
    exec(compile(tree,'fixture.py','exec'), env)
    w = env['Worker']()
    w._pp_send_work, w.model_runner, w.pp = [Handle()], Runner(), PP()
    trace._tls.active = True
    result = w.execute_model(None)
    trace._tls.active = False
    check('AST preserves original wait model send order', order == ['wait','model','send'] and result is marker)
    check('AST reports exact required boundaries', {x['event'] for x in sites} == {'pp.previous_send_wait','worker.model_host','pp.activation_send_submit'})
    tree, _ = trace.transform(source,'fixture.py','vllm_ascend.worker.worker')
    forbidden = {'synchronize', 'query', 'all_reduce', 'barrier', 'add_done_callback'}
    check('AST adds no synchronization or future operation', not any(isinstance(n,ast.Attribute) and n.attr in forbidden for n in ast.walk(tree)))
    with tempfile.TemporaryDirectory(prefix='ks-pp-diag-') as tmp:
        root = Path(tmp)
        base = dict(run='CPU_DEMO_ONLY',group='OFF',node='a',pid=1,thread=1,mono_ns=1,wall_ns=1,
                    evidence='synthetic fixture, not real model')
        rows = [dict(base,event='batch.map',role='executor',**a)]
        for pp in range(2):
            for tp in range(16):
                rows.append(dict(base,event='batch.map',role='worker',pp_rank=pp,tp_rank=tp,**b))
        result = analyze(rows,[],root/'complete')
        check('32-rank association required', result['ambiguous_or_incomplete_batches'] == 0)
        result = analyze(rows[:-1],[],root/'missing')
        check('missing TP rank blocks complete association', result['ambiguous_or_incomplete_batches'] == 1)
        result = analyze(rows+[rows[-1]],[],root/'duplicate')
        check('duplicate rank is ambiguous', result['ambiguous_or_incomplete_batches'] == 1)
        check('no device trace never claims compute gaps', result['device_evidence'] == 'NOT_CAPTURED' and not result['device_lanes'])
    mapping = {'lane_classification_verified':False,'host_device_anchor_verified':False}
    try:
        convert({'traceEvents':[]},mapping)
    except ValueError:
        check('unverified device mapping rejected', True)
    samples = [{'node':'b','reference':'a','anchor':{'node_mono_ns':n,'reference_mono_ns':r,'boot_id':'same','error_ns':2}}
               for n,r in [(100,0),(200,100)]]
    aligned, _ = align([dict(node='b',mono_ns=150,start_ns=140),dict(node='b',mono_ns=250)],[],samples)
    check('calibrated axis converts local times', aligned[0]['aligned_mono_ns'] == 50 and aligned[0]['aligned_start_ns'] == 40)
    check('no extrapolation beyond calibration', 'aligned_mono_ns' not in aligned[1])
    samples[1]['anchor']['boot_id'] = 'reboot'
    try:
        calibration(samples)
    except ValueError:
        check('reboot breaks cross-host alignment', True)
    try:
        check_window({'authorization':None})
    except ValueError:
        check('client refuses missing device authorization',True)
    plan = json.loads((HERE/'prepared/benchmark-plan.json').read_text(encoding='utf-8'))
    runtime = {'variables':dict(P_API_URL='http://localhost:8000',SERVED_MODEL='test',ACTUAL_MODEL_PATH='/private/model',PRIVATE_DATA_DIR='/private/data',PRIVATE_RUN_DIR='/private/run')}
    argv = argv_for(plan,runtime,'OFF','cold')
    check('client uses original output lengths and unlimited client concurrency',argv[argv.index('--custom-output-len')+1] == '-1' and '--max-concurrency' not in argv)
    check('native command has no unresolved variables or historical window',not any('${' in x or '0312' in x or '1794357900' in x for x in argv))
    mapping = dict(lane_classification_verified=True,host_device_anchor_verified=True,compute_coverage_verified=True,
                   run='CPU_DEMO_ONLY',group='OFF',node='a',clock_domain='node_monotonic',raw_trace_path='synthetic',
                   anchors=dict(mono_ns=0,trace_us=0,mono_ns_per_trace_us=1000,error_ns=1),
                   capture_start_trace_us=0,capture_end_trace_us=10,
                   lanes=[dict(pid=1,tid=2,pp_rank=0,tp_rank=0,kind='compute',event_names=['matmul'])])
    devices = convert({'traceEvents':[dict(ph='X',pid=1,tid=2,ts=1,dur=4,name='matmul'),dict(ph='X',pid=1,tid=2,ts=3,dur=4,name='matmul')]},mapping)
    with tempfile.TemporaryDirectory(prefix='ks-pp-device-') as tmp:
        r = analyze([],devices,Path(tmp)/'analysis')
        check('device overlapping kernels do not double count',r['device_lanes'][0]['compute_union_ns'] == 6000)
        check('device gaps retain unknown cause',all(g['category'] == 8 for g in r['device_lanes'][0]['compute_gaps']))
    for enabled, pp in [('0','2'),('1','1')]:
        env = dict(os.environ, PYTHONPATH=str(HERE/'observer'), KS_PP_DIAG=enabled,KS_PP_DIAG_PP=pp)
        startup = "import sys; sys.path.insert(0, " + repr(str(HERE/'observer')) + "); import ks_pp_startup; assert 'ks_pp_trace' not in sys.modules"
        r = subprocess.run([sys.executable,'-I','-S','-c',startup],env=env,capture_output=True,text=True)
        check('no hook import for disabled or PP1 '+enabled+pp,r.returncode == 0)
    return done


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=HERE/'cpu-check-result.json')
    args = parser.parse_args()
    names = checks()
    serving_roots = {'torch','torch_npu','vllm','vllm_ascend','lmcache','lmcache_ascend','acl'}
    imported = sorted(n for n in sys.modules if n.split('.')[0] in serving_roots)
    if imported:
        raise RuntimeError('CPU-only check imported serving frameworks: ' + repr(imported))
    result = {'state':'CPU_CONTRACT_CHECKS_PASSED','count':len(names),'checks':names,
              'serving_framework_modules_imported': imported,
              'boundary':'Synthetic CPU checks only; no real vllm execution, NPU, model correctness or performance benefit'}
    args.out.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({'state':result['state'],'count':len(names)}))
