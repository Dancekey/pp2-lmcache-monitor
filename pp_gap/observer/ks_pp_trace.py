"""Host-only PP diagnostic hooks. Never query a Future or synchronize a device."""
import ast
import collections
import contextlib
import functools
import hashlib
import inspect
import json
import os
from pathlib import Path
import sys
import threading
import time
import weakref

TARGETS = {
    'vllm.v1.engine.core', 'vllm.v1.executor.multiproc_executor',
    'vllm.v1.core.sched.scheduler', 'vllm.v1.core.sched.async_scheduler',
    'vllm.v1.worker.gpu_worker', 'vllm_ascend.worker.worker',
    'vllm_ascend.worker.model_runner_v1',
    'vllm.distributed.kv_transfer.kv_connector.v1.multi_connector',
    'lmcache.integration.vllm.lmcache_mp_connector',
    'lmcache.integration.vllm.vllm_multi_process_adapter',
    'lmcache.v1.multiprocess.futures', 'lmcache.v1.multiprocess.native_completion',
    'lmcache.v1.multiprocess.modules.lmcache_driven_transfer',
    'vllm.benchmarks.lib.endpoint_request_func',
}
_tls = threading.local()
_fd = None
_pid = None
_counts = collections.Counter()
_future_tags = weakref.WeakKeyDictionary()
_last_worker_batch = None
_polls = {}
_rank = {}
_write_errors = 0
_rpc_tags = weakref.WeakKeyDictionary()
_span_last = {}
_point_last = {}
_externals = {}
_client_ids = {}
_identity_pids = set()
_prompt_sizes = {}


def emit(event, **fields):
    """Write bounded JSON primitives; failure never changes serving behavior."""
    global _fd, _pid, _write_errors
    try:
        pid = os.getpid()
        if _pid != pid:
            folder = Path(os.environ['KS_PP_DIAG_DIR'])
            folder.mkdir(parents=True, exist_ok=True)
            _fd = os.open(str(folder / f'pp-{pid}.jsonl'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            _pid = pid
        row = {'event': event, 'run': os.environ.get('KS_PP_DIAG_RUN'),
               'group': os.environ.get('KS_PP_DIAG_GROUP'), 'node': os.environ.get('KS_PP_DIAG_NODE'),
               'pid': pid, 'thread': threading.get_native_id(), 'mono_ns': time.monotonic_ns(),
               'wall_ns': time.time_ns(), 'write_errors': _write_errors, **_rank,
               **(getattr(_tls, 'batch', None) or {}), **fields}
        os.write(_fd, (json.dumps(row, separators=(',', ':')) + '\n').encode('utf-8'))
    except Exception:
        _write_errors += 1


def pp2(obj):
    cfg = getattr(obj, 'vllm_config', None)
    if cfg is None:
        cfg = getattr(obj, 'parallel_config', None)
    else:
        cfg = getattr(cfg, 'parallel_config', None)
    return getattr(cfg, 'pipeline_parallel_size', None) == 2


def loaded_identity():
    pid = os.getpid()
    if pid in _identity_pids:
        return
    _identity_pids.add(pid)
    modules = {}
    for name in ('lmcache','lmcache_ascend','lmcache_ascend.c_ops','lmcache_native','vllm','vllm_ascend','torch_npu'):
        module = sys.modules.get(name)
        modules[name] = {'path':getattr(module,'__file__',None),
                         'version':str(getattr(module,'__version__',None))} if module is not None else None
    emit('runtime.loaded_identity', modules=modules)


def batch_shape(output):
    """Use only existing CPU metadata. No added serialized batch field."""
    tokens = dict(getattr(output, 'num_scheduled_tokens', {}))
    positions, prompts = {}, {}
    for r in getattr(output, 'scheduled_new_reqs', []):
        rid = str(r.req_id)
        positions[rid] = r.num_computed_tokens
        pt = getattr(r, 'prompt_token_ids', None)
        prompts[rid] = len(pt) if pt is not None else None
        if pt is not None:
            _prompt_sizes[rid] = len(pt)
    cached = getattr(output, 'scheduled_cached_reqs', None)
    if cached is not None:
        positions.update(zip(cached.req_ids, cached.num_computed_tokens))
    canonical = {'tokens': sorted(tokens.items()), 'computed': sorted(positions.items())}
    fingerprint = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()[:24]
    types = {}
    for rid,count in tokens.items():
        p, length = positions.get(rid), _prompt_sizes.get(rid)
        if p is None or length is None:
            types[rid] = 'unknown'
        elif p >= length:
            types[rid] = 'decode'
        elif p+count <= length:
            types[rid] = 'prefill'
        else:
            types[rid] = 'prefill_and_decode'
    kinds = set(types.values())
    return {'fingerprint': fingerprint, 'requests': sorted(tokens), 'scheduled_tokens': tokens,
            'computed': positions, 'prompt_tokens_known': prompts,
            'request_types': types, 'batch_type': next(iter(kinds)) if len(kinds) == 1 else 'mixed' if kinds else 'empty',
            'external_restore_tokens': {rid: _externals.get(rid) for rid in tokens},
            'tokens': sum(tokens.values()), 'request_count': len(tokens)}


def new_batch(output, role):
    shape = batch_shape(output)
    key = (role, shape['fingerprint'])
    _counts[key] += 1
    return dict(shape, occurrence=_counts[key], batch=f"{shape['fingerprint']}:{_counts[key]}")


def local_fields(values):
    """Never serialize tensors, KV blocks, prompts, keys or whole request objects."""
    out = {}
    for k in ('token_budget', 'num_new_local_computed_tokens', 'num_external_computed_tokens',
              'num_new_tokens', 'num_computed_tokens', 'num_reqs', 'output_rank','ext_tokens','load_kv_async'):
        v = values.get(k)
        if v is None or isinstance(v, (int, float, bool, str)):
            out[k] = v
    request = values.get('request')
    if request is not None:
        out['request'] = str(getattr(request, 'request_id', ''))
        out['request_status'] = str(getattr(request, 'status', ''))
    preempted = values.get('preempted_req')
    if preempted is not None:
        out['preempted_request'] = str(getattr(preempted,'request_id',''))
    return out


def point(event, values):
    if not getattr(_tls, 'active', False):
        return
    fields = local_fields(values)
    key = (event, fields.get('request'))
    now = time.monotonic_ns()
    if now-_point_last.get(key, 0) >= 1_000_000_000:
        _point_last[key] = now
        emit(event + '.observed', **fields)


def client_point(values):
    request = values.get('request_func_input')
    rid = getattr(request, 'request_id', None)
    data = values.get('data')
    if not rid or not isinstance(data, dict):
        return
    server_id = data.get('id')
    if rid not in _client_ids:
        _client_ids[rid] = server_id
        emit('client.server_identity', request=rid, server_id=server_id)
    for choice in data.get('choices') or []:
        if choice.get('finish_reason') is not None:
            emit('client.finish_reason', request=rid, server_id=server_id,
                 finish_reason=choice['finish_reason'])


@contextlib.contextmanager
def span(event, values=None):
    if not getattr(_tls, 'active', False):
        yield
        return
    fields = local_fields(values or {})
    start = time.monotonic_ns()
    if event.startswith('scheduler.'):
        key = (event, fields.get('request'))
        if start-_span_last.get(key, 0) < 1_000_000_000:
            yield
            return
        _span_last[key] = start
    emit(event + '.begin', **fields)
    # Existing CPU profiler annotation only; no new device event or synchronization.
    torch = sys.modules.get('torch')
    cm = contextlib.nullcontext()
    try:
        if os.environ.get('KS_PP_DIAG_RANGES') == '1' and torch is not None:
            batch = (getattr(_tls, 'batch', None) or {}).get('batch', 'none')
            cm = torch.profiler.record_function('kspp:' + event + ':' + batch)
    except Exception:
        pass
    try:
        with cm:
            yield
    finally:
        emit(event + '.end', start_ns=start, duration_ns=time.monotonic_ns()-start, **fields)


def resources(obj):
    values = {}
    for k in ('running', 'waiting', 'skipped_waiting', 'batch_queue', 'futures_queue'):
        v = getattr(obj, k, None)
        values[k] = len(v) if v is not None else None
    for k in ('max_num_running_reqs', 'max_num_scheduled_tokens', 'batch_queue_size'):
        values[k] = getattr(obj, k, None)
    # Snapshot existing allocator counter; do not call allocation/eviction methods.
    manager = getattr(obj, 'kv_cache_manager', None)
    pool = getattr(manager, 'block_pool', None)
    free = getattr(pool, 'free_block_queue', None)
    values['kv_free_blocks'] = getattr(free, 'num_free_blocks', None)
    return values


def _wrap(cls, name, factory):
    old = getattr(cls, name, None)
    if old is not None and not getattr(old, '_ks_pp_diag', False):
        wrapped = factory(old)
        wrapped._ks_pp_diag = True
        setattr(cls, name, wrapped)
        emit('hook.method', cls=cls.__name__, method=name,
             source=inspect.getsourcefile(old), line=getattr(getattr(old, '__code__', None), 'co_firstlineno', None))


def _method(event, check_pp=False, make_batch=None, req=False):
    def factory(old):
        @functools.wraps(old)
        def run(self, *args, **kwargs):
            global _last_worker_batch
            if check_pp and not pp2(self):
                return old(self, *args, **kwargs)
            active, previous = getattr(_tls, 'active', False), getattr(_tls, 'batch', None)
            previous_request = getattr(_tls, 'request', None)
            _tls.active = True
            try:
                if make_batch == 'worker' or event == 'cache.retrieve':
                    loaded_identity()
                if make_batch:
                    output = args[0] if args else kwargs['scheduler_output']
                    try:
                        _tls.batch = new_batch(output, make_batch)
                    except Exception as exc:
                        _tls.batch = None
                        emit('observation.gap', operation='batch_shape', error=type(exc).__name__)
                    if make_batch == 'worker':
                        _last_worker_batch = _tls.batch
                        # Distributed groups are already initialized here; access CPU rank metadata only.
                        ps = sys.modules.get('vllm.distributed.parallel_state')
                        if ps is not None:
                            try:
                                _rank.update(pp_rank=ps.get_pp_group().rank_in_group,
                                             tp_rank=ps.get_tp_group().rank_in_group)
                            except Exception as exc:
                                emit('observation.gap', operation='rank', error=type(exc).__name__)
                    emit('batch.map', role=make_batch)
                elif event == 'worker.sample':
                    # Async execution may have advanced beyond the sampled batch.
                    _tls.batch = {'batch_association': 'UNVERIFIED',
                                  'candidate_batch': (_last_worker_batch or {}).get('batch')}
                start = time.monotonic_ns()
                rid = None
                if req and args:
                    rid = str(getattr(args[0], 'request_id', args[0]))
                    _tls.request = rid
                    if event.endswith('.update_state_after_alloc'):
                        external = args[2] if len(args) > 2 else kwargs.get('num_external_tokens')
                        _externals[rid] = external
                        emit('restore.requirement', request=rid, external_tokens=external)
                if event == 'scheduler.kv_notification_consumed' and args:
                    value = args[0]
                    emit('restore.core_notification', finished_recving=sorted(getattr(value, 'finished_recving', None) or []),
                         finished_sending=sorted(getattr(value, 'finished_sending', None) or []),
                         invalid_blocks_count=len(getattr(value, 'invalid_block_ids', None) or []))
                emit(event + '.begin', request=rid, resources=resources(self) if event.startswith(('scheduler.', 'core.')) else None)
                with span(event + '.body'):
                    result = old(self, *args, **kwargs)
                fields = {'request': rid, 'start_ns': start, 'duration_ns': time.monotonic_ns()-start}
                if result is None or isinstance(result, (int, bool, str)):
                    fields['result'] = result
                elif isinstance(result, tuple):
                    fields['result'] = [sorted(x) if isinstance(x, set) else x if x is None or isinstance(x, (int, bool)) else type(x).__name__ for x in result]
                for attr in ('finished_recving', 'finished_sending', 'failed_recving'):
                    value = getattr(result, attr, None)
                    if isinstance(value, (set, list, tuple)):
                        fields[attr] = sorted(value)
                if event == 'scheduler.schedule':
                    fields['shape'] = batch_shape(result)
                    fields['resources'] = resources(self)
                if event == 'adapter.init':
                    fields['nonblocking_lookup_status'] = getattr(self, '_nonblocking_lookup_status', None)
                    self.req_clients = {url: _Client(client, url) for url, client in self.req_clients.items()}
                if event.startswith('core.'):
                    fields['resources'] = resources(self)
                emit(event + '.end', **fields)
                return result
            finally:
                _tls.active, _tls.batch = active, previous
                _tls.request = previous_request
        return run
    return factory


def _poll_method(event):
    def factory(old):
        @functools.wraps(old)
        def call(self, *args, **kwargs):
            rid = str(getattr(args[0], 'request_id', args[0])) if args else str(kwargs.get('request_id', ''))
            start = time.monotonic_ns()
            result = old(self, *args, **kwargs)
            end = time.monotonic_ns()
            key = (event, rid)
            state = _polls.setdefault(key, [0, 0, 0, 0])
            state[0] += 1
            state[1] += end-start
            state[2] = max(state[2], end-start)
            value = result[0] if isinstance(result, tuple) else result
            pending = value is None
            if state[0] == 1 or not pending or end-state[3] >= 1_000_000_000:
                emit(event, request=rid, pending=pending, result=value if isinstance(value, (int, bool, type(None))) else None,
                     start_ns=start, duration_ns=end-start, callbacks=state[0], callback_total_ns=state[1], callback_max_ns=state[2])
                state[3] = end
            if not pending:
                _polls.pop(key, None)
            return result
        return call
    return factory


def transform(source, filename, module):
    """Wrap original statement boundaries without changing values/call ordering."""
    tree = ast.parse(source, filename)
    sites = []

    class Mark(ast.NodeTransformer):
        function = ''

        def visit_FunctionDef(self, node):
            previous = self.function
            self.function = node.name
            node = self.generic_visit(node)
            self.function = previous
            return node

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_If(self, node):
            node = self.generic_visit(node)
            if self.function == 'schedule' and module.endswith('.scheduler') and ast.unparse(node.test) in ('ext_tokens is None','num_external_computed_tokens is None'):
                sites.append({'function':self.function,'line':node.lineno,'event':'scheduler.defer_lookup'})
                call = ast.Expr(value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='_ks_pp_trace',ctx=ast.Load()),attr='point',ctx=ast.Load()),
                    args=[ast.Constant('scheduler.defer_lookup'),ast.Call(func=ast.Name(id='locals',ctx=ast.Load()),args=[],keywords=[])],keywords=[]))
                node.body.insert(0,ast.copy_location(call,node))
            return node

        def marked(self, node):
            text = ast.unparse(node.value)
            event = None
            if module.endswith('endpoint_request_func') and self.function == 'async_request_openai_completions' and text == 'json.loads(chunk)':
                sites.append({'function': self.function, 'line': node.lineno, 'event': 'client.server_identity_and_finish_reason'})
                return [node, ast.copy_location(ast.Expr(value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='_ks_pp_trace', ctx=ast.Load()), attr='client_point', ctx=ast.Load()),
                    args=[ast.Call(func=ast.Name(id='locals', ctx=ast.Load()), args=[], keywords=[])], keywords=[])), node)]
            if self.function == 'execute_model' and module.endswith('.worker'):
                if 'handle.wait(' in text:
                    event = 'pp.previous_send_wait'
                elif 'irecv_tensor_dict(' in text:
                    event = 'pp.activation_recv_submit'
                elif 'isend_tensor_dict(' in text:
                    event = 'pp.activation_send_submit'
                elif 'model_runner.execute_model(' in text:
                    event = 'worker.model_host'
            elif module.endswith('multiproc_executor') and self.function in ('collective_rpc', 'get_response'):
                if '.enqueue(' in text:
                    event = 'core.rpc_enqueue'
                elif '.dequeue(' in text:
                    event = 'core.rpc_reply_read'
            elif self.function == 'schedule' and module.endswith('.scheduler'):
                if '.get_num_new_matched_tokens(' in text:
                    event = 'scheduler.connector_full_callback'
                elif '.get_computed_blocks(' in text:
                    event = 'scheduler.apc_callback'
                elif '.allocate_slots(' in text:
                    event = 'scheduler.allocate'
                elif 'preempted_reqs.append(' in text:
                    event = 'scheduler.preemption'
            elif module.endswith('gpu_worker') and self.function in ('wait_for_comm', '__getitem__'):
                if '.wait(' in text or 'comm_postprocess(' in text or text == 'fn()':
                    event = 'pp.activation_existing_wait'
            elif module.endswith('.model_runner_v1') and ('broadcast' in text or 'recv_tensor' in text or 'send_tensor' in text):
                event = 'feedback.' + self.function
            if event:
                sites.append({'function': self.function, 'line': node.lineno, 'event': event})
                point_node = ast.Expr(value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='_ks_pp_trace', ctx=ast.Load()), attr='point', ctx=ast.Load()),
                    args=[ast.Constant(event), ast.Call(func=ast.Name(id='locals', ctx=ast.Load()), args=[], keywords=[])], keywords=[]))
                wrapped = ast.With(items=[ast.withitem(context_expr=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='_ks_pp_trace', ctx=ast.Load()), attr='span', ctx=ast.Load()),
                    args=[ast.Constant(event), ast.Call(func=ast.Name(id='locals', ctx=ast.Load()), args=[], keywords=[])], keywords=[]))], body=[node, point_node])
                return ast.copy_location(wrapped, node)
            return node

        def visit_Assign(self, node):
            return self.marked(node)

        def visit_Expr(self, node):
            return self.marked(node)

    tree = Mark().visit(tree)
    pos = 1 if tree.body and isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Constant) else 0
    while pos < len(tree.body) and isinstance(tree.body[pos], ast.ImportFrom) and tree.body[pos].module == '__future__':
        pos += 1
    tree.body.insert(pos, ast.Import(names=[ast.alias(name='ks_pp_trace', asname='_ks_pp_trace')]))
    return ast.fix_missing_locations(tree), sites


def install(module):
    name = module.__name__
    emit('module.loaded', module=name, path=getattr(module, '__file__', None))
    for cls in list(vars(module).values()):
        if not isinstance(cls, type) or cls.__module__ != name:
            continue
        if name.endswith('.core') and cls.__name__ == 'EngineCore':
            for method in ('step', 'step_with_batch_queue'):
                _wrap(cls, method, _method('core.' + method, check_pp=True))
        if name.endswith('multiproc_executor'):
            if cls.__name__ == 'MultiprocExecutor':
                _wrap(cls, 'execute_model', _method('core.dispatch', check_pp=True, make_batch='executor'))
            elif cls.__name__ == 'FutureWrapper':
                def init(old):
                    @functools.wraps(old)
                    def call(self, *args, **kwargs):
                        old(self, *args, **kwargs)
                        if getattr(_tls, 'active', False):
                            _future_tags[self] = getattr(_tls, 'batch', None)
                    return call
                _wrap(cls, '__init__', init)
                for method in ('result', '_wait_for_response'):
                    def wait(old, event='core.future.' + method):
                        @functools.wraps(old)
                        def call(self, *args, **kwargs):
                            if self not in _future_tags:
                                return old(self, *args, **kwargs)
                            active, previous = getattr(_tls, 'active', False), getattr(_tls, 'batch', None)
                            _tls.active, _tls.batch = True, _future_tags[self]
                            try:
                                with span(event):
                                    return old(self, *args, **kwargs)
                            finally:
                                _tls.active, _tls.batch = active, previous
                        return call
                    _wrap(cls, method, wait)
        if name.endswith('.worker') and hasattr(cls, 'execute_model'):
            _wrap(cls, 'execute_model', _method('worker.execute', check_pp=True, make_batch='worker'))
            _wrap(cls, 'sample_tokens', _method('worker.sample', check_pp=True))
        if name.endswith(('.scheduler', '.async_scheduler')):
            _wrap(cls, 'schedule', _method('scheduler.schedule', check_pp=True))
            _wrap(cls, '_update_from_kv_xfer_finished', _method('scheduler.kv_notification_consumed', check_pp=True))
            _wrap(cls, '_update_requests_with_invalid_blocks', _method('scheduler.recompute_invalid_blocks',check_pp=True))
        if name.endswith(('.lmcache_mp_connector', '.multi_connector')):
            for method in ('get_num_new_matched_tokens', 'update_state_after_alloc', 'start_load_kv', 'get_finished', 'get_transfer_results'):
                event = 'connector.' + cls.__name__ + '.' + method
                _wrap(cls, method, _poll_method(event) if method == 'get_num_new_matched_tokens' else _method(event, req=method == 'update_state_after_alloc'))
        if name.endswith('.vllm_multi_process_adapter'):
            if cls.__name__ == 'LMCacheMPSchedulerAdapter':
                _wrap(cls, '__init__', _method('adapter.init'))
                _wrap(cls, 'check_lookup_result', _poll_method('lookup.callback'))
            elif cls.__name__ == 'LMCacheMPWorkerAdapter':
                _wrap(cls, 'submit_retrieve_request', _method('restore.submit', req=True))
                _wrap(cls, 'get_finished', _method('restore.worker_finished'))
        if name.endswith('.futures') and cls.__name__ in ('MessagingFuture', 'DeviceMessagingFuture'):
            # Does not call query/result on its own; observes existing operations.
            for method in ('set_result', 'result', 'wait_on_stream'):
                def future(old, event='lmfuture.' + cls.__name__ + '.' + method):
                    @functools.wraps(old)
                    def call(self, *args, **kwargs):
                        tag = _rpc_tags.get(self)
                        if tag is None:
                            return old(self, *args, **kwargs)
                        start = time.monotonic_ns()
                        emit(event + '.begin', **tag)
                        try:
                            return old(self, *args, **kwargs)
                        finally:
                            emit(event + '.end', start_ns=start, duration_ns=time.monotonic_ns()-start, **tag)
                    return call
                _wrap(cls, method, future)
        if name.endswith('.lmcache_driven_transfer'):
            _wrap(cls, 'retrieve', _method('cache.retrieve', req=True))
    if name.endswith('endpoint_request_func'):
        old = getattr(module, 'async_request_openai_completions', None)
        if old is not None:
            @functools.wraps(old)
            async def client(request_func_input, *args, **kwargs):
                rid = getattr(request_func_input, 'request_id', None)
                begin = time.monotonic_ns()
                emit('client.send', request=rid, output_len=getattr(request_func_input, 'output_len', None))
                try:
                    result = await old(request_func_input, *args, **kwargs)
                    emit('client.complete', request=rid, send_ns=begin,
                         completed_ns=time.monotonic_ns(), success=result.success,
                         ttft_s=result.ttft, latency_s=result.latency,
                         output_tokens=getattr(result, 'output_tokens', None), error=getattr(result, 'error', None))
                    return result
                except BaseException as exc:
                    emit('client.incomplete', request=rid, error=type(exc).__name__)
                    raise
            module.async_request_openai_completions = client
    if name.endswith('.native_completion'):
        # Reuse the old observer's existing stream-callback boundary, retrieve only.
        def signature(payload):
            import msgspec
            return hashlib.sha256(msgspec.msgpack.encode(payload)).hexdigest()

        def submit(old):
            @functools.wraps(old)
            def call(stream, kind, payload):
                if kind == 'release_read_borrow':
                    try:
                        emit('restore.stream_callback_submitted', signature=signature(payload), request=getattr(_tls, 'request', None))
                    except Exception:
                        emit('observation.gap', operation='callback_signature')
                return old(stream, kind, payload)
            return call

        def register(old):
            @functools.wraps(old)
            def call(self, kind, handler, payload_type):
                if kind != 'release_read_borrow':
                    return old(self, kind, handler, payload_type)
                @functools.wraps(handler)
                def observed(payload):
                    result = handler(payload)
                    try:
                        emit('restore.stream_callback_observed', signature=signature(payload))
                    except Exception:
                        emit('observation.gap', operation='callback_signature')
                    return result
                return old(self, kind, observed, payload_type)
            return call
        _wrap(module, 'submit_callback_to_stream', submit)
        _wrap(module.DeviceHostFuncDispatcher, 'register', register)


class _Client:
    """Reuse old RPC proxy logic, without the old additional Future.query()."""
    def __init__(self, client, url):
        self.client, self.url = client, url

    def __getattr__(self, name):
        fn = getattr(self.client, name)
        if name not in ('lookup', 'query_prefetch_status', 'retrieve'):
            return fn

        @functools.wraps(fn)
        def call(*args, **kwargs):
            rid = str(args[0]) if args else str(kwargs.get('request_id', ''))
            emit('rpc.submit', rpc=name, request=rid, server=self.url)
            result = fn(*args, **kwargs)
            # If set_result raced before tagging, absence of a reply event is a gap,
            # not evidence that the reply arrived only at result().
            tag = {'rpc': name, 'request': rid, 'server': self.url, 'future_id': id(result)}
            _rpc_tags[result] = tag
            emit('rpc.tagged', **tag)
            return result
        return call
