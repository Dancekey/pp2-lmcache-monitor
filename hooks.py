"""Opt-in host observations. Never drive business futures, queues or devices.

Native payload SHA256 + transfer_key association follows ks_wait_trace v3.
The old extra query(), synchronous JSONL writes and object ledger are omitted.
"""
import atexit
import contextvars
import functools
import hashlib
import importlib.abc
import inspect
import itertools
import json
import os
from pathlib import Path
import sys
import threading
import time
import uuid
import weakref
from collections import Counter, deque

CONTEXT = contextvars.ContextVar('ks_pp_observation', default=None)
KINDS = {'finish_write_by_owner', 'finish_read_by_owner',
         'finish_read_prefetched', 'release_read_borrow'}
TARGETS = {
    'lmcache.integration.vllm.vllm_multi_process_adapter',
    'lmcache.integration.vllm.lmcache_mp_connector',
    'lmcache.v1.multiprocess.modules.lookup',
    'lmcache.v1.multiprocess.modules.lmcache_driven_transfer',
    'lmcache.v1.multiprocess.futures',
    'lmcache.v1.distributed.l1_manager',
    'lmcache.v1.mp_observability.event_bus',
    'lmcache.v1.multiprocess.native_completion',
    'lmcache.v1.multiprocess.transport.zmq_impl.mq',
}


class Sink:
    """One writer/process, bounded queue and bytes. Inference never writes files.

    A shared control file arms all already-wired processes for a short window.
    Stop/expiry disable observations, never model/cache processes.
    """
    def __init__(self, directory, *, control=None, capacity=4096,
                 max_bytes=16 * 1024 * 1024, start_writer=True):
        self.directory = Path(directory)
        self.control = Path(control) if control else None
        self.capacity, self.max_bytes = capacity, max_bytes
        self.pending = deque()
        self.lock = threading.Lock()
        self.stats = Counter()
        self.active = False
        self.sample_rate, self.seed = .01, 'ks-pp-v1'
        self.node = os.environ.get('KS_PP_MONITOR_NODE', 'unknown')
        self.instance = uuid.uuid4().hex
        self.pid = os.getpid()
        self.run_id = 'unarmed'
        self.deadline = 0
        self.sequence = itertools.count(1)
        self.boot_id = 'unknown'
        try:
            self.boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        except OSError:
            pass
        self.closed = threading.Event()
        self.thread = None
        if start_writer:
            self.thread = threading.Thread(target=self._writer, daemon=True,
                                           name='ks-pp-observation-writer')
            self.thread.start()

    def selected(self, request):
        if request is None:
            return False
        digest = hashlib.blake2b((self.seed + '\0' + str(request)).encode(), digest_size=8).digest()
        return int.from_bytes(digest, 'big') / 2**64 < self.sample_rate

    def emit(self, event, **fields):
        if not self.active:
            return
        try:
            row = dict.fromkeys(('request_id', 'generation', 'batch_id', 'pp_stage', 'tp_rank',
                                 'operation', 'operation_id', 'span_id', 'parent_span_id',
                                 'outcome', 'token_start', 'token_end', 'object_group', 'bytes_kind', 'bytes'))
            row.update(schema_version=1, run_id=self.run_id, node=self.node,
                       boot_id=self.boot_id, process_instance=self.instance,
                       pid=self.pid, thread_id=threading.get_native_id(),
                       wall_ns=time.time_ns(), mono_ns=time.monotonic_ns(),
                       sequence=next(self.sequence), event=event,
                       sample_rate=self.sample_rate, **fields)
            if not self.lock.acquire(blocking=False):
                self.stats['dropped_contention'] += 1
                return
            try:
                if len(self.pending) >= self.capacity:
                    self.stats['dropped_buffer'] += 1
                else:
                    self.pending.append(row)
                    self.stats['accepted'] += 1
                    self.stats['max_buffer'] = max(self.stats['max_buffer'], len(self.pending))
            finally:
                self.lock.release()
        except Exception:
            self.stats['observation_errors'] += 1

    def _control(self):
        if self.control is None:
            return
        try:
            config = json.loads(self.control.read_text(encoding='utf-8'))
            deadline = float(config.get('deadline_wall', 0))
            self.sample_rate = min(1., max(0., float(config.get('sample_rate', .01))))
            self.seed = str(config.get('seed', 'ks-pp-v1'))
            self.run_id = str(config.get('run_id', 'unknown'))
            self.deadline = deadline
            self.active = config.get('enabled') is True and time.time() < deadline
        except Exception:
            self.active = False
            self.stats['control_errors'] += 1

    def flush(self):
        with self.lock:
            rows = list(self.pending)
            self.pending.clear()
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            with (self.directory / ('trace-%s-%s.jsonl' % (self.pid, self.instance))).open('ab') as f:
                for row in rows:
                    data = (json.dumps(row, separators=(',', ':')) + '\n').encode()
                    if self.stats['written_bytes'] + len(data) > self.max_bytes:
                        self.stats['dropped_budget'] += 1
                        continue
                    f.write(data)
                    self.stats['written_bytes'] += len(data)
                    self.stats['written_events'] += 1
        except Exception:
            self.stats['write_errors'] += 1
            self.stats['dropped_write'] += len(rows)
        self.status()

    def status(self):
        try:
            path = self.directory / ('status-%s-%s.json' % (self.pid, self.instance))
            data = dict(self.stats, pid=self.pid, process_instance=self.instance,
                        node=self.node, boot_id=self.boot_id, run_id=self.run_id,
                        active=self.active, sample_rate=self.sample_rate,
                        deadline_wall=self.deadline, capacity=self.capacity,
                        max_bytes=self.max_bytes, at=time.time())
            tmp = path.with_suffix('.tmp')
            tmp.write_text(json.dumps(data, indent=2), encoding='utf-8')
            tmp.replace(path)
        except Exception:
            self.stats['status_errors'] += 1
            if self.stats['status_errors'] == 1:
                try:
                    print('KS_PP_MONITOR: status output unavailable; observation completeness UNKNOWN', file=sys.stderr)
                except Exception:
                    pass

    def _writer(self):
        while not self.closed.is_set():
            self._control()
            self.flush()
            self.closed.wait(.5)
        self.active = False
        self.flush()

    def close(self):
        self.active = False
        self.closed.set()
        if self.thread:
            self.thread.join(timeout=1)
        else:
            self.flush()


def request_id(value):
    if isinstance(value, str):
        return value
    # Only approved identifier fields, never repr(request), prompt or KV keys.
    rid = getattr(value, 'request_id', None)
    return rid if isinstance(rid, str) else None


def worker_identity(obj):
    """Use already-validated registration fields, never KV rank as TP rank."""
    data = getattr(obj, '__dict__', {})
    info = data.get('_pp_registration_info')
    info_data = getattr(info, '__dict__', {})
    locator = getattr(info_data.get('locator'), '__dict__', {})
    stage, rank = locator.get('pp_rank'), info_data.get('tp_rank')
    if type(stage) is int and type(rank) is int:
        return dict(pp_stage=stage, tp_rank=rank,
                    global_rank=info_data.get('global_rank'), identity_source='_pp_registration_info')
    return {}


class Observer:
    def __init__(self, sink):
        self.sink = sink
        self.patches = []
        self.queue_patches = []
        self.ids = itertools.count(1)
        self.futures = weakref.WeakKeyDictionary()
        self.capabilities = []
        self.native_pending = {}
        self.native_run = None
        self.native_invalid = False
        self.native_outstanding = 0
        self.native_accounting_unknown = False
        self.native_lock = threading.Lock()

    def emit(self, event, **fields):
        try:
            self.sink.emit(event, **fields)
        except Exception:
            # Never replace a business return/exception with observation failure.
            try:
                self.sink.stats['observation_errors'] += 1
            except Exception:
                pass

    def _patch(self, owner, name, replacement):
        old = getattr(owner, name, None)
        if old is None or getattr(old, '_ks_pp_wrapped', False):
            return
        replacement._ks_pp_wrapped = True
        setattr(owner, name, replacement)
        self.patches.append((owner, name, old, replacement))
        self.capabilities.append(dict(owner=getattr(owner, '__name__', ''), method=name,
                                      signature=str(inspect.signature(old))))

    def compatible(self, old, names):
        signature = inspect.signature(old)
        parameters = list(signature.parameters.values())
        okay = ([p.name for p in parameters] == names and
                all(p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD and p.default is inspect.Parameter.empty
                    for p in parameters))
        if not okay:
            self.capabilities.append(dict(method=getattr(old, '__name__', ''), signature=str(signature),
                                          supported=False, reason='specialized_signature_mismatch'))
        return okay

    def wrap(self, owner, name, operation, *, request_param=None, rpc=False):
        old = getattr(owner, name, None)
        if old is None or not callable(old) or getattr(old, '_ks_pp_wrapped', False):
            return
        signature = inspect.signature(old)
        if request_param and request_param not in signature.parameters:
            self.capabilities.append(dict(owner=getattr(owner, '__name__', ''), method=name,
                                          supported=False, reason='request_parameter_mismatch', signature=str(signature)))
            return

        def begin(args, kwargs):
            if not self.sink.active:
                return None
            previous = CONTEXT.get()
            fields = dict(previous or {})
            if request_param:
                bound = signature.bind_partial(*args, **kwargs)
                rid = request_id(bound.arguments.get(request_param))
                if rid is not None:
                    fields['request_id'] = rid
            if not previous and not self.sink.selected(fields.get('request_id')):
                # Unattributed batch calls remain NA, never sample batches as requests.
                return None
            span = str(next(self.ids))
            fields.update(parent_span_id=fields.get('span_id'), span_id=span,
                          operation=operation, operation_id=span,
                          generation=fields.get('generation'), pp_stage=fields.get('pp_stage'),
                          tp_rank=fields.get('tp_rank'))
            if args:
                fields.update(worker_identity(args[0]))
            if rpc:
                bound = signature.bind_partial(*args, **kwargs)
                op = bound.arguments.get('operation')
                if isinstance(op, str):
                    fields['operation'] = op.upper()
                fields['client_instance'] = str(id(args[0]))
                fields['wire_id'] = None
            token = CONTEXT.set(fields)
            self.emit('span.begin', **fields)
            return token, fields

        def safe_begin(args, kwargs):
            try:
                return begin(args, kwargs)
            except Exception:
                self.emit('observation.error', boundary=name)
                return None

        def end(state, outcome, exception=None):
            if state:
                token, fields = state
                try:
                    self.emit('span.end', **fields, outcome=outcome,
                              exception_type=exception)
                finally:
                    CONTEXT.reset(token)

        if inspect.iscoroutinefunction(old):
            @functools.wraps(old)
            async def wrapped(*args, **kwargs):
                state = safe_begin(args, kwargs)
                try:
                    result = await old(*args, **kwargs)
                except BaseException as exc:
                    if state is None and self.sink.active:
                        self.emit('business.exception.unsampled', operation=operation, exception_type=type(exc).__name__)
                    end(state, 'exception', type(exc).__name__)
                    raise
                end(state, 'returned')
                return result
        else:
            @functools.wraps(old)
            def wrapped(*args, **kwargs):
                state = safe_begin(args, kwargs)
                try:
                    result = old(*args, **kwargs)
                except BaseException as exc:
                    if state is None and self.sink.active:
                        self.emit('business.exception.unsampled', operation=operation, exception_type=type(exc).__name__)
                    end(state, 'exception', type(exc).__name__)
                    raise
                # Result metadata only reads containers already returned by business.
                if state:
                    try:
                        self._result(name, result, state[1])
                    except Exception:
                        self.emit('observation.error', boundary=name)
                end(state, 'returned')
                return result
        self._patch(owner, name, wrapped)
        self.capabilities.append(dict(owner=getattr(owner, '__name__', ''), method=name,
                                      signature=str(signature), operation=operation))

    def _result(self, name, value, fields):
        if name in ('retrieve', 'store_with_chunk_mask') and isinstance(value, tuple) and len(value) >= 2 and type(value[1]) is bool:
            self.emit('operation.result', **fields, success=value[1])
        if name == 'next_transfer_key':
            CONTEXT.get()['transfer_key'] = value
        if name in ('reserve_write', 'unsafe_read', 'reserve_read') and isinstance(value, dict):
            counts = Counter()
            for row in value.values():
                if isinstance(row, tuple) and row:
                    counts[getattr(row[0], 'name', 'UNKNOWN')] += 1
            self.emit('l1.outcomes', **fields, result_counts=dict(counts))
        elif name == 'borrow_read':
            self.emit('l1.borrow', **fields, success=value is not None)
        elif name in ('check_lookup_result', 'get_num_new_matched_tokens'):
            # No unique-token savings claim: these are per-visit observations.
            scalar = value if type(value) in (int, bool, type(None)) else None
            self.emit('lookup.visit', **fields, result=scalar)

    def _future(self, cls):
        old_init = cls.__dict__.get('__init__')
        if old_init:
            @functools.wraps(old_init)
            def init(obj, *args, **kwargs):
                result = old_init(obj, *args, **kwargs)
                try:
                    context = CONTEXT.get()
                    if context:
                        tag = dict(context, future_id=str(next(self.ids)), future_type=type(obj).__name__)
                        raw = vars(obj).get('raw_future_')
                        if raw is not None:
                            raw_tag = self.futures.get(raw)
                            if raw_tag:
                                tag['raw_future_id'] = raw_tag['future_id']
                        self.futures[obj] = tag
                except Exception:
                    self.emit('observation.error', boundary='future.init')
                return result
            self._patch(cls, '__init__', init)
        for name in ('set_result', 'set_exception', 'query', 'result', 'wait', 'wait_on_stream'):
            old = cls.__dict__.get(name)
            if old is None or inspect.iscoroutinefunction(old):
                continue
            def factory(old, name):
                @functools.wraps(old)
                def observed(obj, *args, **kwargs):
                    tag = None
                    try:
                        tag = self.futures.get(obj)
                        if tag and self.sink.active:
                            tag = dict(tag)
                            tag['future_call_id'] = str(next(self.ids))
                            self.emit('future.%s.begin' % name, **tag)
                    except Exception:
                        pass
                    try:
                        value = old(obj, *args, **kwargs)
                    except BaseException as exc:
                        if tag:
                            self.emit('future.%s' % name, **tag, outcome='exception', exception_type=type(exc).__name__)
                        raise
                    if tag:
                        self.emit('future.%s' % name, **tag, outcome='returned',
                                  ready=value if name in ('query', 'wait') and type(value) is bool else None,
                                  blocking='unknown')
                    return value
                return observed
            self._patch(cls, name, factory(old, name))

    def _native(self, module):
        # Observe callbacks already registered by production. No new callbacks.
        def signature(payload):
            codec = sys.modules.get('msgspec')
            return hashlib.sha256(codec.msgpack.encode(payload)).hexdigest()
        def pending():
            run_id = getattr(self.sink, 'run_id', None)
            if self.native_run != run_id:
                self.native_pending.clear()
                self.native_run = run_id
                self.native_invalid = self.native_accounting_unknown or self.native_outstanding > 0
                if self.native_invalid:
                    self.emit('observation.error', boundary='native_prewindow_or_unknown_inflight')
            elif self.native_invalid and not self.native_accounting_unknown and self.native_outstanding == 0:
                self.native_pending.clear()
                self.native_invalid = False
                self.emit('capability.native_association_resumed', reason='naturally_no_inflight_callbacks')
            return self.native_pending
        def track(kind, payload, submitting, context=None):
            # Maintain only an integer while disarmed. This detects pre-window
            # callbacks without hashing payloads or querying devices while off.
            if not self.native_lock.acquire(blocking=False):
                self.native_accounting_unknown = self.native_invalid = True
                self.emit('observation.error', boundary='native_identity_contention')
                return None, False, None
            try:
                sig, selected = None, False
                run_id = getattr(self.sink, 'run_id', None)
                rows = pending() if self.sink.active else self.native_pending
                if submitting:
                    self.native_outstanding += 1
                else:
                    if self.native_outstanding == 0:
                        self.native_accounting_unknown = self.native_invalid = True
                    self.native_outstanding = max(0, self.native_outstanding-1)
                if self.sink.active:
                    sig = signature(payload)
                    key = kind, sig
                    if submitting:
                        if key in rows:
                            rows[key]['count'] += 1
                            rows[key]['selected'] = False
                            self.emit('device.callback.ambiguous', kind=kind, completion_signature=sig)
                        elif len(rows) < 4096:
                            rows[key] = dict(count=1, selected=context is not None)
                        else:
                            rows.clear()
                            self.native_invalid = True
                            self.emit('observation.error', boundary='callback_identity_capacity')
                        if context:
                            self.emit('device.callback.submitted', **context, kind=kind,
                                      completion_signature=sig)
                    else:
                        row = rows.get(key)
                        if row:
                            selected = row['selected'] and row['count'] == 1 and not self.native_invalid
                            if row['count'] > 1:
                                self.emit('device.callback.ambiguous', kind=kind, completion_signature=sig)
                            row['count'] -= 1
                            if row['count'] == 0:
                                rows.pop(key, None)
                return sig, selected, run_id
            except Exception:
                self.native_accounting_unknown = self.native_invalid = True
                self.emit('observation.error', boundary='callback.identity')
                return None, False, None
            finally:
                self.native_lock.release()
        cls = getattr(module, 'DeviceHostFuncDispatcher', None)
        if cls and hasattr(cls, 'register') and self.compatible(cls.register, ['self', 'kind', 'handler', 'payload_type']):
            register_old = cls.register
            @functools.wraps(register_old)
            def register(obj, kind, handler, payload_type):
                if kind not in KINDS or inspect.iscoroutinefunction(handler):
                    return register_old(obj, kind, handler, payload_type)
                @functools.wraps(handler)
                def observed(payload):
                    # Payload digest only; never output payload/KV keys.
                    sig, selected, observed_run = track(kind, payload, False)
                    try:
                        result = handler(payload)
                    except BaseException:
                        if sig and selected and observed_run == getattr(self.sink, 'run_id', None):
                            self.emit('device.callback.failed', kind=kind, completion_signature=sig)
                        raise
                    if sig and selected and observed_run == getattr(self.sink, 'run_id', None):
                        self.emit('device.callback.completed', kind=kind, completion_signature=sig)
                    return result
                return register_old(obj, kind, observed, payload_type)
            self._patch(cls, 'register', register)
        old = getattr(module, 'submit_callback_to_stream', None)
        if old and self.compatible(old, ['stream', 'kind', 'payload']):
            @functools.wraps(old)
            def submit(stream, kind, payload):
                context = CONTEXT.get()
                if kind in KINDS:
                    track(kind, payload, True, context)
                return old(stream, kind, payload)
            self._patch(module, 'submit_callback_to_stream', submit)

    def _mq(self, cls):
        self.wrap(cls, 'submit_request', 'RPC', rpc=True)
        wrapped_request = getattr(cls, 'WrappedRequest', None)
        if wrapped_request and hasattr(wrapped_request, '__init__'):
            original = wrapped_request.__init__
            @functools.wraps(original)
            def create(obj, *args, **kwargs):
                result = original(obj, *args, **kwargs)
                try:
                    tag = self.futures.get(obj.future)
                    if tag:
                        # Established before the real queue put: fast replies retain identity.
                        uid = obj.request_uid
                        tag['wire_id'] = uid if type(uid) in (int, str) else None
                        self.emit('rpc.identity', **tag)
                except Exception:
                    self.emit('observation.error', boundary='wire.identity')
                return result
            self._patch(wrapped_request, '__init__', create)
        old = cls.__init__
        @functools.wraps(old)
        def init(obj, *args, **kwargs):
            result = old(obj, *args, **kwargs)
            try:
                q = obj.input_queue
                # Observe only naturally invoked queue operations. Never inspect/drain it.
                for name in ('put', 'get_nowait'):
                    method = getattr(q, name)
                    def factory(method, name):
                        @functools.wraps(method)
                        def observed(*a, **kw):
                            if name == 'put':
                                value = a[0] if a else kw.get('item')
                                try:
                                    tag = self.futures.get(getattr(value, 'future', None))
                                except Exception:
                                    tag = None
                                if tag:
                                    self.emit('rpc.queue.put.begin', **tag)
                                answer = method(*a, **kw)
                                if tag:
                                    self.emit('rpc.queue.put.return', **tag)
                                return answer
                            answer = method(*a, **kw)
                            try:
                                tag = self.futures.get(getattr(answer, 'future', None))
                                if tag:
                                    self.emit('rpc.queue.dequeued', **tag)
                            except Exception:
                                self.emit('observation.error', boundary='queue.dequeue')
                            return answer
                        return observed
                    replacement = factory(method, name)
                    # Weak registry avoids retaining business queues or their futures.
                    self.queue_patches.append((weakref.ref(q), name, method.__func__, weakref.ref(replacement)))
                    setattr(q, name, replacement)
            except Exception:
                self.emit('observation.error', boundary='queue.install')
            return result
        self._patch(cls, '__init__', init)

    def _scheduler(self, cls):
        old = getattr(cls, 'build_connector_meta', None)
        if old and self.compatible(old, ['self', 'scheduler_output']):
            @functools.wraps(old)
            def build(obj, scheduler_output, *args, **kwargs):
                batch = None
                started = None
                try:
                    if self.sink.active:
                        tokens = getattr(scheduler_output, 'num_scheduled_tokens', None)
                        if isinstance(tokens, dict):
                            members = {rid: n for rid, n in tokens.items() if isinstance(rid, str)
                                       and type(n) is int and self.sink.selected(rid)}
                            if members:
                                batch = dict(batch_id=str(next(self.ids)), members=members,
                                             total_members=len(tokens), operation='PP.build_meta',
                                             timing_kind='shared_batch_host_elapsed_not_apportioned')
                                started = time.monotonic_ns()
                                self.emit('scheduler.batch.begin', **batch)
                except Exception:
                    self.emit('observation.error', boundary='batch.identity')
                try:
                    result = old(obj, scheduler_output, *args, **kwargs)
                except BaseException:
                    if batch:
                        self.emit('scheduler.batch.end', **batch, elapsed_ns=time.monotonic_ns()-started, outcome='exception')
                    raise
                if batch:
                    self.emit('scheduler.batch.end', **batch, elapsed_ns=time.monotonic_ns()-started, outcome='returned')
                return result
            self._patch(cls, 'build_connector_meta', build)

    def install(self, module):
        name = module.__name__
        before = len(self.capabilities)
        table = {
            '.lookup': [('LookupModule', 'lookup', 'LOOKUP', 'key'),
                        ('LookupModule', 'query_prefetch_status', 'STATUS', 'request_id')],
            '.lmcache_driven_transfer': [('LMCacheDrivenTransferModule', 'retrieve', 'RETRIEVE', 'key'),
                                        ('LMCacheDrivenTransferModule', 'store_with_chunk_mask', 'STORE', 'key')],
            '.l1_manager': [('L1Manager', n, 'L1.' + n, None) for n in
                            ('reserve_write', 'unsafe_read', 'reserve_read', 'borrow_read')],
            '.lmcache_mp_connector': [('LMCacheMPConnector', 'get_num_new_matched_tokens', 'LOOKUP.callback', 'request'),
                                     ('LMCacheMPConnector', 'update_state_after_alloc', 'PP.allocate', 'request'),
                                     ('LMCacheMPConnector', 'start_load_kv', 'RETRIEVE.submit', None),
                                     ('LMCacheMPConnector', 'wait_for_save', 'STORE.submit', None)],
            '.vllm_multi_process_adapter': [('LMCacheMPSchedulerAdapter', 'check_lookup_result', 'STATUS.consume', 'request_id'),
                                           ('LMCacheMPWorkerAdapter', 'submit_store_request', 'STORE.submit', 'request_id'),
                                           ('LMCacheMPWorkerAdapter', 'submit_retrieve_request', 'RETRIEVE.submit', 'request_id')],
        }
        for suffix, specs in table.items():
            if name.endswith(suffix):
                for cls_name, method, operation, param in specs:
                    cls = getattr(module, cls_name, None)
                    if cls:
                        self.wrap(cls, method, operation, request_param=param)
        if name.endswith('.futures'):
            seen = set()
            for cls_name in ('MessagingFuture', 'DeviceMessagingFuture', 'CUDAMessagingFuture'):
                cls = getattr(module, cls_name, None)
                if cls and cls not in seen:
                    seen.add(cls)
                    self._future(cls)
        elif name.endswith('.mq'):
            cls = getattr(module, 'MessageQueueClient', None)
            if cls:
                self._mq(cls)
        elif name.endswith('.lmcache_mp_connector'):
            cls = getattr(module, 'LMCacheMPConnector', None)
            if cls:
                self._scheduler(cls)
        elif name.endswith('.native_completion'):
            self._native(module)
        elif name.endswith('.lmcache_driven_transfer'):
            old = getattr(module, 'next_transfer_key', None)
            if old:
                @functools.wraps(old)
                def next_key(*args, **kwargs):
                    value = old(*args, **kwargs)
                    context = CONTEXT.get()
                    if context:
                        context['transfer_key'] = value
                    return value
                self._patch(module, 'next_transfer_key', next_key)
        elif name.endswith('.vllm_multi_process_adapter'):
            cls = getattr(module, 'LMCacheMPWorkerAdapter', None)
            if cls:
                for method in ('get_finished', 'get_finished_with_lazy_offload'):
                    old = getattr(cls, method, None)
                    if old is None:
                        continue
                    def factory(old):
                        @functools.wraps(old)
                        def finished(obj, *args, **kwargs):
                            value = old(obj, *args, **kwargs)
                            try:
                                if self.sink.active and isinstance(value, tuple) and len(value) == 2:
                                    for operation, ids in zip(('STORE', 'RETRIEVE'), value):
                                        if isinstance(ids, set):
                                            for rid in ids:
                                                if isinstance(rid, str) and self.sink.selected(rid):
                                                    self.emit('worker.completion_reported', request_id=rid,
                                                              generation=None, operation=operation,
                                                              outcome='reported_not_proven_success', **worker_identity(obj))
                            except Exception:
                                self.emit('observation.error', boundary='get_finished')
                            return value
                        return finished
                    self._patch(cls, method, factory(old))
        elif name.endswith('.event_bus'):
            cls = getattr(module, 'EventBus', None)
            old = getattr(cls, 'publish_on_stream', None) if cls else None
            if old and self.compatible(old, ['self', 'stream', 'event']):
                @functools.wraps(old)
                def publish(obj, stream, event):
                    context = CONTEXT.get()
                    if context and self.sink.active:
                        try:
                            kind = getattr(event.event_type, 'value', None)
                            if kind in ('mp.store.end', 'mp.retrieve.end'):
                                allowed = ('total_bytes', 'transfer_key', 'num_tokens', 'stored_count',
                                           'retrieved_count', 'success')
                                meta = {k: v for k, v in event.metadata.items()
                                        if k in allowed and type(v) in (int, float, bool, str, type(None))}
                                self.emit('transfer.enqueued_metadata', **context,
                                          transfer_kind=kind, metadata=meta, stream_id=str(id(stream)))
                        except Exception:
                            self.emit('observation.error', boundary='event.metadata')
                    return old(obj, stream, event)
                self._patch(cls, 'publish_on_stream', publish)
        record = dict(module=name, source=getattr(module, '__file__', None),
                      hooks=self.capabilities[before:])
        self.capabilities.append(record)
        self.emit('module.instrumented', **record)

    def uninstall(self):
        for ref, name, original, replacement_ref in reversed(self.queue_patches):
            owner = ref()
            if owner is not None and getattr(owner, name, None) is replacement_ref():
                setattr(owner, name, original.__get__(owner, type(owner)))
        self.queue_patches.clear()
        for owner, name, old, replacement in reversed(self.patches):
            if getattr(owner, name, None) is replacement:
                setattr(owner, name, old)
        self.patches.clear()


class Finder(importlib.abc.MetaPathFinder):
    """Delegate to existing finders, retaining private source mappings."""
    def __init__(self, observer):
        self.observer = observer
    def find_spec(self, fullname, path=None, target=None):
        if fullname not in TARGETS:
            return None
        after_self = False
        for finder in list(sys.meta_path):
            if finder is self:
                after_self = True
                continue
            if not after_self or not hasattr(finder, 'find_spec'):
                continue
            spec = finder.find_spec(fullname, path, target)
            if spec is None:
                continue
            if spec.loader and hasattr(spec.loader, 'exec_module'):
                loader, observer = spec.loader, self.observer
                class Loader:
                    def create_module(self, spec):
                        return loader.create_module(spec) if hasattr(loader, 'create_module') else None
                    def exec_module(self, module):
                        loader.exec_module(module)
                        try:
                            observer.install(module)
                        except Exception:
                            observer.emit('observation.error', boundary='install', module=fullname)
                    def __getattr__(self, name):
                        return getattr(loader, name)
                spec.loader = Loader()
            return spec
        return None


_runtime = None


def bootstrap():
    """Called explicitly from existing sitecustomize during a planned restart."""
    global _runtime
    if _runtime is not None or not os.environ.get('KS_PP_MONITOR_DIR'):
        return _runtime
    if any(name in sys.modules for name in TARGETS):
        # Native handlers/from-import aliases could already have escaped wrapping.
        raise RuntimeError('PP monitor must be wired before LMCache imports; late injection unsupported')
    sink = Sink(os.environ['KS_PP_MONITOR_DIR'], control=os.environ.get('KS_PP_MONITOR_CONTROL'),
                capacity=int(os.environ.get('KS_PP_MONITOR_BUFFER', '4096')),
                max_bytes=int(os.environ.get('KS_PP_MONITOR_MAX_BYTES', '16777216')))
    observer = Observer(sink)
    finder = Finder(observer)
    sys.meta_path.insert(0, finder)
    _runtime = sink, observer, finder
    # Capabilities are maintained outside event budget, even while disarmed.
    def attach_status(target_sink):
        original_status = target_sink.status
        def status():
            original_status()
            try:
                path = target_sink.directory / ('capabilities-%s-%s.json' % (target_sink.pid, target_sink.instance))
                tmp = path.with_suffix('.tmp')
                tmp.write_text(json.dumps(observer.capabilities, indent=2), encoding='utf-8')
                tmp.replace(path)
            except Exception:
                target_sink.stats['capability_write_errors'] += 1
        target_sink.status = status
    attach_status(sink)
    atexit.register(sink.close)
    # Forked workers need a fresh writer/identity; inherited locks are discarded.
    if hasattr(os, 'register_at_fork'):
        def child():
            global _runtime
            atexit.unregister(sink.close)
            new = Sink(sink.directory, control=sink.control, capacity=sink.capacity, max_bytes=sink.max_bytes)
            observer.sink = new
            observer.futures = weakref.WeakKeyDictionary()
            observer.native_pending.clear()
            observer.native_run = None
            observer.native_invalid = False
            observer.native_accounting_unknown = observer.native_accounting_unknown or observer.native_outstanding > 0
            observer.native_outstanding = 0
            observer.native_lock = threading.Lock()
            CONTEXT.set(None)
            attach_status(new)
            _runtime = new, observer, finder
            atexit.register(new.close)
        os.register_at_fork(after_in_child=child)
    return _runtime
