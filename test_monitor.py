"""CPU contract checks; synthetic cases are not customer measurements."""
import asyncio
import ast
import gc
import json
from pathlib import Path
from collections import defaultdict, deque
import tempfile
import threading
import time
import types
import typing
import unittest
import weakref

import analyze
import capture
import hooks


class MemorySink:
    active = True
    sample_rate = 1.0
    seed = 'test'
    def __init__(self):
        self.rows = []
    def emit(self, event, **fields):
        self.rows.append(dict(event=event, mono_ns=time.monotonic_ns(), **fields))
    def selected(self, request):
        return True


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.sink = MemorySink()
        self.obs = hooks.Observer(self.sink)
    def tearDown(self):
        self.obs.uninstall()

    def test_sync_arguments_identity_exception_and_disabled(self):
        calls = []
        answer = object()
        error = ValueError('private text must not be logged')
        class Business:
            def action(self, value, *, fail=False):
                calls.append((value, fail))
                if fail:
                    raise error
                return answer
        original = Business.action
        self.obs.wrap(Business, 'action', 'STORE', request_param='value')
        value = Business()
        for active in (False, True):
            self.sink.active = active
            self.assertIs(value.action('r'), answer)
            with self.assertRaises(ValueError) as caught:
                value.action('r', fail=True)
            self.assertIs(caught.exception, error)
        self.assertEqual(calls, [('r', False), ('r', True)] * 2)
        self.assertNotIn('private text', json.dumps(self.sink.rows))
        self.obs.uninstall()
        self.assertIs(Business.action, original)

    def test_async_nested_and_cancellation(self):
        class Business:
            async def inner(self, request_id):
                await asyncio.sleep(0)
                return 7
            async def outer(self, request_id):
                return await self.inner(request_id)
        for name in ('inner', 'outer'):
            self.obs.wrap(Business, name, name, request_param='request_id')
        self.assertEqual(asyncio.run(Business().outer('r')), 7)
        starts = [r for r in self.sink.rows if r['event'] == 'span.begin']
        self.assertEqual(starts[1]['parent_span_id'], starts[0]['span_id'])
        class Cancel:
            async def action(self, request_id):
                raise asyncio.CancelledError()
        self.obs.wrap(Cancel, 'action', 'async', request_param='request_id')
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(Cancel().action('r'))

    def test_future_fast_reply_no_extra_calls_no_retention(self):
        calls = []
        class Future:
            def __init__(self):
                self.value = None
            def query(self):
                calls.append('query')
                return True
            def result(self, timeout=None):
                calls.append(('result', timeout))
                return self.value
            def set_result(self, result):
                calls.append('set')
                self.value = result
        class MQ:
            def submit_request(self, operation, request_payloads):
                f = Future()
                f.set_result(9)  # Before submit returns.
                return f
        module = types.SimpleNamespace(__name__='lmcache.v1.multiprocess.futures',
                                       MessagingFuture=Future, __file__=__file__)
        self.obs.install(module)
        self.obs.wrap(MQ, 'submit_request', 'RPC', rpc=True)
        f = MQ().submit_request('lookup', [])
        self.assertEqual(f.result(timeout=60), 9)
        self.assertTrue(f.query())
        self.assertEqual(calls, ['set', ('result', 60), 'query'])
        self.assertTrue(any(r['event'] == 'future.set_result' for r in self.sink.rows))
        ref = weakref.ref(f)
        del f
        gc.collect()
        self.assertIsNone(ref())

    def test_observation_exception_does_not_replace_business(self):
        class Broken(MemorySink):
            def emit(self, *a, **k):
                raise OSError('disk')
        obs = hooks.Observer(Broken())
        class B:
            def action(self, request_id):
                return 3
        obs.wrap(B, 'action', 'STORE', request_param='request_id')
        try:
            self.assertEqual(B().action('r'), 3)
        finally:
            obs.uninstall()

    def test_bounded_unwritable_and_budget(self):
        with tempfile.TemporaryDirectory() as d:
            sink = hooks.Sink(Path(d), capacity=1, max_bytes=300, start_writer=False)
            sink.active = True
            sink.emit('first')
            sink.emit('second')
            self.assertEqual(sink.stats['dropped_buffer'], 1)
            sink.flush()
            for _ in range(10):
                sink.emit('more')
                sink.flush()
            self.assertGreater(sink.stats['dropped_budget'], 0)
            self.assertLessEqual(sink.stats['written_bytes'], 300)
            sink.close()
            file = Path(d) / 'file'
            file.write_text('occupied')
            bad = hooks.Sink(file, start_writer=False)
            bad.active = True
            bad.emit('x')
            bad.flush()
            self.assertGreater(bad.stats['write_errors'], 0)
            bad.close()

    def test_consistent_sampling(self):
        with tempfile.TemporaryDirectory() as d:
            a = hooks.Sink(Path(d), start_writer=False)
            b = hooks.Sink(Path(d), start_writer=False)
            a.sample_rate = b.sample_rate = .05
            self.assertEqual([a.selected(str(i)) for i in range(1000)],
                             [b.selected(str(i)) for i in range(1000)])
            self.assertFalse(a.selected(None))
            a.close(); b.close()


    def test_native_callback_is_existing_only_and_both_read_policies(self):
        calls = []
        codec_before = hooks.sys.modules.get('msgspec')
        hooks.sys.modules['msgspec'] = types.SimpleNamespace(msgpack=types.SimpleNamespace(
            encode=lambda x: json.dumps(x).encode()))
        class Dispatcher:
            def register(self, kind, handler, payload_type):
                calls.append(('register', kind, payload_type))
                self.handler = handler
                return 19
        def submit(stream, kind, payload):
            calls.append(('submit', stream, kind, payload))
            return 23
        module = types.SimpleNamespace(__name__='lmcache.v1.multiprocess.native_completion',
                                       __file__=__file__, DeviceHostFuncDispatcher=Dispatcher,
                                       submit_callback_to_stream=submit)
        try:
            self.obs.install(module)
            for kind in ('finish_read_by_owner', 'release_read_borrow'):
                d = Dispatcher()
                marker = object()
                def handler(payload):
                    calls.append(('handler', payload))
                    return marker
                self.assertEqual(d.register(kind, handler, tuple), 19)
                token = hooks.CONTEXT.set(dict(request_id='r', transfer_key=kind))
                try:
                    self.assertEqual(module.submit_callback_to_stream('stream', kind, (7, 8)), 23)
                finally:
                    hooks.CONTEXT.reset(token)
                self.assertIs(d.handler((7, 8)), marker)
            self.assertEqual(len(calls), 6)
            self.assertEqual(sum(e['event'] == 'device.callback.completed' for e in self.sink.rows), 2)
            # A callback from before a new window cannot complete a newer transfer.
            self.sink.active = False
            module.submit_callback_to_stream('stream', kind, (1, 2))
            self.sink.run_id = 'new-window'
            self.sink.active = True
            token = hooks.CONTEXT.set(dict(request_id='r', transfer_key='later'))
            try:
                module.submit_callback_to_stream('stream', kind, (1, 2))
            finally:
                hooks.CONTEXT.reset(token)
            self.assertIs(d.handler((1, 2)), marker)
            self.assertIs(d.handler((1, 2)), marker)
            self.assertEqual(sum(e['event'] == 'device.callback.completed' for e in self.sink.rows), 2)
        finally:
            if codec_before is None:
                hooks.sys.modules.pop('msgspec', None)
            else:
                hooks.sys.modules['msgspec'] = codec_before

    def test_mq_natural_queue_order_wire_identity_and_restore(self):
        import queue
        from dataclasses import dataclass
        class Future:
            def __init__(self): pass
            def set_result(self, value): self.value = value
        @dataclass
        class Wrapped:
            request_uid: int
            future: object
        class MQ:
            WrappedRequest = Wrapped
            def __init__(self): self.input_queue = queue.Queue()
            def submit_request(self, operation, request_payloads):
                future = Future()
                self.input_queue.put(self.WrappedRequest(17, future))
                item = self.input_queue.get_nowait()
                item.future.set_result(5)
                return future
        self.obs.install(types.SimpleNamespace(__name__='lmcache.v1.multiprocess.futures',
                                              __file__=__file__, MessagingFuture=Future))
        self.obs.install(types.SimpleNamespace(__name__='lmcache.v1.multiprocess.transport.zmq_impl.mq',
                                              __file__=__file__, MessageQueueClient=MQ))
        obj = MQ()
        f = obj.submit_request('store', [])
        self.assertEqual(f.value, 5)
        dequeued = next(r for r in self.sink.rows if r['event'] == 'rpc.queue.dequeued')
        self.assertEqual(dequeued['wire_id'], 17)
        self.assertEqual(self.obs.futures[f]['wire_id'], 17)
        ref = weakref.ref(obj.input_queue)
        del obj, f
        gc.collect()
        self.assertIsNone(ref())


    def test_transfer_key_metadata_and_failure_keep_original_contract(self):
        calls = []
        class Bus:
            def publish_on_stream(self, stream, event):
                calls.append((stream, event))
                return 4
        self.obs.install(types.SimpleNamespace(__name__='lmcache.v1.mp_observability.event_bus',
                                              __file__=__file__, EventBus=Bus))
        module = types.SimpleNamespace(__name__='lmcache.v1.multiprocess.modules.lmcache_driven_transfer',
                                       __file__=__file__, next_transfer_key=lambda request: request + '#1')
        marker = b'ipc-event'
        class Transfer:
            def retrieve(self, key):
                transfer = module.next_transfer_key(key.request_id)
                event = types.SimpleNamespace(event_type=types.SimpleNamespace(value='mp.retrieve.end'),
                                              metadata=dict(transfer_key=transfer, total_bytes=100,
                                                            num_tokens=0, prompt='NEVER-LOG'))
                self.assertion = Bus().publish_on_stream('stream', event)
                return marker, False
        module.LMCacheDrivenTransferModule = Transfer
        self.obs.install(module)
        instance = Transfer()
        value = instance.retrieve(types.SimpleNamespace(request_id='r'))
        self.assertIs(value[0], marker)
        self.assertFalse(value[1])
        self.assertEqual(instance.assertion, 4)
        self.assertEqual(len(calls), 1)
        metadata = next(e for e in self.sink.rows if e['event'] == 'transfer.enqueued_metadata')
        self.assertEqual(metadata['transfer_key'], metadata['metadata']['transfer_key'])
        self.assertEqual(metadata['transfer_key'], 'r#1')
        self.assertNotIn('NEVER-LOG', json.dumps(self.sink.rows))
        self.assertFalse(next(e['success'] for e in self.sink.rows if e['event'] == 'operation.result'))

    def test_specialized_signature_mismatch_leaves_method_untouched(self):
        class Dispatcher:
            def register(self, name, handler, payload_type):
                return handler
        original = Dispatcher.register
        module = types.SimpleNamespace(__name__='lmcache.v1.multiprocess.native_completion',
                                       __file__=__file__, DeviceHostFuncDispatcher=Dispatcher)
        self.obs.install(module)
        self.assertIs(Dispatcher.register, original)
        self.assertTrue(any(r.get('supported') is False for r in self.obs.capabilities))


def event(name, t, **kw):
    return dict(event=name, mono_ns=t, node='n', boot_id='b', process_instance='p',
                pid=1, run_id='run', request_id='r', generation=0, **kw)


class AnalysisTests(unittest.TestCase):

    def test_queue_bounds_fast_consumer_and_unknown_generation(self):
        rows = [event('rpc.queue.put.begin', 10, future_id='f'),
                event('rpc.queue.dequeued', 12, future_id='f'),
                event('rpc.queue.put.return', 14, future_id='f'),
                event('future.set_result', 20, future_id='f'),
                event('future.result.begin', 19, future_id='f')]
        phase = analyze.rpc_phases(rows)[0]
        self.assertEqual((phase['queue_lower_ns'], phase['queue_upper_ns']), (0, 2))
        self.assertIsNone(phase['ready_observation_to_result_begin_ns'])

    def test_worker_rank_requires_validated_registration(self):
        obj = types.SimpleNamespace(worker_id=0)
        self.assertEqual(hooks.worker_identity(obj), {})
        obj._pp_registration_info = types.SimpleNamespace(
            locator=types.SimpleNamespace(pp_rank=1), tp_rank=3, global_rank=19)
        self.assertEqual(hooks.worker_identity(obj)['tp_rank'], 3)

    def test_union_nested_serial_parallel(self):
        self.assertEqual(analyze.union_ns([(0, 10), (2, 5), (8, 15)]), 15)
        rows = [event('span.begin', 0, span_id='a', operation='outer'),
                event('span.begin', 2, span_id='b', parent_span_id='a', operation='inner'),
                event('span.end', 7, span_id='b', outcome='returned'),
                event('span.end', 10, span_id='a', outcome='returned')]
        spans, gaps = analyze.spans_from(rows)
        self.assertFalse(gaps)
        self.assertEqual(spans[0]['exclusive_ns'], 5)
        self.assertEqual(analyze.join_observed(0, {'0/0': 10, '1/0': 25}, ['0/0', '1/0'])['join_ns'], 25)
        self.assertIsNone(analyze.join_observed(0, {'0/0': 10}, ['0/0', '1/0'])['join_ns'])

    def test_ids_generation_out_of_order_missing_restart(self):
        rows = []
        for generation in (0, 1):
            for op in ('status1', 'status2', 'store1', 'store2'):
                rows.extend([event('span.begin', 1, span_id=op, operation='x') | {'generation': generation},
                             event('span.end', 3, span_id=op) | {'generation': generation}])
        spans, gaps = analyze.spans_from(list(reversed(rows)))
        self.assertEqual(len(spans), 8)
        self.assertFalse(gaps)
        rows[-1]['process_instance'] = 'restart'
        spans, gaps = analyze.spans_from(rows)
        self.assertEqual(len(spans), 7)
        self.assertEqual(len(gaps), 2)

    def test_native_exact_signature_zero_failed_ambiguous(self):
        rows = [event('device.callback.submitted', 1, kind='finish_read_by_owner',
                      completion_signature='sig', transfer_key=9),
                event('device.callback.completed', 3, kind='finish_read_by_owner', completion_signature='sig'),
                event('transfer.enqueued_metadata', 2, transfer_kind='mp.retrieve.end',
                      metadata={'transfer_key': 9, 'total_bytes': 100, 'success': True})]
        result = analyze.native_completions(list(reversed(rows)))
        self.assertEqual(result['completed_bytes'], 100)
        rows[-1]['metadata']['total_bytes'] = 0
        self.assertEqual(analyze.native_completions(rows)['positive_transfers'], 0)
        rows[-1]['metadata'].update(total_bytes=100, success=False)
        self.assertEqual(analyze.native_completions(rows)['completed_bytes'], 0)
        rows[-1]['metadata']['success'] = True
        rows.append(rows[0].copy())
        self.assertGreater(analyze.native_completions(rows)['ambiguous'], 0)
        self.assertEqual(analyze.native_completions(rows)['completed_bytes'], 0)

    def test_cross_clock_span_rejected(self):
        rows = [event('span.begin', 1, span_id='x', operation='x'),
                event('span.end', 5, span_id='x') | {'node': 'another'}]
        self.assertEqual(analyze.spans_from(rows)[0], [])

    def test_partial_legacy_retrieve_not_success(self):
        rows = [event('device.callback.submitted', 1, kind='finish_read_by_owner', completion_signature='s', transfer_key=1),
                event('device.callback.completed', 3, kind='finish_read_by_owner', completion_signature='s'),
                event('transfer.enqueued_metadata', 2, transfer_kind='mp.retrieve.end',
                      metadata={'transfer_key': 1, 'total_bytes': 100, 'retrieved_count': 1, 'num_tokens': 0})]
        self.assertEqual(analyze.native_completions(rows)['completed_bytes'], 0)


class CaptureTests(unittest.TestCase):
    def test_record_budget_finishes_without_touching_source(self):
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / 'source.log'
            log.write_text('private content\n')
            result = capture.record(dict(logs=[str(log)], pids=[999999]), Path(d)/'run',
                                    duration=.03, interval=.01, max_bytes=1)
            self.assertGreater(result['budget_dropped'], 0)
            self.assertEqual(log.read_text(), 'private content\n')
    def test_privacy_rotation_and_tail(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'service.log'
            p.write_text('old prompt secret\n', encoding='utf-8')
            tail = capture.LogTail(p)
            with p.open('a', encoding='utf-8') as f:
                f.write('prompt password secret\nRetrieved 64 tokens\n')
            rows = tail.read()
            self.assertEqual(len(rows), 1)
            self.assertNotIn('secret', json.dumps(rows))
            p.write_text('Retrieved 32 tokens\n', encoding='utf-8')
            rows = tail.read()
            self.assertTrue(any(r['event'] == 'log.rotation' for r in rows))

    def test_inspect_no_framework_import(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            p = root / 'lmcache/v1/distributed/l1_manager.py'
            p.parent.mkdir(parents=True)
            p.write_text('class L1Manager:\n def unsafe_read(self, keys): pass\n', encoding='utf-8')
            info = capture.inspect_sources(root)
            self.assertEqual(info['read_policy'], 'legacy_ttl_candidate')
            self.assertFalse(info['runtime_path_verified'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
