# 完整采集、详细接入与离线汇总

默认用 README 的 quick 命令即可。本指南给出详细模式的实际命令及能力限制；不自动部署或重启客户服务。

## 1. 配置与只读能力检查

在客户自己的私有容器里，进入已经克隆的仓库目录：

```bash
cp capture.example.json capture.json
vim capture.json
```

仅填写已确认的信息：`node`、原框架源码 `source_root`、已有 `metrics`/`status` URL、日志绝对路径 `logs`、进程 PID，以及模型/TP/PP/DP/MTP/APC/池大小/负载等 `settings`。没有地址或字段就留空/null，不填写凭据。模板的 128 GiB 只是配置参考，不代表实际分配量、驻留量或命中量。

```bash
python -S capture.py inspect --config capture.json --out capabilities-static.json
```

这是 AST 检查，不导入框架，不证明服务实际加载路径。`-S` 下 distribution 版本可能为 null。存在私有源码映射时，用 `source_overrides` 填实际文件。旧 TTL 和新 borrow 同时可用时，实际策略仍须由客户配置和运行事件确认。

已有指标/status 的默认采集：

```bash
python -S capture.py record --config capture.json --duration 300 --interval 5 --max-bytes 67108864 --out readonly-run
```

至少配置一种来源；输出目录必须新建。GET 超时上限 2 秒，不发推理或 MP PING。Ctrl+C 仅结束此采集命令。日志从命令启动时的末尾增量读，轮转/截断/落后跳过记入清单。

## 2. 计划内接入详细 hooks

需要客户批准的接入/重启窗口。保留原解释器、源码版本、STORE/RETRIEVE 行为和所有 TP/PP/MTP 参数。只修改已确认私有的接线文件，先保留备份及原启动环境。

假设仓库按 README 克隆在 `/opt/ks-pp-monitor-luv/pp2-lmcache-monitor`。在原来的源码映射 finder 安装之后、首次导入 LMCache 之前加入：

```python
import sys as _ks_pp_sys
try:
    import importlib.util as _ks_pp_util
    _ks_pp_spec = _ks_pp_util.spec_from_file_location(
        'ks_pp_monitor_hooks',
        '/opt/ks-pp-monitor-luv/pp2-lmcache-monitor/hooks.py')
    _ks_pp_module = _ks_pp_util.module_from_spec(_ks_pp_spec)
    _ks_pp_sys.modules['ks_pp_monitor_hooks'] = _ks_pp_module
    _ks_pp_spec.loader.exec_module(_ks_pp_module)
    _ks_pp_module.bootstrap()
except Exception as _ks_pp_error:
    print('KS_PP_MONITOR bootstrap failed: ' + type(_ks_pp_error).__name__,
          file=_ks_pp_sys.stderr)
```

原启动环境追加以下观察器变量，并让它们传入 scheduler、worker、headless、cache 的实际进程。每端 `KS_PP_MONITOR_NODE` 填自己的节点标签。

```bash
export KS_PP_MONITOR_DIR=/opt/ks-pp-monitor-luv/observations/traces
export KS_PP_MONITOR_CONTROL=/opt/ks-pp-monitor-luv/observations/control.json
export KS_PP_MONITOR_NODE='填写本端节点标签'
export KS_PP_MONITOR_BUFFER=4096
export KS_PP_MONITOR_MAX_BYTES=16777216
python -S capture.py disarm --control "$KS_PP_MONITOR_CONTROL"
```

然后按原启动脚本执行已批准的计划内重启。此工具不提供或执行清退、模型启动命令。bootstrap 拒绝晚于目标模块导入的热注入；接口不匹配会记录 unsupported。

接入但 disarm 时不写详细业务事件，writer 和轻量原生在途计数仍存在，不能称零开销。

## 3. 真实加载路径与窗口回读

`traces/capabilities-PID-INSTANCE.json` 记录实际模块 source 和方法签名。与新鲜 status 的 PID/instance、两端实际进程清单逐一对照；缺进程或缺 rank 不算完整接入。

```bash
python -S capture.py status --dir "$KS_PP_MONITOR_DIR"
```

两端使用同样的 run-id、seed 和采样率，先试 60 秒、1%：

```bash
python -S capture.py arm --control "$KS_PP_MONITOR_CONTROL" --run-id sr-001 --duration 60 --sample-rate 0.01 --seed ks-pp-v1
python -S capture.py status --dir "$KS_PP_MONITOR_DIR" --run-id sr-001
```

writer 每约 0.5 秒读取控制文件，首次回读可能尚未刷新。核对 fresh、active、deadline、PID/instance 及覆盖数量；单独写出 control 文件不能证明采集已启用。

只停止详细观察：

```bash
python -S capture.py disarm --control "$KS_PP_MONITOR_CONTROL"
python -S capture.py status --dir "$KS_PP_MONITOR_DIR" --run-id sr-001
```

稍后回读本轮新鲜 status 应为 active=false。到期同样只关闭观察，不停止模型、缓存或 writer。恢复接线在下一次批准的计划内重启前移除新增片段和五个专用变量，保留原有修改；不在运行中替换已注册 handler。

## 4. 两端汇总

将两个节点的观察文件通过现有渠道分别放入 `node-a`、`node-b` 目录，保留原文件和节点身份。不要把运行原件提交到公开 Git 仓库。

```bash
python -S analyze.py --trace-dir node-a --trace-dir node-b --status-dir node-a --status-dir node-b --run-id sr-001 --out report-sr-001
```

如需带上默认采集的配置，追加 `--capture-run node-a/readonly-run/run.json`，另一端同理。默认采集 run-id 要与筛选值一致，否则分开分析。

已有 benchmark 可追加 `--benchmark hooks-off=/实际关闭观察结果/benchmark.json --benchmark hooks-on=/实际开启观察结果/benchmark.json`。分析器只分列读取，不自动认定同条件、不把 LMCache OFF/ON 当观察器 OFF/ON。缺请求 ID 的 summary 不能与动作强配。

旧 v3 的 JSONL 或含 `events` 的 JSON，可用 `--events /实际原件路径` 离线分析。

## 5. 具体测量边界

| 已记录边界 | 含义 |
|---|---|
| `submit_store_request` / `submit_retrieve_request` 进入→返回 | 每次主机提交耗时，不代表设备完成 |
| 原有队列 put 进入、返回和 get_nowait 成功 | 本地队列等待上下界；不额外读取或消费队列 |
| server `store_with_chunk_mask` / `retrieve` 进入→返回 | 服务端主机处理总耗时，仍有未细拆步骤 |
| L1 reserve_write / reserve_read / unsafe_read / borrow_read | 主机方法耗时及实际返回结果分类，保持原读取策略 |
| 原生 callback 提交→已有 handler 返回 | 剩余 stream 工作、派发及 handler 的混合观察区间，不是纯 DMA |
| Future 自然 set_result/query/result/wait 等 | 完成准备、ready 被观察、业务消费的主机边界，不额外 query/result |
| worker get_finished 的返回集合 | 完成上报，不自动证明成功恢复 |
| LOOKUP/STATUS 每次回访及 server 方法 | 单次查询成本；回访间隔不自动归为 RPC |
| scheduler batch 元数据调用 | 已选成员及共享耗时，不平摊到每请求 |

旧读取 handler 包括 `finish_read_by_owner`，新 borrow 包括 `release_read_borrow`，STORE 为 `finish_write_by_owner`。使用已有 `msgspec` 的载荷签名＋transfer_key 关联；同签名多提交不按 FIFO 强配。零字节、去重、部分失败、缺回调不计成成功搬运。窗口前仍在途时保守拒绝关联，等集合自然归零再恢复，不增加等待或同步。

主机方法耗时包含未知等待；exclusive 只扣已知同线程、同请求/代的直接子区间并集。`result(timeout=60)` 的参数不算等待时间；实际阻塞为 NA。缺请求上下文的根 batch 调用会跳过，不能声称每次 wait_for_save/start_load_kv 整批入口均覆盖。

纯 socket 发送/接收、server 派发队列、staging/gather/scatter 细项、设备 stream 等待、纯 DMA、全 rank 汇合、重新调度到算子仍缺安全接缝。generation 未知或 rank 不全时不输出完整关键路径；跨机未校时不相减。设备 profiler 需另行批准短窗口，本工具不默认开启。

## 6. 报告和成本

`actions.csv` 是主机调用统计；`rpc_phases.csv` 是 Future/本地队列边界；`transfers.csv` 是 native handler 关联的元数据字节；`requests.csv` 是部分覆盖；`batches.csv` 不平摊耗时。`stragglers.csv` 无必要集合时只有表头，表示 NA。timeline 每进程独立归零，不可横向判断跨机重叠。

详细 trace 每进程整个生命周期默认最多 16 MiB、缓冲 4096 条，native 关联最多 4096 项；多次 arm 不重置预算。每进程仅一个 writer。满缓冲/竞争/预算/不可写时丢观察并计数，不阻塞推理等落盘。检查 status 的 dropped_*、write_errors、observation_errors、max_buffer 和新鲜度。缺 status、丢事件及序号首尾缺口不能当作完整采集。

原生签名关联在详细窗口会检查未采样 callback 的签名，降采样不保证成本同比下降。观察关闭、接线但 disarm、详细 arm、设备 profiler 应分别标注。吞吐影响约 2% 以内是待验证目标，须同版本、同输入/缓存初态和负载比较；目前尚未验证设备开销。
