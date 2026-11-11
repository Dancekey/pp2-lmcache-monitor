# 夜间长压：按怀疑点留证据

长压应回答“什么时候开始变慢、持续多久、前后有什么变化”，不能只留整轮平均值。`watch.py` 是独立只读进程，复用现有 LogTail 和 metrics/status 读取，不修改生产调度、缓存策略或设备完成条件。

## 先确定看什么

| 怀疑点 | 记录内容 | 判断边界 |
|---|---|---|
| 查询长时间没有推进 | lookup 日志指标、队列/Deferred 的分钟趋势；已有 trace 中的 RPC、ready→消费区间 | lookup 日志均值含回访间隔，不等于单次 RPC |
| 有请求等待但没调度 | 同一来源/PID/engine 的 Running、Waiting、持续时间与前后窗口 | 日志持续零 Running 不能证明设备无计算/通信 |
| PING 降级或恢复 | unhealthy/healthy 标志、同窗查询/队列/已配置缓存 status | 无逐 PING 事件时不能证明网络断开或唯一根因 |
| STORE/RETRIEVE 偶发慢 | 主机日志耗时、调用/对象信息的已有 trace、异常前后状态 | 不叫纯 D2H/H2D；重复 RPC、日志 token 不算成功搬运量 |
| 恢复失源或重算 | source miss、Recovered/BYPASS 标志及同窗状态 | 标志不代表请求最终成功；需对应 client 结果 |
| 完成已产生但消费迟 | 同一进程实例/Future ID 的原有完成/自然消费边界 | 没有 hooks 不补造；缺 end 只叫“未观察到结束” |
| 运行越久资源越积越多 | 已配置的 sessions/completed_results/pending/容量等分钟原值 | 没有导出的字段为 NA；不能用 HTTP 健康代替 MP PING |

## 命令：先监控，再压测，压测完再停

在自己的容器私有目录、仓库根目录执行：

```bash
git pull --ff-only
python -S watch.py --auto-logs --duration 0
```

`--duration 0` 表示持续到你在**监控终端**按一次 Ctrl+C，没有一小时限制。也可明确设置 `--duration 21600`（最多六小时）；这只控制监控，不限制服务或压测。模型/缓存先就绪，再启动监控，看到 WATCH_STARTED 后在另一个终端运行原压测。

监控每 5 秒读已有来源，每分钟打印 PROGRESS，新异常窗口打印 ANOMALY_WINDOW。压测结束后在监控终端 Ctrl+C：只结束监控并打印 SUMMARY，服务和压测进程不受它控制。

需要指定输出目录和保存终端输出时：

```bash
RUN="runs/night-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$RUN"
set -o pipefail
python -S watch.py --auto-logs --duration 0 --out "$RUN" 2>&1 | tee -i "$RUN/console.log"
```

没有找到文件日志时，用 `python -S find_logs.py` 看实际输出位置，或用一个/多个 `--log /实际绝对路径`。工具不读取服务的 pipe/终端、不为获得日志重启服务。每分钟重新发现新文件，最多64个来源；新增来源已有历史不会倒当成本轮新事件。

## 可选已有指标与已有详细 trace

如果已有 metrics/status URL，沿用 capture 配置，填写实际日志和端点及 read_policy/版本等 settings：

```bash
python -S watch.py --config capture.json --auto-logs --duration 0
```

没有配置就不发任何 HTTP 请求。配置中的计算形态/读取策略只是声明，不代替真实加载路径核验。不自动猜客户地址/端口。

若服务已按 GUIDE 接入 hooks，并已有详细 trace 文件，可旁路读取：

```bash
python -S watch.py --auto-logs --trace-dir /实际已有trace目录 --duration 0
```

这不会安装/开启 hooks。也不建议在尚未验证设备开销的情况下把所有详细 hook 全量开一夜。trace 输出窗口关闭、预算耗尽或丢事件后，missing-end 可能是观察缺失；近窗会保留已有 writer status，便于核对。

## 初始触发条件

这些阈值只筛选证据，**不改变业务 timeout 或压测参数**：

- lookup 日志均值≥10秒：捕捉查询/回访长窗口。
- STORE/RETRIEVE 自报主机耗时≥1秒：捕捉慢方法候选。
- 同一日志/PID/engine 中，Running=0且Waiting>0连续被观察≥30秒：保留低推进候选；缺样、轮转、PID更换会重新计时。
- unhealthy、读取失源、Recovered/BYPASS：直接留窗。
- 已有 trace 的同进程 host span/Future 调用≥1秒、RPC 或 ready→消费≥30秒，或结束未被观察到≥30秒：留窗并保存开始事件。重复 ready 回访不重置计时，快回复、乱序、不同进程实例不按 FIFO 强配。

可调整 `--lookup-ms`、`--host-ms`、`--wait-seconds`。默认保留异常前后各120秒，可用 `--pre-seconds`/`--post-seconds` 修改；相邻触发合并在固定的后窗口内，不声称整段故障已被精确界定。

## 最后看哪些文件

| 文件 | 用途 |
|---|---|
| `minutes.jsonl` | 全程分钟趋势：事件数、数字字段 n/sum/max/last、丢失计数、采集机 UTC 分钟；lookup 的 sum/n 不作为唯一请求均值 |
| `windows.jsonl` | 哪个 UTC 时刻、什么原因触发了哪个异常窗口，文件是否成功保留 |
| `incidents/window-*.jsonl` | 前后数字证据、触发原因和可取得的身份/字节索引/原日志时间；缺部分会有计数 |
| `status.json` | 每轮刷新，含 active、来源偏移、预算/丢失和 tracker 状态；长压过程中即可读 |
| `summary.json`、`report.md` | 结束后的汇总及解释限制 |

```bash
cat "$RUN/status.json"
cat "$RUN/windows.jsonl"
tail -n 10 "$RUN/minutes.jsonl"
cat "$RUN/report.md"
```

采集机时间用于定位观察窗口；原日志时间字符串与 byte offset 用于回查。未自动校时，也不把容器日志时区当成北京时间。两端各采一份，按节点、源码版本、负载和数据范围对照，不能拿跨机 monotonic 相减。

## 五小时以上的成本与缺口

正常期间主要留分钟统计和近期环形上下文，不保存全夜每条日志。前窗口最多2048个已筛选事件；事件很密时不保证保全120秒，pre_context_evicted 会披露。开始事件也保存在长等待触发记录里，避免等几分钟后把起点挤掉。

正常的快速 STORE/RETRIEVE 日志在每次读取内按来源/PID和操作合并为数字批摘要，分钟统计仍计入全部已读行；它们不会以几千条逐行记录挤掉队列/健康上下文。慢操作和异常仍单独留证。批摘要的 token 和主机耗时不解释成实际搬运量或请求数。

异常证据默认上限128 MiB，趋势/窗口索引默认64 MiB，状态/最终摘要另为固定大小文件；可用 `--max-evidence-bytes`、`--max-trend-bytes` 调整。**达到预算后继续检测和计数，不因预算退出**，但停止保存超预算的内容，losses 会明确报告，不声称数据完整。`retained=false` 表示该窗口未能落盘。磁盘不可写会报丢失/状态失败。

单来源每轮最多读256 KiB，残片64 KiB；落后太多会跳过并记字节数。每轮只读文件/已有接口，不 query 业务 Future、不 drain 业务队列、不新增设备同步、线程或回调。

已有 hooks 中的原生完成、真实设备工作、PP 汇合、scheduler 重新接纳等能力缺口仍按 GUIDE 处理。仅日志模式不能区分“RPC慢、调度回访迟、结果消费迟”的每个请求；没有逐请求事件时，只定位候选等待窗口，不宣称唯一根因。

本地已覆盖模拟五小时之后的异常、预算耗尽继续工作、正常/迟到/乱序/缺失事件等合同。模拟时间轴不代表真实五小时压测，更不代表设备开销≤2%已验收。
