# 一轮客户压测，多方向留档

2026-11-11：本轮只跑一组，必须开启 LMCache；计划发压两小时。历史慢池是 **480 GiB/台**。本文件是统一入口，旧工具说明作为细节参考。本地新增留档脚本不等于客户已部署，更不等于所有探针已接通。

## 今晚怎样取舍

优先 **LMCache + APC、128 GiB/台、已有设备验证的稳定版本**，保留客户模型、TP/PP/DP/MTP、原始数据和输出长度。128 是本次建议的容量，不把 480 擅自写成 800，也不把历史 480 记录改成 128。实际部署版本、形态和容量必须在新进程上核对；本工具不切代码或容量。

不为本次监控临时替换客户代码，优先客户现场已经验通的 LMCache 组合。128 的历史请求成功不保证所有客户输入数值等价。若现场只有 480 组合已验通，应保留其实际值并记录原因，不能未经核对套用另一环境128的稳定性结论。

单组能回答：慢在接纳、查询、恢复、STORE、PP 等待、计算还是资源压力；运行多久后恶化；失败或重算与什么条件同时出现。

单组不能独立证明：LMCache 比原生 APC 快/慢多少，或 480 容量本身导致变慢。原生 APC 和 480 的历史资料进入独立目录，只在数据、负载、版本、计算形态、缓存历史可比时作对照；否则只用于找机理线索。凌晨合成 7000 条不能充当今晚客户原文的原生基线。

两小时是客户发压时长，监控需覆盖启动/预热、发压和最后排空。不要在第 7200 秒把监控或尚未完成的客户端直接杀掉。正式请求速率、并发、输出、预热、数据范围沿用本轮客户确认条件，模板不替用户补定参数。具体启动时间、机器和服务结束处理尚未安排。

## 同一份数据怎样服务四个方向

| 方向 | 必留数据 | 探针/来源 | 最后用它判断什么 |
|---|---|---|---|
| 480 GiB/台为何慢 | 配置容量、初始化/注册起止与失败、实际分配、驻留/占用、对象数、回收/淘汰、读锁/borrow、staging；缓存进程 RSS/PSS/VmLck、缺页、NUMA、内存压力和 swap | 原始 cache 启动/运行日志、已有 status/metrics；session 资源快照；L1 详细 hook | 初始化与稳态分开；大池是否真的用到、是容量/内存压力还是更多恢复/控制等待。RSS 不是有效 KV，VmLck 也不等同驱动注册总量 |
| APC 与 LMCache 的收益/损失 | 每请求输入/输出、APC 实际命中、外部匹配与实际恢复 token、实际计算 token、STORE 去重/提交/完成、重算、TTFT/端到端/完整成功 | 客户原生逐请求结果、scheduler/connector、STORE/RETRIEVE hook、设备窗口 | 节省的计算与新增动作分别发生在哪些请求；只有同条件原生基线才能给净收益 |
| PP=2 空泡 | batch 成员、scheduled/computed tokens、各 rank 接纳/派发/进入/返回、KV ready、activation/旧 send wait、尾段/MTP反馈、资源名额；设备 kernel/通信/搬运区间 | pp_gap 主机探针＋原生 profiler＋校时＋各 rank 覆盖清单 | 区分没工作、资源限制、控制等待、等 KV/activation/反馈、设备通信；主机空等不等于设备空泡 |
| 长压性能与稳定性 | 逐请求到达/发送/首 token/结束、Running/Waiting/Deferred、吞吐、失败/超时/取消/未完成、PING 降级恢复、pending/completed/sessions、CPU/内存、丢采计数 | 原生 client、watch、session | 找变慢起点和异常前后 120 秒，关联长尾与最终请求结果，而非只看整轮均值 |

## 必须覆盖的动作与关联字段

每份证据带 `run_id / group / node / role / PID+start_ticks`；能取得时再带 PP/TP rank、request ID、源数据索引、batch、尝试/代次。缺少的字段写 unknown，不凭数组顺序拼请求。每机同时保存 wall 和 monotonic；跨机需额外校时及误差。

| 动作 | 需要保留的边界/返回值 | 当前能力与缺口 |
|---|---|---|
| 客户端 | 计划到达、实际发送、响应 ID、首 token、完成、输出长度、finish_reason、错误/超时/取消；最终完整结果 JSON | 保存原生详细结果和客户端日志；现有 pp_gap 客户端 hook 需与现场接口核对。HTTP200 不等于完整成功 |
| 本地 APC | 查询 token、实际命中 token、缓存块/占用、抢占/重算、请求实际计算 token | 已有 metrics/日志与 pp_gap；每请求字段未提供时仅有累计量，不能硬分流量 |
| LMCache lookup | 请求发起、ACK、首次 STATUS 发起、回复已设置、轮询返回 None/命中/miss、结果消费、放弃原因；两段返回与最小公共前缀 | hooks/pp_gap 覆盖部分边界；首次 STATUS/预算原因逐请求精确记录仍需现场确认，不能从 lookup 日志均值推算 |
| RPC/MQ | 自然入队/出队、server 方法起止、Future ready 观察/消费 | hooks 部分支持；socket 实际发送接收、server dispatch 内部等待仍缺安全接缝 |
| STORE | 需保存 token、RPC、reserve 成功/已存在/失败、元数据字节、真实提交与自然完成 | hooks 已有部分接缝；不能把 Stored token/RPC 数当唯一写入或物理 D2H 字节 |
| RETRIEVE | 匹配/请求恢复 token、read/borrow 结果、提交、自然 handler、各 rank get_finished、核心消费、recompute、最终 client 结果 | hooks 与 pp_gap 各有部分；Retrieved 日志不是全部设备完成；恢复失源必须关联最终结果 |
| L1 生命周期 | 配置容量与实际 allocated/used/resident、去重/淘汰/分配失败、读资格年龄、保护/释放、挂起写/读 | 原 status 只暴露其中部分；无字段即缺口。不能因 missing key 直接归因旧 TTL |
| 调度/PP | 等待队列、deferred、token/seq/KV block 名额、在途 batch、派发、各 rank 模型边界、PP 通信与反馈等待 | pp_gap 主机事件；所有必要 rank 清单与 batch 映射要现场核对 |
| 设备 | 每 rank 计算区间、HCCL/activation/反馈、H2D/D2H、stream 依赖；host-device 锚点 | 短时原生 profiler。独立 cache 进程必须有自己的设备 trace；模型 profiler 不代替它 |
| 资源 | CPU ticks/线程、缺页、PSS/RSS/VmLck、NUMA 驻留、cgroup 归属、PSI、swap、容器网络；设备利用/内存/错误 | session 连续采 CPU/内存，已有设备指标另填 URL。未配置设备源时无 NPU 利用率；普通容器网络量不代表 RDMA 流量 |
| 观测自身 | 每文件原路径/inode/字节范围、预算/积压、轮转/截断、write_errors、缺 rank/采样率、详细窗口起止 | session、watch、hooks 回执分别保留；某一个输出文件存在不代表全链路覆盖 |

## 采集分三层，避免一次压测被探针改变

1. **全程基础留档**：session.py 运行在独立 CPU 进程，自动启动现有 watch.py。保存完整原日志副本、完整 HTTP 指标/status 响应、5 秒资源快照和 60 秒 PSS/NUMA。watch 继续输出分钟趋势、异常窗口。无 hook 注入、无推理请求、无设备同步、无服务启停。
2. **短窗口动作观察**：复用 GUIDE 的 hooks 或 PP_GAP 的 observer。它们要在服务启动前接线。先验证实际源码、每 rank 回执、返回值/异常不变及观察开销。**两套都覆盖 adapter/Future/native handler，尚无联合设备验收，不应直接叠挂整轮。** 只切 arm/disarm 不能撤去已安装包装，不能假称这样已经隔离双 hook。运行配置必须选择一种经现场验证的接线，另一套覆盖不到的项如实保留缺口。
3. **短窗口设备 trace**：使用该版本已存在的 profiler 入口。建议在稳定段和变慢段各留短窗；之前空泡方案的约20批可作诊断长度参考，不把它当本轮已确认压测参数。保存启停请求/返回时间及实际捕获批次。跨机校时前后各留锚点；cache 无安全入口则写 `CACHE_DEVICE_TRANSFER_NOT_CAPTURED`。

现有 pp_gap 在业务线程同步写事件文件，且没有整轮字节预算；hooks 是有界异步 writer，但关闭状态仍有成本。两者都只有本地 CPU 检查，不能称全量开两小时已验证低开销。今晚优先稳定，因此不能为了“全部”默认启用未验证组合。若详细接线/设备 trace 未就绪，基础留档仍可运行，但 PP 设备空泡与精确全 rank 恢复因果不能宣称已采齐。

## 容器内一个留档入口

在包含`session.py`的工具版本中，优先使用免配置入口。两端在自己的容器、现有监控仓库各执行：

```bash
git pull --ff-only
python -B -S session.py record --run-id customer-night-20261111
```

它自动记录本端hostname、可见服务进程、日志及白名单参数，并查找采集开始后新增/修改的benchmark结果候选。模型/版本等无法安全读取的项保留未知，原始启动日志一并留档。不自动猜HTTP端口；完整metrics/status仍可用下述可选配置提供。无需为基础采集重填整套现场配置。压测完成并排空后Ctrl+C，执行`python -B -S session.py summary`查看最近一轮；独立客户端若在第三个容器，也需在那里采集或保留其结果。

下面是需要补充接口、文件或身份时的**可选手动配置方式**，不是自动入口的前置条件。只在已确认私有目录、原 Python 3.10+ 环境操作；不安装依赖。

先把 session.example.json 复制为私有的 session.json，集中填入：

- node、实际 run_id；两端同 run_id，不同 node；group 固定 LMCache_APC。
- settings：实际版本、池容量、形态、数据/负载/缓存初态；未知留 null。comparison_context 的 preferred 不是实际容量。
- logs：实际模型/cache/client 日志绝对路径，可配尚未创建的本轮文件；pids：本轮需要的 core/全部 worker/cache/client PID。
- metrics/status：**已经存在的只读完整 URL**，没有就空；不猜端口、不创建指标服务。该入口保留完整 labels 和原始响应，数据仅存服务器。
- retain_files：其他明确文件，例如最终 benchmark JSON、请求映射、已知 trace JSONL。不要填会反复覆盖的状态文件；其完整版本由原工具保留，必要时另做前/后快照。设备 profiler 整目录留在它原本的本轮私有输出位置并登记路径。

在实际工具仓库根目录，创建两个独立终端：监控终端执行下面命令；压测终端执行客户原命令。不要同时再手动开 watch，否则重复读日志。

```bash
RUN="/opt/ks-pp-monitor-luv/evidence/customer-night-$(date +%Y%m%d-%H%M%S)-$$"
python -B -S session.py record --config session.json --auto-logs --out "$RUN"
```

默认监控直到 Ctrl+C。SESSION_STARTED 只证明父采集器已启动；约5秒后检查 `$RUN/status.json` 的新鲜时间、watch_pid/watch_exit、archive 文件和积压；再看 `$RUN/watch-console.log` 的 WATCH_STARTED 与 `$RUN/watch/status.json`。确认覆盖两端和全部必要进程后再发压。自动发现仅识别当前容器可见的 vLLM/LMCache 输出，不能保证发现客户端/所有 worker，显式路径与 PID 清单是兜底。

原始日志从字节0复制，每文件每秒最多1MiB，默认64个源；保存初始字节数，分析时按本轮时间范围隔离历史。文件重命名后保留旧描述符，继续复制旧文件；copytruncate 在两次轮询间丢掉的字节不能恢复，会在可检测时报告。输入增长过快或预算耗尽不会偷偷跳过字节，但 pending_bytes 会增加；收尾有积压就不是完整归档。

默认每节点 raw 上限8GiB，HTTP/资源元数据上限256MiB，至少保留1GiB磁盘余量；watch 自身还占约192MiB预算。原服务日志、hook 和 profiler 文件另占空间，不包含在这些上限内。开跑前按可用空间和已有日志增长速度估算两小时成本；必要时提高 `--raw-mib`，不是先承诺8GiB能装全。预算满只停止相应留档，保持服务原样；原件不自动删除、不自动轮转覆盖。

发压结束且最后请求排空后，在监控终端 Ctrl+C。它只结束自己启动的 watch 子进程，保存最终 status；不停止模型/cache/client。必须核对 active=false、各 pending_bytes=0、archive errors、metadata_dropped，以及 watch 的输入跳过/过滤计数。filtered 是 watch 白名单过滤，不能当 raw 丢失；raw 完整也不能补出从未启用的探针。

使用现有可持久终端维持监控，断开连接后父进程不保证继续。此入口没有后台定时器、没有服务释放守卫，未安排今晚自动执行。

## 服务器上保留哪些文件

```text
本轮私有证据目录/                 # 两节点分目录，同 run_id
  session.json                   # 配置声明、开始时间、boot/Python 身份
  status.json                    # 文件→原路径/inode/字节范围、积压/错误、结束回执
  discovery.jsonl                # 当时发现的日志/进程
  processes.jsonl                # PID 启动身份、白名单运行参数
  resources.jsonl                # 连续资源样本；累计量按相同身份作差
  endpoints.jsonl                # 完整指标/status，保留 labels、采集前后时刻
  raw/000000.bin ...             # 精确原始字节，原路径见 status；后缀不改变内容
  watch/                         # 原来的分钟趋势、异常索引和前后窗口
  watch-console.log

另外登记并保留在本轮私有位置：
  模型/cache启动命令与日志、版本/实际加载路径/能力清单
  原始输入文件及manifest、client逐请求JSON/输出/最终benchmark JSON
  详细hooks事件+全部writer status、PP host事件、profile启停回执
  设备profiler完整目录、两机前后clock锚点、request/batch/rank映射
  historical/apc-only/ 与 historical/480-per-host/ 的原件及各自manifest
```

输入/输出文本、完整日志和配置可以只留服务器，汇总对外仅取数字与定位索引。不要把本轮数据加到公开工具仓库。历史目录明确原始版本、容量、数据、时间，不覆盖旧证据；首次快照与结束快照不能互相替代。

## 原件传不出来时怎样继续定位

先拿文件索引和当前采集回执：

```bash
python -B -S session.py index --run "$RUN"
python -B -S collect_case.py --run "$RUN/watch"
```

collect_case 的 CURRENT 部分永远是执行命令当时的新快照；今晚实际资源用 resources.jsonl，不能把明天重启后的 CURRENT 反填今晚。

需要查某个异常时，先通过 watch/windows.jsonl 定位时间，通过 status 的 archive 映射找到原文件。按实际文件名与字节偏移读取一小段：

```bash
python -B -S session.py extract --run "$RUN" --file raw/000000.bin --offset 0 --bytes 16384
```

这里的0仅是文件开头示例，后续按索引给具体偏移。每次最多1MiB；输出包含原件路径与字节范围。原始片段可能含客户文本，留在服务器查看或按客户允许范围回传。若只需数字，优先 collect_case 现有过滤摘要。设备原件不便传出时，在服务器运行已有 import_device/align_timeline/analyze，保留原trace与导出CSV之间的映射，不能只留截图。

## 开跑前用一张覆盖表判定准备状态

| 项 | 准备状态 | 真正开跑后需要的证据 |
|---|---|---|
| 单组 LMCache+APC、每台历史480口径 | 用户已确认 | 本轮实际源码、CLI、容量/形态回读 |
| session 原始留档与既有 watch | 本地实现，客户未运行 | 新鲜状态、持续新增文件、双节点及必要PID覆盖 |
| 客户端逐请求与最终结果 | 复用原工具，现场待确认 | 发送数=成功+失败+取消/未完成；最后排空回执 |
| L1/STORE/RETRIEVE详细接缝 | 已有单套工具；现场版本与开销待验 | 实际方法/签名、采样窗口、丢失及关联覆盖 |
| PP主机事件 | 已有独立工具；与另一套联合运行未验 | 所有必要rank、实际batch映射、资源状态 |
| PP设备/独立cache搬运与跨机校时 | 现场待验 | 原设备trace、前后锚点、误差与映射 |
| 今晚原生APC/容量切组 | 不执行 | 无本轮对照，历史可比性另审 |
| 定时启动/服务释放 | 未安排 | 具体机器、起止时间与服务处理明确后另落实 |

完成采集的判断是这张表逐项有本轮证据；不是监控进程活着，也不是所有文件夹存在。缺项保留，并限定能回答的问题。

## 本次本地核验

`python -B -S -m unittest test_session.py -v`：11项通过，覆盖原始字节与追加、预算满后保持积压、截断分代、文件晚创建、准确片段提取、路径越界拒绝、低磁盘保留源文件、完整指标/子监控异常收尾，以及免配置采集和新结果/客户端结果目录发现。

采集入口检查替代Linux平台判断、HTTP响应和子进程边界，实际采集/写文件/收尾代码运行；不是Linux客户运行结果。语法、配置JSON和新增文件UTF-8检查通过。未运行真实两小时、未验设备开销，也未验证客户上的双探针联合接线。
