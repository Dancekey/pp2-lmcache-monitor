# 一次性打印诊断资料

在两端各执行一次。命令只读，不发模型/缓存请求、不重启、不修改配置、不创建诊断文件；会读取少量 `/proc` 并取约1秒CPU快照。客户输出不提交公开仓库。

```bash
git pull --ff-only
python -B -S collect_case.py
```

默认读取仓库 `runs` 下最近三份有 status 的长压结果，逐轮标记，不混为一组；打印采集概况、首末及最高值分钟、各类最严重触发及所在窗口的队列上下文。无需 RUN 环境变量。

要固定重启前某一轮，使用启动时已有的实际输出目录：

```bash
python -B -S collect_case.py --run /实际旧采集目录
```

输出区段：

- `HISTORICAL_RUN`：旧轮次的路径、节点、覆盖和丢失；配置未登记时仍为unknown。
- `MINUTE`：首末及高lookup/高Waiting/慢STORE/慢RETRIEVE的分钟行；不是全夜每条原始日志。
- `WORST`：每种触发的最高值、原日志时间、字节索引及周边队列，保留对照时序。
- `CURRENT_LOG_DESTINATIONS`、`CURRENT_PROCESS/PARAMETERS/MEMORY`：**当前快照**。重启后采集的参数和CPU/内存不能倒推旧轮原因；只打印白名单参数/环境字段，不打印完整命令或环境。
- `CURRENT_LOG_TAIL_NUMERIC_ONLY`：当前日志尾部的白名单数字，明确与旧监控分开；可能已是重启后的新日志。
- `BENCHMARK_CANDIDATE`：仅从仓库父目录及常见results/benchmark/benchmarks/output目录有限查找小型JSON，打印数字摘要及时间，不自动归为128或480等组。可用 `--base /实际个人工作目录` 指定这个查找根。

如果 `BENCHMARK_SEARCH` 为0，仍需要操作者补贴压测结束时的数字表，或提供实际结果文件路径；监控条数不能替代请求成功数、吞吐和TTFT。

默认每轮最多扫描32MiB异常证据，最多打印40个最新结果JSON中的有效摘要；超出或不可读会有范围提示，不声称全量。没有找到CPU/proc或cgroup字段时保留不可用/null；cgroup事件和缺页等累计值不能直接归因本轮。RSS含共享页，不将各worker RSS简单相加当总占用。

本入口不自动对照、不改容量、不执行新压测。先保留旧轮证据，再用相同版本/输入/计算形态/冷暖流程做容量对照。
