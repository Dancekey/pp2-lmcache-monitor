# PP2 / LMCache 只读监控与离线分析

Python 3.10+，采集与分析仅用标准库。公开仓库可通过 HTTPS 免登录克隆，不需要在客户机器上登录 GitHub、安装 GitHub CLI 或配置 token。

工具包含默认日志/指标采集、短窗口详细 hooks、离线分析。**默认命令不接入 hook、不启动或重启服务、不发推理请求、不占用设备。设备开销尚未验证；约 2% 以内是待验证目标。**

## 客户终端：克隆并运行

**PP=2 空泡独立诊断**：见 [PP_GAP.md](PP_GAP.md)，使用同一个仓库，拉取后运行 `python -S pp_gap/deploy.py --out /opt/ks-pp-gap-luv --run-id pp-gap-001`。该入口只部署参数模板并做CPU检查，不启动模型或缓存。

**夜间五小时或更长压测用 [LONG_RUN.md](LONG_RUN.md) 的 `watch.py`**：持续到手动停止、全程分钟趋势、异常前后留窗。下面 quick 的60秒是短时试采，不作为夜间长压方案。

在自己的容器内、已确认的私有文件系统中执行：

```bash
mkdir -p /opt/ks-pp-monitor-luv
cd /opt/ks-pp-monitor-luv
GIT_TERMINAL_PROMPT=0 git -c credential.helper= clone --depth 1 https://github.com/Dancekey/pp2-lmcache-monitor.git
cd pp2-lmcache-monitor
python -S quick.py --auto-logs --duration 60
```

`python` 应为现有 Python 3.10+；若名字不同，换成对应解释器。`-S` 跳过 sitecustomize，默认入口不导入 torch/NPU 框架。

`--auto-logs` 只查看当前容器 `/proc` 中 vLLM/LMCache 进程已有的 stdout/stderr 重定向，发现普通日志文件后增量读取。它不会读取 pipe/终端输出流、修改日志接线或重启服务。只查看当前容器有权限访问的进程。

会先打印 `DISCOVERED_LOGS` 和本次输出目录，再运行 60 秒，自动生成报告并打印 `SUMMARY`。两端各运行一次。直接把两端 `SUMMARY` 复制回来也能初步分析。

如果没有找到日志，先单独查看发现结果：

```bash
python -S find_logs.py
```

`logs` 是发现的文件路径；`processes` 显示 PID、角色和实际输出位置，不打印完整命令参数。`regular_file=false` 表示输出可能是 pipe/终端，不能当作日志文件读取。

也可显式指定已经确认的日志绝对路径：

```bash
python -S quick.py --log '/实际模型日志绝对路径' --log '/实际缓存日志绝对路径' --duration 60
```

只有一个日志时只写一个 `--log`。未找到匹配进程或日志时会明确报错，不能把它当作“没有 STORE/RETRIEVE”。输出只到终端而没有文件时，请按现有运维方式确认日志来源；工具不会自动改变服务接线。

中途 Ctrl+C 只结束本次采集并汇总已采数据。正常到期同样只结束监控，不停止模型或缓存。

### 一轮压测十几分钟时（短时 quick）

服务就绪后先开监控，再在另一个终端运行原压测。60 秒是试采，长轮可设置最多 1 小时、128 MiB 的观察窗口：

```bash
RUN="runs/monitor-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$RUN"
set -o pipefail
python -S quick.py --auto-logs --duration 3600 --max-bytes 134217728 --out "$RUN" 2>&1 | tee -i "$RUN/console.log"
```

压测完成后，在**监控终端**按一次 Ctrl+C，程序会保存并生成报告，终端打印 SUMMARY；不会结束另一个终端中的模型或压测。3600 秒是自动结束上限，不要求压测跑满一小时。若先达到时间或输出预算，监控也会提前汇总，需检查覆盖清单，不能当作已覆盖整轮。

`tee -i` 会保留 Python 收到 Ctrl+C 后打印的最终摘要，避免输出记录进程同时退出而丢失 SUMMARY。

```bash
cat "$RUN/console.log"
cat "$RUN/report/report.md"
```

## 更新工具，无需登录

先结束本工具自己的采集窗口，再在仓库目录执行：

```bash
GIT_TERMINAL_PROMPT=0 git -c credential.helper= pull --ff-only
```

若首次 clone 提示目录已存在，先确认它就是这个工具仓库，再进入目录更新；不要覆盖已有目录或删除实验结果。拉取代码不会改变运行中服务加载的 hooks；详细接线更新仍需相应计划内窗口。

## 默认日志版能看到什么

- 当前窗口新增的 Stored/Retrieved 日志条数与日志自报 token 数，不是唯一请求数或实际链路字节。
- 已有 Stored 日志里的主机耗时、最后观察到的 Running/Waiting、lookup 日志均值。
- 缓冲跳过、轮转、读取错误和输出预算；没有原始事件就不能补造更细时序。

默认 quick 每 5 秒增量读取，输出上限 8 MiB，可用 `--max-bytes` 调整；单日志每轮最多 256 KiB、残片最多 64 KiB。超过时跳过并计数。仅提取白名单数字，不写 prompt、KV 内容、完整环境变量或任意原始日志行。

输出位于 `runs/readonly-时间-编号/`：

- `run.json`：采集清单及丢失/覆盖情况。
- `capture.jsonl`：提取后的观察原件。
- `report/report.md`、CSV、`summary.json`、`timeline.json`：离线报告。

`null/NA` 表示缺证据；0 条匹配日志不证明没有发生相应业务。旧读取与新 borrow 路径不自动互换。默认读取器未提取的 RETRIEVE 耗时保留 NA。

## 详细时间边界与接入

见 [GUIDE.md](GUIDE.md)。详细 hooks 需要在目标进程导入 LMCache 前接入，因此首次接入通常需要批准的计划内重启。克隆仓库和运行 quick **不会完成这一步**。

已实现的主机观察包括：每次 STORE/RETRIEVE 提交、客户端自然入队/出队、server 方法、L1 reserve/read/borrow、原生完成 handler、Future 自然调用与 worker 完成上报。未安全覆盖的 socket 发送/接收、server 派发队列、纯设备执行、完整 PP 汇合及重新调度保持 NA。

## 本地验证

```bash
python -S -m unittest discover -s . -p 'test_*.py'
```

公开测试只使用构造的 CPU 合同与临时文件，不含客户数据、日志、模型、私有框架源码或历史实验原件。测试通过不代表设备、客户现场或性能开销验收。

原生回调观察沿用 native-v3 观察器的载荷签名＋transfer_key 关联思路；增加旧读取完成方式、失败过滤和歧义拒绝。没有启用关闭的 EventBus、添加设备完成回调、额外调用业务 Future 的 query/result 或消费业务队列。
