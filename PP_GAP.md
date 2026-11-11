# PP=2 空泡诊断：Git 拉取、部署、启动

这是独立的 PP 诊断工具，现有 `watch.py`、`quick.py` 不变。Python 3.10+，只用标准库，不需要 pip 安装。

## 1. 两端容器：拉取代码

在自己的容器、已确认的私有文件系统中执行。首次克隆：

```bash
mkdir -p /opt/ks-pp-monitor-luv
cd /opt/ks-pp-monitor-luv
GIT_TERMINAL_PROMPT=0 git -c credential.helper= clone --depth 1 https://github.com/Dancekey/pp2-lmcache-monitor.git
cd pp2-lmcache-monitor
```

如果已经按性能监控文档克隆过，进入原仓库更新即可：

```bash
cd /opt/ks-pp-monitor-luv/pp2-lmcache-monitor
GIT_TERMINAL_PROMPT=0 git -c credential.helper= pull --ff-only
```

不使用压缩包、上传工具或编码粘贴。

## 2. 两端容器：运行部署和CPU检查

```bash
python -S pp_gap/deploy.py --out /opt/ks-pp-gap-luv --run-id pp-gap-001
```

`python` 是当前环境已有的 Python 3.10+。两端使用相同 run-id，各自保存输出。成功时打印：

```text
CPU_CHECKS_PASSED=30
DEPLOYED_CPU_CHECKED_NO_SERVICES
DEPLOYMENT_RECEIPT=/opt/ks-pp-gap-luv/deployment.json
```

这条命令就是当前CPU准备阶段的启动入口。它检查诊断源码和30项CPU合同，生成环境及参数模板；不启动模型、缓存、监控采集或HTTP请求，不导入torch/NPU/vLLM/LMCache框架，不修改现有Python环境或推理源码。代码保留在Git仓库中，不复制安装到site-packages。

输出包括 `deployment.json`、`cpu-check-result.json`、`runtime-OFF.json`、`runtime-ON.json` 和四个 `env-组-阶段.sh`。重复部署会重新检查代码，但保留已经编辑的runtime/env文件；拉取新代码不会热更新已运行的服务。新实验应使用新的out目录和run-id。

仓库不含客户原文、模型、运行日志、Core源码或native二进制。重放参数模板中的389条、10RPS、seed2002等仅是模板值，不能证明已有数据或已获设备授权；正式执行前按本轮确认条件冻结。

## 3. 以后有设备窗口：启动详细观测

CPU部署完成不等于观测已经开始。空泡挂钩需要在原有source finder之后、首次导入目标框架模块之前安装，通常需要本轮授权下的计划内服务启动；不能给一个已经运行的模型另开普通Python进程来补装。

沿用性能监控 GUIDE 的接线位置，在原有私有启动入口中追加下面三行；**不替换原来的sitecustomize.py或源码finder**：

```python
import sys
sys.path.insert(0, '/opt/ks-pp-monitor-luv/pp2-lmcache-monitor/pp_gap/observer')
import ks_pp_startup
```

该入口只有在 `KS_PP_DIAG=1` 且 `KS_PP_DIAG_PP=2` 时才安装。需要确保scheduler、全部worker、headless及ON专用cache的实际进程都执行该入口。PP1或其他缓存路径不接入。已加载目标模块会在observer.enabled中列出，不能把晚注入当完整覆盖。

ON组前段，在原有服务启动的同一个终端中执行：

```bash
source /opt/ks-pp-gap-luv/env-ON-pp0.sh
```

ON组后段：

```bash
source /opt/ks-pp-gap-luv/env-ON-pp1.sh
```

OFF组分别换成 `env-OFF-pp0.sh`、`env-OFF-pp1.sh`。这些文件只设置观察变量，不启动任何服务。随后仍使用本轮已核对的原生 `vllm serve` 和cache命令；本工具不提供通用模型启动器，不改查询、STORE、通信或流水策略。模型和native版本由本轮实际环境单独核实。

`KS_PP_DIAG_RANGES=1`仅添加CPU profiler范围，不自动开始NPU profiler。短窗口设备采集仍需用实际版本原有的profiler能力启停，独立cache的设备搬运也不能由模型进程profiler代替。

既有finder选择顺序保留；非标准loader执行原路径，只安装方法挂钩并报告缺少语句级边界。必须查看实际module.loaded、hook.method、hook.coverage及全部相关rank，不能用CPU检查代替现场覆盖验收。

## 4. 回读与分析

当前部署结果：

```bash
cat /opt/ks-pp-gap-luv/deployment.json
cat /opt/ks-pp-gap-luv/cpu-check-result.json
```

未来实际采集的事件保存在环境文件声明的 `traces/OFF或ON/pp0或pp1/pp-PID.jsonl`。默认不打开旧版逐key和全量STORE日志。

`pp_gap/analyze.py`支持批次关联表、逐请求TTFT及完成吞吐、host/设备时间线。输入要求真实事件；没有设备证据时只输出host边界，没有校时不能直接跨机相减。`align_timeline.py`、`clock_probe.py`、`import_device.py`用于经核验的校时和设备事件映射；不自动猜设备lane或把Python模型入口当设备计算。

现有性能监控仍按原方式启动：

```bash
python -S watch.py --auto-logs --duration 0
```

这是读取既有日志的独立监控，不自动开启空泡hooks，不替代两段真实设备时间线。当前只做CPU部署时无需运行它。
