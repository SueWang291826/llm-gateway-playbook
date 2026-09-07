# llm-gateway-playbook

> **生产级 LLM 网关的稳定性与性能工程手册**
> Battle-tested reliability & performance patterns for production LLM gateways — distilled from a real deployment.

这个仓库是我在一个生产级 LLM 网关项目（OpenAI 兼容 API 包装器，转发请求给 CLI agent 子进程，8 worker 无共享内存部署）上做稳定性与性能改造的**技术精华沉淀**：六次线上事故的完整复盘、四层防御架构、Map-Reduce 分析管线的设计演进、对冲请求治理尾延迟的实战、以及三个可以直接复用的模式参考实现。

所有内部标识（公司名、域名、IP、模型别名、工单系统）均已匿名化；数据（耗时、规模、超时率）保留真实测量值——**它们是结论的证据**。

---

## 成果速览

| 指标 | 改造前 | 改造后 |
|---|---|---|
| 多轮工单分析 | prompt 滚雪球至 35k~66k 字符，**62.5% 撞 600s 超时** | **约 1 分钟**稳定出结论 |
| 大工单（~470 文件）分析 | 单 agent 串行读文件，大概率超时 | Map-Reduce 并发，**切片 ≤24 片有界** |
| 追问轮 | 重跑全量分析 | 文件小结**跨轮缓存命中 0 次调用** + 直答 |
| 汇总阶段稳定性 | 主模型网关排队 ~200s 必超预算，串行重试 ≈2× 预算 | **对冲并行，≤1× 预算必达** |
| 静默故障 | 模型挂了用户先发现，无告警无熔断 | 启动探针 + 跨进程熔断预置，`[ALERT]`/`[WARN]` 透明日志 |
| 大 prompt 秒 500 | 78KB 中文 prompt 触发 argv 上限（E2BIG） | **stdin 传递，根除** |
| 测试 | — | 全量 **217 passed**，随提交从 207 逐次增长（每个修复带回归用例） |

## 仓库结构

```
llm-gateway-playbook/
├── README.md                        # 你在这里
├── docs/
│   ├── 01-architecture.md           # 四层防御：LLM 网关稳定性架构
│   ├── 02-postmortems.md            # 六次线上事故完整复盘（本仓库核心）
│   ├── 03-blackbox-debugging.md     # 无服务器权限的黑盒取证方法论
│   ├── 04-map-reduce-pipeline.md    # 日志分析 Map-Reduce 管线三次演进
│   ├── 05-hedged-requests.md        # 对冲请求：用实验定罪网关排队，用并行兜底
│   ├── 06-context-compression.md    # 滑动窗口摘要 + 前缀缓存适配
│   └── 07-pattern-references.md     # 每个机制背后的成熟模式与出处
├── patterns/                        # 干净的通用参考实现（无业务依赖，可直接复用）
│   ├── circuit_breaker.py           # 跨进程 SQLite 熔断器（closed→open→half-open）
│   ├── hedged_request.py            # asyncio 对冲请求（预算内主优先 + 保底必达）
│   └── staged_budget.py             # 分级截止期预算 + 自适应波次预算公式
└── tests/                           # 参考实现的测试
```

## 快速上手

```bash
git clone <this-repo>
cd llm-gateway-playbook
python -m pytest tests/ -v     # 无第三方依赖，标准库 only
```

三个模式实现刻意零依赖（标准库 `sqlite3` / `asyncio` / `dataclasses`），复制单文件即可用。

## 六次事故一览（详见 [docs/02-postmortems.md](docs/02-postmortems.md)）

| # | 现象 | 根因 | 一句话教训 |
|---|---|---|---|
| ① | 全量请求 500 | 重构闭包后变量赋值位置变化 → `UnboundLocalError` | 没有端点级测试的重构是裸奔 |
| ② | SSE 连接被网关掐死（200 但 0 字节） | 先算完再开流，空闲期无字节 | 流式接口必须**先开流、有心跳、失败也走 200** |
| ③ | 连续分析 409 锁超时 | 单切片挂住烧全局超时、占锁不放 | 每个阶段要有**独立的死线**，挂住的只烧自己那份 |
| ④ | 436 文件切片 60.0–60.1s **整齐**全超时 | 无差别切片 + 切片过大 | 超时点整齐 = 你自己的预算不够，不是网关抖 |
| ⑤ | 白名单生效后巨型 batch 切片再全灭 | 切片数量无上限 → 合并成超大 prompt | 问题从「筛选」升级为「预算」；轮转分配保公平 |
| ⑥ | 汇总频繁降级 + 78KB prompt 秒 500 | 网关通道排队（与输入无关）+ argv 128KB 上限 | 平坦的延迟曲线指向排队；对冲并行 ≤1× 预算必达 |

## 模式清单（出处与适配见 [docs/07-pattern-references.md](docs/07-pattern-references.md)）

- **熔断器状态机**（Netflix Hystrix / Resilience4j）→ SQLite 跨进程共享 + 「超时不算失败」
- **对冲请求**（Google《The Tail at Scale》）→ 预算内主模型优先、保底必达、总成本 ≤1× 预算
- **Map-Reduce 范式**（Google / LangChain map_reduce）→ 白名单筛选、全局预算、目录轮转、跨轮缓存
- **滑动窗口 + 渐进摘要**（ConversationSummaryBufferMemory / memGPT）→ 摘要只追加，喂饱上游 prefix cache
- **Stale-While-Revalidate**（RFC 5861）→ 慢源数据秒回旧值
- **头尾采样 / Round-Robin / 分级截止期 / SSE 心跳** 等工程惯例

## 声明

- 本仓库为个人技术沉淀，**不含任何原项目的业务代码、配置与内部标识**；`patterns/` 下为实现思路的独立重写。
- 发布前请自行确认符合雇主的相关政策。

## License

[MIT](LICENSE)
