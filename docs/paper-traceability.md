# 论文→代码映射、验收范围与未复现项

## 来源与“复现”的含义

来源：Mingxin Li et al., *Scaling LLM Agent Tool Access: The MCP Gateway*, SIGOPS ATC 2026，16 页，[DOI 10.1145/3828523.3857007](https://doi.org/10.1145/3828523.3857007)，[作者 PDF](https://inno-open.oss-accelerate.aliyuncs.com/open/paper/atc26-final25.pdf)。下文是独立分析，不是论文原文复制；仓库不包含 PDF。

本实现复现的是核心架构机制和可观察协议行为。论文原实现为 Alibaba Cloud L7 平台上的 Go 协议/会话组件与 WASM OpenAPI 转换组件；本实现是独立 Python 服务，不能称为原生产系统的源码复刻。

“已实现”仅表示有对应代码；“已测试通过”须由实际测试结果佐证，见 [测试记录](testing.md)。测试不会证明所有可能场景正确，更不会把本地桩数据上的结果转化为论文生产指标。

## 核心机制映射

| 论文位置 / 机制 | 对应实现 | 当前范围与差异 |
|---|---|---|
| §3 / Fig.5 统一网关 | `app.py`、`gateway.py` | 客户端面对统一 MCP 接入；独立 HTTP 服务，不嵌入云 L7 基础设施 |
| §3.1 / §4.1 body-driven 路由 | `Gateway.process/_dispatch`、OpenAPI compiler、`BackendManager` | 按 JSON-RPC 工具名解析、查路由、改写、转发；模块化阶段，不是 WASM 插件 ABI |
| §3.1 一 operation 一 tool | `openapi.compile_openapi` | OpenAPI 3.0/3.1 子集；命名空间、Schema 与参数映射挂载期生成 |
| §4.1 mount-time fail-fast | `openapi._Compiler`、`Gateway._mount` | 校验本地引用、参数/名称冲突、支持的 Schema/序列化；没有论文离线 LLM 语义补全 |
| §3.1 两类 MCP transport | `app.py`、`backend.py`、官方 SDK | frontend 与 backend 均支持 legacy SSE / Streamable HTTP 的工具调用路径 |
| §3.2 / §4.2 ingress 身份 | `auth.Authenticator`、`config.PrincipalConfig` | Bearer 字符串凭证映射主体、恒定时间比较；不实现 OAuth/OIDC/JWT 签发/验证 |
| §4.2 egress 身份映射 | `config.EgressConfig.credentials` | 环境变量提供共享/按主体覆盖的后端头；入口 bearer 不自动透传 |
| §4.2 细粒度工具权限 | `Identity.permits`、`Gateway`、`HybridRetriever` | list/search/call 都限制为已授权工具；后端 owner 重新验证授权 |
| §4.3 标准 Tool Search | `gateway.SEARCH_TOOL` | 普通 MCP 工具 `gateway.search`；无需扩展 `tools/list` 查询语义 |
| §4.3 dual-path indexing | `retrieval.HybridRetriever` | 名称/描述/顶层参数名；BM25 加 dense cosine；dense 索引初始化时构建 |
| §4.3 BM25 | `_sparse_ranking`、`tokenize` | `k1=1.2`、`b=.75`，camelCase 拆分与固定英文停用词；统计只基于授权语料 |
| §4.3 RRF | `reciprocal_rank_fusion` | 每路 `L=120`、`k=20`、1-based rank、并集融合；同分按名称稳定排序 |
| §4.3 pretrained dense + ANN | `DeterministicEmbedding` / `HTTPEmbeddingProvider` | 默认词法哈希不是预训练模型；可接真实 HTTP embedding；使用 exact cosine scan，**不实现 ANN** |
| §4.4 frontend/backend 双 session | `sessions.SessionManager`、`backend.BackendManager` | 随机 frontend ID，首次调用才建立 backend session；映射与 owner 元数据写共享存储 |
| §4.4 shared metadata | `store.RedisStore` | 真实 Redis，TTL、原子锁/令牌比较；内存 store 仅单进程开发 |
| §4.4 Routing-Hash | `SessionManager.create`、`_Connection.run` | SHA-256 派生稳定头，避免原始 frontend ID 透传；**未提供论文服务层多副本一致性哈希负载均衡器** |
| §4.4 cross-gateway Pub/Sub | `broker.Broker`、`Gateway.deliver_frontend` | 前端 SSE POST 转发到流 owner；后端连接 owner 经关联 reply 通道返回结果 |
| §4.4 Streamable HTTP 会话接管 | `backend.BackendManager` | 接收实例直接读取共享 backend ID/协议版本/路由头并发起后端 HTTP 调用；支持 JSON 或请求级 SSE 回应，不依赖初始化实例存活 |
| §7 Lesson 2 长连接复用 | `backend._Connection`、HTTP client pool | SSE 由专用 task 持有 SDK/AnyIO 上下文；HTTP 复用后端 session 与连接池；超时不自动重试 |

### MCP 支持矩阵

初始化支持 `2024-11-05`、`2025-03-26`、`2025-06-18`、`2025-11-25` 的版本协商；这表示受支持的工具调用子集，不表示整个版本规范全部实现。

| 特性 | 本版本 |
|---|---|
| initialize / initialized / ping | 实现 |
| tools/list / tools/call | 实现；一次返回当前可见目录，不提供多页 cursor 遍历 |
| 标准通知与取消 | 通知不产生 JSON-RPC 回应；支持已实现请求的取消路径 |
| `/mcp` POST + session header + DELETE | 实现 |
| `/mcp` GET SSE stream | 不提供，返回 405（协议允许的选择） |
| legacy `/sse` + POST endpoint event | 实现 |
| JSON-RPC 批请求 | 不支持；拒绝，不逐个猜测执行 |
| resources / prompts / sampling / elicitation / tasks | 不对外宣称或实现通用能力 |
| SSE event replay / Last-Event-ID 持久恢复 | 不实现 |
| 任意 server-initiated 双向请求 | 不提供通用透明代理保证 |

版本规范来源：[当前传输](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)、[生命周期](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle)、[工具](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)、[legacy SSE](https://modelcontextprotocol.io/specification/2024-11-05/basic/transports)。

## 明确未实现的生产扩展

| 论文项目 | 状态及原因 |
|---|---|
| §7 prerequisite knowledge cache / DAG | 未实现。论文未完整给出跨任意工具 Schema 自动推断依赖的方法；当前搜索不会补全前置调用链。 |
| §7 semantic LRU cache | 未实现。当前版本不缓存查询结果，避免把普通精确键缓存误称为语义缓存。 |
| §7 RE-Agent / Rewrite-Agent | 未实现。不调用 LLM 自动抽取领域规则或改写 ID-only 查询。 |
| §7 owner hint 编码 session ID | 未实现。沿用共享 metadata；论文也将编码方案描述为演进方向，未大规模部署。 |
| 原 Go (~8K LoC) / WASM (~6K LoC) 平台 | 未复现。使用 Python/Starlette/官方 MCP SDK，运行时与性能特性不同。 |
| 315 个云 OpenAPI 服务与生产认证体系 | 未集成。提供可独立运行的受控后端示例。 |
| 独立检索集群、ANN 向量索引与 3,616-tool 私有目录 | 未提供。exact scan 更易审计，但不代表同等扩展能力。 |
| service-tier 多副本 hash-ring 及扩缩容迁移 | 不包含。仅输出稳定路由头；接入真实服务 LB 后仍需额外验证。 |
| 进程崩溃后 legacy SSE owner 长连接无损迁移 | 不保证。SSE owner 不可达会显式失败，用户需建立新 session；HTTP 路径另有初始化实例退出后的续用测试。 |
| 持久 Pub/Sub / exactly-once | 不保证。Redis Pub/Sub 为瞬时协调通道，不是持久执行日志。 |
| 全面生产 metrics / tracing / multi-region HA | 不包含生产运维完整方案。健康探针与内部计数不等同于完整监控系统。 |

## 不可移植的论文数字

论文中的 Top-15 召回率、工具选择时间/Token 节省、转换微秒级开销、连接复用收益与缓存加速，来自论文对应硬件、工作负载、模型和云生产平台。它们是**论文结果，不是本项目结果**。

本仓库没有取得其完整私有目录、四条生产 trace、精确 pretrained encoder、ANN 配置、全部工具依赖与模型交互轨迹。因此：

1. 不把原论文数字写成本实现的性能承诺或测试阈值。
2. 不把手工构造的搜索 fixture 当成可比较的语义召回率基准。
3. 不把成功发送 HTTP embedding 请求当成模型质量评测。
4. 不把两网关/一个受控后端的测试当作千级实例压力与故障验证。
5. 若后续开展 benchmark，必须固定并发布工具/查询标注集、模型版本、参数、硬件与计时方法，单列测量结果。

## 安全与可靠性上的工程补充

论文没有逐一规定所有边界。本实现额外采用身份绑定 session、默认拒绝工具授权、环境变量凭证、输入大小限制、Origin 校验、请求超时、唯一关联 ID、owner 租约、取消传播、并发约束与明确的不重放策略。这些属于独立工程选择，不应归因成论文已详述或已证明的机制。

同样，这些防护不构成全面安全审计：后端 URL/OpenAPI 是可信管理输入；Redis 是可信协调网络；生产使用仍应另行评估网络隔离、TLS、凭证管理、审计、容量与故障模型。
