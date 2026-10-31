# MCP Gateway：论文架构的独立 Python 实现

根据 **[Scaling LLM Agent Tool Access: The MCP Gateway](https://doi.org/10.1145/3828523.3857007)**（Li et al., SIGOPS ATC 2026）实现的可运行参考系统：统一 MCP 接入、OpenAPI 转换、工具级权限、混合工具检索，以及跨网关会话与 SSE 协调。

> **复现边界**：这是独立的 Python 实现，不是论文作者的 Go/WASM 源码。默认 dense 路为确定性词法哈希向量，**不是预训练语义模型**；可显式接入真实 embedding HTTP 服务。未获得论文的私有工具库与生产流量，**不声称复现其 98% 召回率、生产规模或性能指标**。详见 [论文→代码映射与差距](docs/paper-traceability.md)。

**推荐入口：** [Docker 部署](#docker-deployment) · [配置与权限](#configuration) · [测试与验收](docs/testing.md) · [架构与论文差距](docs/paper-traceability.md)

## 1. 已实现的核心机制

| 能力 | 本仓库实现与范围 |
|---|---|
| 统一接入 | `/mcp` Streamable HTTP；`/sse` + `/messages` legacy HTTP+SSE |
| 后端适配 | 官方 MCP Python SDK 连接 Streamable HTTP 或 SSE 后端；跨传输调用 |
| OpenAPI → MCP | 3.0.x / 3.1.x 的明确子集；每个 operation 一个工具；挂载期编译和 fail-fast |
| 函数卸载 | Bearer 凭证→主体；工具级 allow 通配策略；入口与出口凭证分离 |
| 工具搜索 | 标准 MCP 元工具 `gateway.search`；BM25 + cosine 两路，RRF 融合 |
| 会话路由 | frontend/backend session 分离；Redis 共享会话元数据；稳定 `Routing-Hash` |
| 长连接协调 | 复用后端会话/连接，跨实例 Pub/Sub 关联请求与返回通道 |
| 测试与交付 | 单元、协议边界/功能、真实 Redis 与官方 SDK 集成；Docker Compose 与 GitHub Actions |

**不包含**：完整 OAuth/OIDC/JWT 登录系统、所有 OpenAPI/MCP 特性、ANN 向量数据库、论文 §7 的知识 DAG 缓存/语义 LRU/RE-Agent、生产级指标体系与私有基准复现。Python 的独立实现和小规模正确性测试不等于云规模验证。

<a id="docker-deployment"></a>

## 2. Docker 部署：两个网关 + Redis + 后端

```bash
git clone https://github.com/liuzongquan/mcp_gateway.git
cd mcp_gateway
export GATEWAY_TOKEN="$(openssl rand -hex 32)"
docker compose config --quiet
docker compose up --build --wait --wait-timeout 120

# 真正检查 A/B 交替请求仍使用同一有状态后端会话，计数应为 1、2、3、4
docker compose exec -T gateway-a python - < docs/compose_smoke.py

docker compose down --volumes --remove-orphans
```

- Gateway A：`http://127.0.0.1:8080/mcp`
- Gateway B：`http://127.0.0.1:8082/mcp`
- SSE：对应端口的 `/sse`；SDK 会从 `endpoint` 事件取得 POST 地址。
- Redis 和演示后端不映射宿主机端口；仅在 Compose 网络内可访问。
- 网关容器使用非 root 用户、只读根文件系统与删除的 Linux capabilities。
- Compose 是本地开发拓扑，**不是生产高可用部署**；不启用 Redis 持久化/TLS，不提供外部负载均衡器。

Docker 检查的实际执行状态见 [测试与复现记录](docs/testing.md)。提供配置文件不等于已运行容器验收。

### 2.1 首次 Docker 部署：无需本地 Python / Redis

需要 Docker Engine + Compose v2；获取代码与生成随机 token 的上述命令使用 Git 和 OpenSSL。Docker 路径不要求宿主机安装 Python 依赖或 Redis，镜像内已安装锁定依赖。镜像由本仓库本地构建，未声称提供预发布的公共镜像。首次拉取基础镜像和依赖需要联网。

启动的服务：

| 服务 | 镜像/来源 | 网络与用途 |
|---|---|---|
| `gateway-a` | 本仓库 Dockerfile，`paper-mcp-gateway:local` | 宿主 `127.0.0.1:8080` → 容器 8080 |
| `gateway-b` | 同上 | 宿主 `127.0.0.1:8082` → 容器 8080 |
| `backend` | 同一个镜像，启动 `examples.demo_backend:app` | 内部 8081；仅演示、无入口认证 |
| `redis` | `redis:7.2.10-alpine` | 内部 6379；协调数据，不映射宿主端口 |

若希望重启终端后保留演示凭证，**仅在没有 `.env` 时**创建；Compose 自动读取它：

```bash
# 不覆盖已有配置；文件禁止提交，权限限制为当前用户
if [ ! -e .env ]; then
  (umask 077; printf 'GATEWAY_TOKEN=%s\n' "$(openssl rand -hex 32)" > .env)
fi
docker compose up --build --wait --wait-timeout 120
```

shell 中已导出的 `GATEWAY_TOKEN` 优先于 `.env`，不要误以为修改文件就会覆盖旧 shell 值。Compose 未设置凭证时拒绝启动，不提供公开的默认万能 token。`.env` 仅适合本地演示；生产应使用受控 secret 注入，而不是把真实 token 写入 Git。

### 2.2 常用运维命令

```bash
docker compose ps                              # 各服务健康状态
docker compose logs --tail=100 gateway-a gateway-b
docker compose logs --follow backend redis
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8082/healthz
docker compose exec -T gateway-a python - < docs/compose_smoke.py

# 停止但保留容器；start 不会重建环境变量/镜像
docker compose stop
docker compose start

# 配置/镜像变更后应重新创建容器
docker compose up -d --build --wait --wait-timeout 120
# 清理演示部署（默认 Redis 没有持久化）
docker compose down --volumes --remove-orphans
```

`/healthz` 验证当前协调存储与实例租约，不会主动调用所有后端；后端故障需通过业务探针和日志定位。不要把健康检查误认为端到端业务成功。默认网关关闭含 session query string 的 HTTP access log；仍应避免将敏感业务输出写入共享日志。

### 2.3 配置与镜像更新

Dockerfile 将 `src/` 和 `examples/` 打入镜像，因此编辑宿主机 JSON 后必须重建，才会影响默认部署。替换真实后端时，先修改 gateway A/B 的同一套工具/策略/后端配置，再执行 `docker compose up --build --wait`。多副本网关必须拥有不同 `instance_id`、相同 Redis 与 `redis_prefix`。

若使用只读配置挂载，可在 Compose override 中把受控目录挂载到 `/config:ro`，并将 command 改为 `mcp-gateway --config /config/gateway-a.json --host 0.0.0.0 --port 8080`（B 指向其唯一实例配置）；挂载的 OpenAPI 相对文件须同时可读。入口和后端密钥仍由环境变量提供。

### 2.4 升级与回滚

以下用于**本地参考部署**，不是零停机升级保证。先保存当前源代码版本、Compose 配置、环境变量引用与数据库运维方案；不可把 token 复制到公开 issue。

```bash
# 升级前给当前镜像留回滚标签；首次部署无旧镜像时无需此步
docker image tag paper-mcp-gateway:local paper-mcp-gateway:rollback
# 然后检出经过审核的新提交；构建、重新创建并运行业务冒烟
docker compose up -d --build --wait --wait-timeout 120
docker compose exec -T gateway-a python - < docs/compose_smoke.py

# 若验收失败：先恢复原来的 Compose/配置版本，然后使用旧镜像重建容器
docker image tag paper-mcp-gateway:rollback paper-mcp-gateway:local
docker compose up -d --no-build --force-recreate --wait --wait-timeout 120
docker compose exec -T gateway-a python - < docs/compose_smoke.py
```

容器替换会关闭进程持有的 SSE 连接，相关旧会话不保证保留；客户端应支持重新 initialize。Streamable HTTP 已覆盖初始化实例退出后的续用，但这不等于任意配置/版本升级均向后兼容。Redis schema/策略变更的兼容性需要单独验证，不能依赖镜像回滚恢复旧后端副作用。生产发布应固定镜像 digest、保留不可变版本标签，并预先演练回滚。

### 2.5 生产部署必须补齐的条件

1. **TLS 与入口**：在受控反向代理/LB 后终止 TLS，公网只暴露 HTTPS；保留 Authorization、MCP session/version 头，并为 SSE 禁用代理缓冲、设置合理 idle timeout。不要直接把演示后端暴露公网。
2. **Redis**：默认 Compose 为临时演示，关闭持久化。生产按需求配置 ACL/认证、TLS（`rediss://`）、持久卷/AOF、容量和高可用，并限制仅网关可访问。持久化会话 metadata 也不能恢复已经丢失的 TCP/SSE 通道。
3. **网络与凭证**：限制 egress、配置可信后端 allowlist 与最小权限凭证；入口 token 与各后端 token 分开。Redis URL 如含密码，应放入受保护配置，不要提交仓库。
4. **容量与故障**：设置请求体、并发、SSE 连接数和超时；部署日志/审计/监控，验证 Redis、owner、后端异常和长连接清理。
5. **部署范围**：示例没有 TLS 代理、Redis HA、生产 secret manager、服务层 hash-ring 或灰度发布系统；它们不是靠开启 Compose 就自动获得的能力。

## 3. 本地开发快速开始

要求：**Python 3.12+**；运行 Redis 系统集成测试还需 `redis-server`。开发和 CI 的锁定环境使用 Python 3.12。

```bash
git clone https://github.com/liuzongquan/mcp_gateway.git
cd mcp_gateway
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r requirements.lock
python -m pip install --no-deps --no-build-isolation -e .
python -m pip check

# 生成本地演示凭证；请勿提交到仓库或复用为生产凭证
export GATEWAY_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
```

终端 A（启动演示后端，无外部副作用）：

```bash
source .venv/bin/activate
python -m uvicorn examples.demo_backend:app --host 127.0.0.1 --port 8081
```

终端 B（使用相同 `GATEWAY_TOKEN` 环境变量）：

```bash
source .venv/bin/activate
mcp-gateway --config examples/gateway-local.json --host 127.0.0.1 --port 8080
```

终端 C（同样设置 `GATEWAY_TOKEN`），用官方 MCP SDK 完成握手、搜索与调用：

```python
import asyncio
import os

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

async def main():
    headers = {"Authorization": "Bearer " + os.environ["GATEWAY_TOKEN"]}
    async with httpx.AsyncClient(headers=headers, trust_env=False) as http:
        async with streamable_http_client(
            "http://127.0.0.1:8080/mcp", http_client=http
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                print(await session.list_tools())
                print(await session.call_tool(
                    "gateway.search", {"query": "echo a message", "top_k": 2}
                ))
                print(await session.call_tool("demo__echo", {"message": "你好 MCP"}))
                print(await session.call_tool("demo__increment", {"amount": 1}))

asyncio.run(main())
```

`gateway-local.json` 不配置 Redis，仅适用于单进程开发。多实例不能各自使用独立内存存储。

<a id="configuration"></a>

## 4. 配置

配置文件为 JSON；未知字段会被拒绝。CLI 必须指定 `--config PATH`，或设置 `MCP_GATEWAY_CONFIG` 环境变量；没有隐式匿名启动模式。示例见 [`examples/gateway-local.json`](examples/gateway-local.json)、[`gateway-a.json`](examples/gateway-a.json)、[`gateway-b.json`](examples/gateway-b.json)。

### 4.1 身份与权限

```json
{
  "principals": [
    {"subject": "alice", "token_env": "ALICE_TOKEN", "allow": ["orders__get*", "demo__echo"]},
    {"subject": "operator", "token_env": "OPERATOR_TOKEN", "allow": ["*"]}
  ],
  "list_mode": "search_only",
  "allowed_origins": ["https://your-approved-client.example"]
}
```

这只是配置片段，需合并到完整配置。`token_env` 指向真实凭证所在环境变量；未设置、太短或重复凭证导致启动失败。`allow` 使用大小写敏感通配匹配；未匹配的普通工具默认拒绝。`gateway.search` 是所有已认证主体可见的元工具，但其结果只包含该主体有权使用的工具。

`list_mode="all"`（默认）列出全部已授权工具和搜索元工具；`"search_only"` 只列出搜索元工具，适合希望避免将完整目录注入上下文的客户端。该模式不会赋予额外调用权限。

`allowed_origins=[]` 默认拒绝带未批准 `Origin` 的请求；原生 SDK 通常不发送该头。它不是完整浏览器 CORS 托管方案，不应使用通配开放来回避认证。

### 4.2 原生 MCP 后端

```json
{
  "name": "orders",
  "url": "https://your-backend.example/mcp",
  "transport": "streamable-http",
  "headers_env": {"Authorization": "ORDERS_BACKEND_AUTH"},
  "identity_headers_env": {
    "alice": {"Authorization": "ALICE_ORDERS_AUTH"}
  },
  "tools": [{
    "name": "getOrder",
    "description": "Read an order by its identifier",
    "inputSchema": {
      "type": "object",
      "properties": {"orderId": {"type": "string"}},
      "required": ["orderId"]
    }
  }]
}
```

加入 `mcp_backends` 数组。工具对外名为 `orders__getOrder`，转发时恢复后端名称。工具目录由管理员显式配置，便于在第一次调用前完成发现与检索；当前版本不自动爬取或持续刷新后端目录。

`transport` 也可为 `"sse"`，此时 URL 指向后端 SSE GET 地址。`ORDERS_BACKEND_AUTH` 的内容为完整 HTTP 头值，例如 `Bearer ...`；不是入口用户 token 的自动透传。主体专属头覆盖共享头，实际秘密不写进示例配置。

### 4.3 OpenAPI 服务

加入完整配置的 `openapi_services`：

```json
{
  "name": "inventory",
  "document": "inventory.openapi.json",
  "base_url": "https://your-inventory.example",
  "headers_env": {"X-API-Key": "INVENTORY_BACKEND_KEY"}
}
```

`document` 可为内嵌 OpenAPI 对象或本地 **JSON** 文件；相对路径以网关配置文件所在目录为基准。`base_url` 显式指定时覆盖规范的服务器地址，否则使用受支持的 `servers` 声明。

支持的请求映射：

- path/query/header 的 primitive 和 primitive-array 参数；query `form`、path/header `simple`。
- JSON 请求体映射为工具参数 `body`，并进行 Schema 验证。
- 本地、无环 `$ref`；OpenAPI 3.0 nullable 的受支持转换；3.1 JSON Schema 子集。
- 一对一 operation→tool；冲突名称、未知 Schema 语义、模糊参数映射在挂载期拒绝。

不支持的输入会明确失败，而不是近似序列化：远程/循环引用、cookie 参数、deepObject/matrix/label 等样式、multipart/form、callbacks、动态服务器模板等。规范内 `securitySchemes` 不会自动部署 OAuth：后端认证由配置注入。完整边界以 [`openapi.py`](src/mcp_gateway/openapi.py) 和对应测试为准。

### 4.4 接入真实 dense embedding

不配置 `embedding` 时使用无需网络/模型下载的确定性词法哈希向量。若要使用真实语义编码器，增加：

```json
{
  "embedding": {
    "endpoint": "https://your-embedding-service.example/v1/embeddings",
    "model": "your-pinned-embedding-model",
    "api_key_env": "EMBEDDING_API_KEY"
  }
}
```

此适配器使用 OpenAI-compatible JSON 形状：POST `model`、`input`、`encoding_format=float`，读取 `data[].index/embedding`。URL 是**完整 endpoint**。模型/服务由部署者提供，不会自动购买或下载。无认证服务可省略 `api_key_env`。

适配器校验数量、索引、维数、有限值与非零向量；调用失败不会悄悄退回离线模式。工具目录文本和搜索文本会发送给配置的服务，启用前需确认其数据处理权限。本项目验证适配器协议行为，不把桩服务测试算作真实模型效果测试。

### 4.5 运行参数

| 字段 | 默认值 | 作用 |
|---|---:|---|
| `instance_id` | 随机 UUID | 同一 Redis 协调域内每个运行实例应唯一 |
| `redis_url` | `null` | `null` 为单进程内存模式；多实例须配置同一 Redis |
| `redis_prefix` | `mcpgw:` | 不同部署使用不同前缀，避免协调域混用 |
| `session_ttl` | 1800 秒 | 会话有效期 |
| `request_timeout` | 30 秒 | 工具/跨实例请求超时上限 |
| `connect_timeout` | 15 秒 | 后端连接初始化等待 |
| `lease_ttl` | 10 秒 | 分布式 owner 存活租约 |
| `max_request_bytes` | 1048576 | 入口请求体大小上限 |
| `max_concurrent_requests` | 256 | 单实例并发工作上限 |
| `max_sse_connections` | 1024 | 单实例前端 SSE 连接数上限 |

所有实例须部署一致的工具目录、策略和模型配置。Redis 同步会话协调状态，**不自动同步配置文件**。

## 5. 测试

```bash
# Ubuntu / Debian；其他平台安装对应的 Redis 服务端二进制
sudo apt-get update && sudo apt-get install -y redis-server
# 如不在 PATH：export REDIS_SERVER=/absolute/path/to/redis-server

ruff check .
pytest -q -m 'not functional and not integration'  # 单元测试
pytest -q -m 'functional and not integration'      # 功能测试
pytest -q -m integration                         # 系统集成
pytest -q --cov=mcp_gateway --cov-report=term-missing
```

Redis 测试使用独立临时目录和端口启动真实进程，不使用 fakeredis；缺少可执行文件应失败而不是静默跳过。集成测试使用真实 TCP 和官方 MCP SDK，并覆盖前后端两种传输、后端连接复用、两网关会话连续性与跨实例返回通道等。测试类别与具体覆盖见 [docs/testing.md](docs/testing.md)。

GitHub Actions 会分别执行测试类别、保存 JUnit/coverage 报告，并通过 Compose 验证真实双网关拓扑。**本地通过、容器通过、远程 CI 通过是三个不同结论**，以实际运行记录为准。

## 6. 项目结构

```text
src/mcp_gateway/       网关、认证、检索、OpenAPI、会话和协调实现
examples/             SDK 演示后端与本地/双实例配置
tests/                单元、功能、独立协议与系统集成测试
docs/architecture.md  处理流程、会话模型、安全与失败边界
docs/paper-traceability.md  逐项论文映射与未复现内容
docs/testing.md       可复现命令、验证记录与证据界限
compose.yaml          双网关 + Redis + 有状态演示后端
.github/workflows/ci.yml   测试与容器冒烟 CI
requirements.lock     带哈希的 Python 依赖锁
```

## 7. 生产使用前

本项目是可审计参考实现，不应不加评估地作为互联网生产网关部署。至少需要：TLS、入口与后端网络隔离、Redis 身份认证/TLS/高可用、最小权限凭证管理和轮换、流量与资源限额、审计、监控、负载/故障演练及依赖漏洞审查。不要把管理配置、规范或后端 URL 交给不可信调用者。

Pub/Sub 不是持久任务队列；超时可能发生在后端已执行操作之后。本项目不提供 exactly-once，也不应自动重放具有副作用的调用。legacy SSE 的 owner 进程失效后需要重建会话，不能声称 SSE 无损迁移；Streamable HTTP 通过共享 backend session 允许接收实例直接转发，支持初始化实例退出后的续用。详见 [架构与安全边界](docs/architecture.md)。

## 8. 参考与归属

- 论文：[DOI](https://doi.org/10.1145/3828523.3857007)；[作者提供的 PDF](https://inno-open.oss-accelerate.aliyuncs.com/open/paper/atc26-final25.pdf)。仓库仅引用和独立实现，**不再分发论文 PDF**。
- MCP 官方规范：[生命周期](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle)、[传输](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)、[工具](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)。
- SDK：[modelcontextprotocol/python-sdk](https://github.com/modelcontextprotocol/python-sdk)。

本仓库不代表论文作者、Alibaba Cloud 或 MCP 官方背书。
