# 测试、证据与可复现性

## 1. 环境与安装

开发基线：Python 3.12；系统集成需要真实 `redis-server` 可执行文件。使用锁文件，避免把当前机器预装包偶然当成项目依赖：

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r requirements.lock
python -m pip install --no-deps --no-build-isolation -e .
python -m pip check
```

`requirements.lock` 锁定 Python 的直接与传递依赖及下载哈希；安装本地项目使用已安装构建依赖，不在隔离环境隐式拉取新版本。`pyproject.toml` 描述兼容范围，锁文件描述被验收的具体组合。

依赖锁**不等于整个环境位级复现**：Docker 的 `python:3.12-slim` 与 Redis tag 未按平台 digest 固定；Ubuntu apt 包随安全更新变化；操作系统/CPU/网络不同也会影响性能。升级依赖后须重新执行全部测试。不要使用现有虚拟环境的 `pip freeze` 无差别收录无关预装包。

CI 使用已核实的固定 release commit：[`checkout v7.0.1`](https://github.com/actions/checkout/releases/tag/v7.0.1)、[`setup-python v7.1.0`](https://github.com/actions/setup-python/releases/tag/v7.1.0)、[`upload-artifact v7.0.2`](https://github.com/actions/upload-artifact/releases/tag/v7.0.2)。版本固定提升可追溯性，不代表它们永久无需安全更新。

## 2. 分层执行

```bash
# Redis 不在 PATH 时设置绝对路径；测试会自行选临时端口和临时目录
export REDIS_SERVER=/absolute/path/to/redis-server
"$REDIS_SERVER" --version

ruff check .
pytest -q -m 'not functional and not integration' --junitxml=reports/unit.xml
pytest -q -m 'functional and not integration' --junitxml=reports/functional.xml
pytest -q -m integration --junitxml=reports/integration.xml

# 完整回归与覆盖率（包括全部类别）
pytest -q --junitxml=reports/all.xml \
  --cov=mcp_gateway --cov-report=term-missing --cov-report=xml:reports/coverage.xml
```

没有 Redis 时，不能把被跳过的测试报告为系统集成通过。相关 fixture 要求二进制存在，否则失败。默认向回环地址绑定临时 Redis；不连接真实生产服务，不修改生产数据库。

### 单元测试

- OpenAPI 编译、3.0/3.1 Schema、参数序列化、local ref、类型和名称碰撞、敏感头与不支持特性拒绝。
- 检索 tokenizer、BM25、RRF 排名/候选上限、cosine 向量验证、离线 provider 和 HTTP embedding 协议桩。
- 权限过滤、主体隔离、会话存储 TTL/原子操作。

### 功能与协议边界测试

独立原始 HTTP/ASGI 协议测试覆盖认证、Origin、JSON-RPC 输入、生命周期、session 绑定、参数与错误回应。ASGI 边界测试不冒充真实 TCP 测试；其作用是快速、精确地验证状态码和协议输入输出。

通过一套网关与受控后端验证正常生命周期、OpenAPI 调用、检索/权限可见性等组合路径。具体测试以 `functional` marker 收集结果为准；测试输出而非目录命名决定该次执行范围。

### 系统集成

`tests/test_sdk_acceptance.py` 使用真实 TCP 监听与官方 `mcp.ClientSession`，覆盖前端/后端 Streamable HTTP、legacy SSE 的四种组合、会话复用、真实 Redis 跨网关协调、返回通道关联与超时不重放等。

这些 SDK acceptance 中的多个 Uvicorn 实例可以在**同一个 Python 测试进程**内运行，不能仅因监听多个端口就称为独立网关 OS 进程。独立进程的集成测试/Compose 单独验证进程隔离；请结合具体测试与实际日志判断覆盖程度。Streamable HTTP 还覆盖初始化实例退出后继续调用，以及显式 DELETE 终止后端 session；legacy SSE owner 失效则明确失败。

健康检查通过不等于业务会话正确；反过来，成功的工具调用也不等于已执行全量压力、故障注入或安全审计。尚未覆盖的论文规模与故障边界详见 [论文映射](paper-traceability.md)。

## 3. Docker 端到端冒烟

```bash
export GATEWAY_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
docker compose config --quiet
docker compose up --build --wait --wait-timeout 120
docker compose exec -T gateway-a python - < docs/compose_smoke.py
docker compose down --volumes --remove-orphans
```

冒烟脚本初始化于 A，在 B 完成 initialized 通知，然后 A/B 交替调用演示计数器，断言得到 `1,2,3,4`，最后通过 B 删除 session。它验证示例部署实际配置、共享 Redis、稳定 Routing-Hash 与跨网关会话续用，不能替代完整 pytest 回归。

## 4. 实际验证记录

下列本地记录于 **2026-10-31（UTC）**完成，最终全量运行期间源码与测试未变更。依据真实执行证据，不根据已编写的测试推断成功。

| 检查 | 记录 |
|---|---|
| 全量 pytest（最终源码冻结） | **320 passed / 0 failed / 0 errors / 0 skipped**，37.23 秒；252 单元/边界、51 功能、17 集成；Python 3.12.14 + lock MCP 1.30.0 + 真实 Redis 7.2.10；证据为该次 `reports/all.xml` 与 `reports/coverage.xml` |
| 覆盖率 | 行覆盖 **1819/2022 = 89.96%**；分支覆盖 **660/810 = 81.48%**；pytest 的合并展示约 88%，三者定义不同，不混用 |
| 安装、构建与静态检查 | 全新环境 `--require-hashes` 安装、`pip check`、wheel 构建、`ruff check .` 均通过 |
| 独立协议/SDK acceptance | 2026-10-31，clean lock 环境 MCP 1.30.0 + Redis 7.2.10，59 passed / 0 skipped；44 functional + 15 integration，18.14 秒；随后已包含在上述最终全量回归 |
| 文档与配置静态检查 | 三份示例配置通过 Pydantic 验证；Compose/Actions YAML 可解析；本地 Markdown 链接全部有效；smoke script Ruff 通过（不等于容器验收） |
| CLI 独立进程部署冒烟 | 2026-10-31，真实 Redis + 演示后端 + A/B 两个网关进程，交替计数 1→2→3→4 和 DELETE 通过；使用全新 clean lock 环境复查通过（Python 3.12.14 / MCP 1.30.0）；另验证 Docker Host `backend:8081` 被允许、不可信 Host 被拒绝 |
| 真实 Redis | 从官方源码构建 Redis 7.2.10；独立 SDK acceptance 与上述 CLI 进程冒烟均实际使用该二进制，而非模拟 Redis |
| Docker / Compose | 本地执行环境没有 Docker 可执行文件，**未在本地运行容器构建/Compose**；CI 配置了独立 job，远程结果须另外核验 |
| 外部真实 embedding 模型 | 未进行模型质量或真实供应商效果验收；HTTP adapter 的受控协议测试不算语义 benchmark |
| 论文生产 benchmark | 未复现；私有数据集、生产平台与精确模型不可得 |

CI 的 `test` job 会独立运行三个 pytest 选择表达式并上传 JUnit/coverage artifact；`compose` job 构建镜像、启动双网关并运行跨网关计数断言。未查看对应提交的 CI 终态前，不应声称“CI 已通过”。

## 5. 失败定位

| 症状 | 先检查 |
|---|---|
| 启动提示凭证缺失 | `token_env` / `headers_env` 引用的环境变量是否已设置；不要把秘密贴入 issue |
| 请求 401 | 入口 `Authorization: Bearer ...` 是否匹配主体配置 |
| 带 Origin 的请求失败 | 是否在 `allowed_origins` 中；反向代理是否意外注入 Origin |
| session 不存在/失效 | 是否发送初始化返回的 `MCP-Session-Id`；是否已删除/过期；是否用了另一部署的 Redis |
| 请求到 B 后 session 不可用 | A/B 是否共享相同 Redis/前缀，且 `instance_id` 唯一；不能混用本地内存模式 |
| owner unavailable | legacy SSE 持有连接的网关是否存活、租约是否有效；SSE 旧会话不自动迁移，应新建 session |
| Redis 集成测试立即失败 | `redis-server` 是否在 PATH，或 `REDIS_SERVER` 是否指向可执行二进制 |
| OpenAPI 挂载失败 | 对照支持的参数样式/Schema/$ref 范围；修正规范而不是静默略过操作 |
| embedding 错误 | endpoint/model/密钥、响应数量/维数/有限值；不会自动降级后伪称真实 dense 成功 |
| 工具调用超时 | 结果可能不确定；先确认后端实际状态，不要盲目重放有副作用操作 |


## 6. 合成目录规模冒烟（不是论文召回率）

```bash
pytest -q -s tests/unit/test_retrieval.py -k synthetic_3000
```

2026-10-31 的一次 clean 环境测量：Python 3.12.14、Linux 6.18.57、x86_64、9 个可见 CPU；3,000 个合成工具，其中 1,500 个对当前主体可见；默认 384 维词法哈希 + exact cosine；10 条合成查询均返回 15 个有权限的工具。该次索引构建约 636.171 ms，检索中位数约 31.432 ms，最大约 39.921 ms。

这些数字是一次小规模、合成数据、特定环境的运行观察，性能会随机器和并行负载变化；测试不以这些计时为固定阈值。它不测真实语义模型、不构成 Recall@15 标注评测，也不复现论文的生产召回率或云规模吞吐。
