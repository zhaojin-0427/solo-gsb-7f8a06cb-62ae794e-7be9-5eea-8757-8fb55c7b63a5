# 双站独占授权互换 API（FastAPI 两阶段提交）

一个协调器 + A/B 两个站点，**同一镜像按 `ROLE` 启动为三种角色**，三者各用
独立的持久化 SQLite（独立卷）。站点之间不直接通信，**站点只能经 HTTP 被协调器
访问**。实现跨站两项独占授权的 owner 原子互换，容忍进程崩溃与消息丢失/重复，
保证卷完整、站点最终恢复时数据一致。

## 启动

前置：Docker + Docker Compose v2。

```bash
docker compose up --build -d
```

一条命令构建镜像并启动三个容器（compose 会等两个站点健康后再启动协调器）：

| 角色        | 容器内监听 | 宿主机端口 | 持久卷             |
|-------------|-----------|-----------|--------------------|
| coordinator | 8000      | **8000**  | `coordinator_data` |
| site-a      | 8000      | 不发布（仅容器网络 `swapnet`） | `site_a_data` |
| site-b      | 8000      | 不发布（仅容器网络 `swapnet`） | `site_b_data` |

客户端只需访问 `http://localhost:8000`。

```bash
docker compose ps
curl -s http://localhost:8000/health
```

## 初始数据

- A 站：`a1`、`a2`，owner 均为 **甲**，revision 均为 **0**
- B 站：`b1`、`b2`，owner 均为 **乙**，revision 均为 **0**

互换成功后：两项授权 owner 互换、revision 各 **+1**。站点保留全部历史修订。

## API（均在协调器 8000 端口）

| 方法 & 路径 | 说明 |
|---|---|
| `POST /swap` | 提交互换（`request_id` 幂等键）。成功 `200`，未决 `202`，版本冲突/参数冲突 `409`，非法输入 `422` |
| `GET /swap/{request_id}` | 查询交换当前状态（`preparing`/`committing`/`pending`/`committed`/`aborted`） |
| `POST /swaps/{request_id}/pause` | 暂停该交换在 **B 站**的提交处理（可在交换创建前预先登记） |
| `POST /swaps/{request_id}/resume` | 解除暂停，自动完成 B 提交并公布；幂等 |
| `GET /state` | 按协调器**已公布快照**指定的修订从两站取回四项授权；任一站不可达返回 `503` |
| `GET /health` | 健康检查 |

## curl 示例

```bash
# 初始状态：a1/a2=甲(rev0)，b1/b2=乙(rev0)
curl -s http://localhost:8000/state | python3 -m json.tool

# 1) 互换 a1 <-> b1，双方期望修订都是 0（请求在联系站点前先落盘）
curl -s -X POST http://localhost:8000/swap \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"sw-1","side_a_auth":"a1","side_b_auth":"b1",
       "expected_rev_a":0,"expected_rev_b":0}'
# 200 committed -> a1=乙(rev1)，b1=甲(rev1)

# 2) 同一 request_id + 相同参数重试：返回原交换当前状态（幂等）
curl -s -X POST http://localhost:8000/swap -H 'Content-Type: application/json' \
  -d '{"request_id":"sw-1","side_a_auth":"a1","side_b_auth":"b1",
       "expected_rev_a":0,"expected_rev_b":0}'

# 同一 request_id 但不同参数 -> 409
# 期望修订与当前不符（a1 已到 rev1，仍传 0）-> 409（交换 aborted，fail_reason 说明）
curl -s -X POST http://localhost:8000/swap -H 'Content-Type: application/json' \
  -d '{"request_id":"sw-bad","side_a_auth":"a1","side_b_auth":"b2",
       "expected_rev_a":0,"expected_rev_b":0}'

# 非法输入（空 request_id / 授权 ID 不属于对应站 / 负修订）-> 422
curl -s -X POST http://localhost:8000/swap -H 'Content-Type: application/json' \
  -d '{"request_id":"","side_a_auth":"b1","side_b_auth":"b1",
       "expected_rev_a":-1,"expected_rev_b":0}'
```

暂停 / 恢复 B 站提交（A 站提交不受影响，照常完成）：

```bash
# 可在提交交换之前就登记暂停意图，便于确定性演示
curl -s -X POST http://localhost:8000/swaps/sw-2/pause

# 提交互换：A 提交完成，B 挂起；接口返回 202，状态为 pending
curl -s -X POST http://localhost:8000/swap \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"sw-2","side_a_auth":"a2","side_b_auth":"b2",
       "expected_rev_a":0,"expected_rev_b":0}'

curl -s http://localhost:8000/swap/sw-2
# {"state":"pending","a_committed":true,"b_committed":false,...}

# 此时 GET /state 仍返回【旧快照】（a2/b2 仍是 rev0），尽管 A 站其实已提交 rev1
curl -s http://localhost:8000/state

# 暂停期间，不相交授权的互换照常成功；
# 共享 a2/b2 任一授权的并发互换最多一项成功，其余 409（aborted）。

# 解除暂停：worker 自动完成 B 提交并公布新快照
curl -s -X POST http://localhost:8000/swaps/sw-2/resume
curl -s http://localhost:8000/state
```

## 一致性与故障语义

两阶段提交状态机（全部状态转移都先持久化）：

```
preparing ──两站 prepare 成功──▶ committing ──两站 commit 成功并公布──▶ committed
    │                                │
    │ 任一站拒绝(409) / 超时到期      │ B 被暂停：A 先提交，解除暂停后提交 B
    ▼                                ▼
aborted（持久撤销决定；abort 幂等重试直到两站清理完成）
```

- **先记录后联系**：`POST /swap` 在本地事务里先把请求以 `preparing` 落盘。
- **站点本地事务**：`prepare` 在 `BEGIN IMMEDIATE` 事务内原子完成
  “校验期望修订 → 检查授权锁 → 持久化准备记录”；准备记录即授权锁，
  共享任一授权的并发 prepare 必然串行，后者得到 409。
- **决定点**：只有 A、B 两站 prepare 都成功，协调器才持久提交决定
  （`committing`）。决定提交后任何超时/崩溃都**只重试 commit，绝不撤销**。
- **决定前失败**：任一站拒绝（409）或 prepare 整体超时
  （`PREPARE_DEADLINE`，默认 10s）→ 持久写入撤销决定，随后对两站幂等重发
  `abort` 清理 prepare；协调器重启后继续清理。
- **消息幂等**：站点按 `swap_id`（即 `request_id`）幂等处理 prepare/commit/
  abort；同键同参数重放返回既有状态，同键异参数 409。
- **重启自动续办**：协调器后台 worker 每个节拍推进一个可推进的交换，
  从 SQLite 读出全部未决交换自动续办；未收到的 commit/abort 会不断重发。
- **快照公布**：仅当 A、B 都提交后，协调器才在同一事务内把两项授权
  公布修订推进到 `期望修订 + 1`。因此“A 已提交、B 未提交（如被暂停）”时
  `GET /state` 仍严格返回旧快照。
- **按修订读取**：站点 `auth_revisions` 表保留每次提交的旧修订，
  `/state` 用快照中的修订号从两站经 HTTP 取回；任一站不可达即 `503`，
  不会给出新旧混合的结果。
- **故障范围**：仅考虑进程中断与消息中断/重复/乱序；持久卷完整，站点最终恢复。

## 本地不使用 Docker 的开发运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

ROLE=site-a DATA_DIR=./data/a PORT=8001 python -m app.main &
ROLE=site-b DATA_DIR=./data/b PORT=8002 python -m app.main &
ROLE=coordinator DATA_DIR=./data/c PORT=8000 \
  SITE_A_URL=http://127.0.0.1:8001 SITE_B_URL=http://127.0.0.1:8002 \
  python -m app.main
```

## 关键环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `ROLE` | `coordinator` | `coordinator` / `site-a` / `site-b` |
| `DATA_DIR` | `/data` | SQLite 所在目录（每角色独立卷） |
| `SITE_A_URL` / `SITE_B_URL` | compose 内地址 | 协调器访问站点的 HTTP 基址 |
| `HTTP_TIMEOUT` | `3.0` | 单次站点 HTTP 调用秒数 |
| `PREPARE_DEADLINE` | `10` | 决定前 prepare 整体超时秒数 |
| `WORKER_INTERVAL` | `0.1` | 后台续办节拍 |
