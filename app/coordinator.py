"""协调器：双站独占授权互换的两阶段提交协调者。

持久化状态机（exchanges 表，任何动作前先落盘）：

  preparing ──两站 prepare 成功──▶ committing ──两站 commit 成功并公布──▶ committed
      │                               │
      │ 任一站拒绝(409) / 超时到期     │ B 暂停：A 先提交完成并停留，
      ▼                               │ 解除暂停后提交 B 并公布
  aborted（持久撤销决定，abort 消息幂等重试，直至两站清理完成）

关键保证：
* 联系站点之前已持久记录请求（POST /swap 在 IMMEDIATE 事务内插入 preparing）；
* 两站都同意 prepare 之后，才在本地事务里持久“提交决定”（committing）；
  决定提交后无论任何超时/崩溃，只会重试 commit，永不撤销；
* 决定之前被拒绝或超时，持久写入“撤销决定”，随后幂等重试 abort，
  重启后继续清理两站的 prepare；
* 站点按 swap_id（=request_id）幂等处理消息，协调器重发安全；
  重启后后台 worker 自动续办所有未决交换；
* 快照只在两站均提交后公布；GET /state 严格按已公布修订号从两站取四项
  授权，任一站不可达 -> 503；单站已提交而另一站未提交时仍返回旧快照；
* pause_intents 持久登记“暂停 B 提交”的意图，可在交换创建前预先登记，
  从而确定性地让 A 先提交、B 挂起；暂停中的交换对外为 pending；
  不相交授权的交换照常推进。
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from . import config
from .db import DB
from .schemas import (
    SiteAbortRequest, SiteCommitRequest, SitePrepareRequest, SwapRequest,
)

db = DB(config.db_path("coordinator"))
http: httpx.AsyncClient | None = None
worker_task: asyncio.Task | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS exchanges (
    request_id      TEXT PRIMARY KEY,
    side_a_auth     TEXT NOT NULL,
    side_b_auth     TEXT NOT NULL,
    expected_rev_a  INTEGER NOT NULL,
    expected_rev_b  INTEGER NOT NULL,
    state           TEXT NOT NULL CHECK (state IN
                        ('preparing','committing','committed','aborted')),
    owner_a         TEXT,
    owner_b         TEXT,
    a_committed     INTEGER NOT NULL DEFAULT 0,
    b_committed     INTEGER NOT NULL DEFAULT 0,
    a_aborted       INTEGER NOT NULL DEFAULT 0,
    b_aborted       INTEGER NOT NULL DEFAULT 0,
    published       INTEGER NOT NULL DEFAULT 0,
    fail_reason     TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    committed_at    REAL
);
CREATE TABLE IF NOT EXISTS pause_intents (
    request_id TEXT PRIMARY KEY,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot (
    auth_id    TEXT PRIMARY KEY,
    revision   INTEGER NOT NULL,
    updated_at REAL NOT NULL
);
"""

ALL_AUTHS = ("a1", "a2", "b1", "b2")


async def init_coordinator() -> None:
    await db.connect()
    await db.executescript(SCHEMA)
    now = time.time()
    for auth_id in ALL_AUTHS:
        await db.execute(
            "INSERT OR IGNORE INTO snapshot(auth_id, revision, updated_at)"
            " VALUES(?,?,?)", (auth_id, config.INITIAL_REVISION, now))


# ---------------------------------------------------------------------------
# 状态工具
# ---------------------------------------------------------------------------

async def pause_active(request_id: str) -> bool:
    row = await db.query_one(
        "SELECT 1 FROM pause_intents WHERE request_id=?", (request_id,))
    return row is not None


def public_state(d: dict, paused: bool) -> str:
    if d["state"] == "committed":
        return "committed"
    if d["state"] == "aborted":
        return "aborted"
    # preparing / committing：B 提交被暂停挂起时对外为 pending
    return "pending" if paused else d["state"]


async def status_payload(request_id: str) -> dict | None:
    ex = await db.query_one(
        "SELECT * FROM exchanges WHERE request_id=?", (request_id,))
    if ex is None:
        intent = await db.query_one(
            "SELECT 1 FROM pause_intents WHERE request_id=?", (request_id,))
        if intent is None:
            return None
        return {"request_id": request_id, "state": "pending",
                "paused": True, "committed": False}
    d = dict(ex)
    paused = await pause_active(request_id)
    return {
        "request_id": request_id,
        "state": public_state(d, paused),
        "paused": paused,
        "committed": d["state"] == "committed",
        "published": bool(d["published"]),
        "a_committed": bool(d["a_committed"]),
        "b_committed": bool(d["b_committed"]),
        "side_a_auth": d["side_a_auth"], "side_b_auth": d["side_b_auth"],
        "expected_rev_a": d["expected_rev_a"],
        "expected_rev_b": d["expected_rev_b"],
        "fail_reason": d["fail_reason"],
    }


async def save(rid: str, **fields) -> None:
    fields["updated_at"] = time.time()
    fields["request_id"] = rid
    sets = ", ".join(f"{k}=:{k}" for k in fields)
    await db.execute(
        f"UPDATE exchanges SET {sets} WHERE request_id=:request_id", fields)


# ---------------------------------------------------------------------------
# 站点 HTTP 调用（站点只能经 HTTP 访问）
# ---------------------------------------------------------------------------

class SiteUnavailable(Exception):
    pass


async def _post(url: str, payload: dict) -> httpx.Response:
    assert http is not None
    try:
        return await http.post(url, json=payload, timeout=config.HTTP_TIMEOUT)
    except httpx.HTTPError as e:
        raise SiteUnavailable(str(e)) from e


async def site_prepare(side: str, swap_id: str, auth_id: str,
                       revision: int) -> dict:
    base = config.SITES[side]["url"]
    r = await _post(f"{base}/internal/prepare", SitePrepareRequest(
        swap_id=swap_id, auth_id=auth_id,
        expected_revision=revision).model_dump())
    if r.status_code != 200:
        raise HTTPException(r.status_code, r.text)
    return r.json()


async def site_commit(side: str, swap_id: str, new_owner: str) -> None:
    base = config.SITES[side]["url"]
    r = await _post(f"{base}/internal/commit", SiteCommitRequest(
        swap_id=swap_id, new_owner=new_owner).model_dump())
    if r.status_code == 200:
        return
    # 站点返回 409：已 committed 的幂等重放视为成功，其余下轮再判
    if r.status_code == 409 and "committed" in r.text:
        return
    raise HTTPException(r.status_code, r.text)


async def site_abort(side: str, swap_id: str) -> bool:
    """返回 True 表示该站可视为已撤销；False 表示站点已提交（异常路径）。"""
    base = config.SITES[side]["url"]
    r = await _post(f"{base}/internal/abort", SiteAbortRequest(
        swap_id=swap_id).model_dump())
    if r.status_code == 200:
        return True
    if r.status_code == 409 and "already committed" in r.text:
        return False
    raise HTTPException(r.status_code, r.text)


async def site_get_auth(side: str, auth_id: str, revision: int) -> dict:
    base = config.SITES[side]["url"]
    assert http is not None
    try:
        r = await http.get(f"{base}/auths/{auth_id}",
                           params={"revision": revision},
                           timeout=config.HTTP_TIMEOUT)
    except httpx.HTTPError as e:
        raise SiteUnavailable(str(e)) from e
    if r.status_code != 200:
        raise SiteUnavailable(f"{auth_id} rev {revision}: HTTP {r.status_code}")
    return r.json()


# ---------------------------------------------------------------------------
# 后台 worker：每 tick 为一个可推进的交换执行一步；重启后自动续办
# ---------------------------------------------------------------------------

def _next_action(d: dict, paused: bool) -> str | None:
    state = d["state"]
    if state == "preparing":
        return "prepare"
    if state == "committing":
        if not d["a_committed"]:
            return "commit_a"
        if not d["b_committed"]:
            # B 暂停：A 的提交仍照常完成（上面一步），之后挂起
            return None if paused else "commit_b"
        return None
    if state == "aborted":
        if not d["a_aborted"] or not d["b_aborted"]:
            return "abort"
    return None


async def _decide_abort(rid: str, reason: str) -> None:
    """决定前拒绝或超时：持久写入撤销决定。"""
    await save(rid, state="aborted", fail_reason=reason)


async def _publish(d: dict) -> None:
    """B 也提交成功：与公布快照在同一本地事务内完成。"""
    now = time.time()
    async with db.tx() as conn:
        await conn.execute(
            "UPDATE exchanges SET state='committed', b_committed=1,"
            " published=1, committed_at=?, updated_at=? WHERE request_id=?",
            (now, now, d["request_id"]))
        await conn.execute(
            "UPDATE snapshot SET revision=?, updated_at=? WHERE auth_id=?",
            (d["expected_rev_a"] + 1, now, d["side_a_auth"]))
        await conn.execute(
            "UPDATE snapshot SET revision=?, updated_at=? WHERE auth_id=?",
            (d["expected_rev_b"] + 1, now, d["side_b_auth"]))


async def process_one(d: dict) -> None:
    rid = d["request_id"]
    action = _next_action(d, await pause_active(rid))
    if action is None:
        return

    if action == "prepare":
        # 决定前整体超时：即便一站都没联系上也持久撤销
        if time.time() - d["created_at"] > config.PREPARE_DEADLINE:
            await _decide_abort(rid, "prepare deadline exceeded")
            return
        try:
            pa = await site_prepare("A", rid, d["side_a_auth"],
                                    d["expected_rev_a"])
            pb = await site_prepare("B", rid, d["side_b_auth"],
                                    d["expected_rev_b"])
        except HTTPException as e:
            await _decide_abort(rid, f"site rejected prepare: {e.detail}")
            return
        except SiteUnavailable:
            # 联系不上/超时：留 preparing，下 tick 重试，到期仍失败则撤销
            return
        # owner 互换：A 项新 owner=乙侧当前 owner，B 项新 owner=甲侧当前 owner
        await save(rid, state="committing", owner_a=pb["owner"],
                   owner_b=pa["owner"], fail_reason=None)
        return

    if action == "commit_a":
        try:
            await site_commit("A", rid, d["owner_a"])
        except (SiteUnavailable, HTTPException):
            return  # 决定后超时/暂时冲突：只能重试提交
        await save(rid, a_committed=1)
        return

    if action == "commit_b":
        # 提交阶段再次确认暂停（暂停可能在 A 提交期间到达）
        if await pause_active(rid):
            return
        try:
            await site_commit("B", rid, d["owner_b"])
        except (SiteUnavailable, HTTPException):
            return
        fresh = dict(await db.query_one(
            "SELECT * FROM exchanges WHERE request_id=?", (rid,)))
        await _publish(fresh)
        return

    if action == "abort":
        # 幂等撤销：逐站重发直到两站清理完成（进程重启后继续）
        if not d["a_aborted"]:
            try:
                await site_abort("A", rid)
            except (SiteUnavailable, HTTPException):
                return
            await save(rid, a_aborted=1)
        if not d["b_aborted"]:
            try:
                await site_abort("B", rid)
            except (SiteUnavailable, HTTPException):
                return
            await save(rid, b_aborted=1)


async def worker_loop() -> None:
    while True:
        try:
            rows = await db.query_all(
                "SELECT * FROM exchanges ORDER BY created_at, rowid LIMIT 50")
            for row in rows:
                d = dict(row)
                rid = d["request_id"]
                if _next_action(d, await pause_active(rid)) is not None:
                    try:
                        await process_one(d)
                    except Exception:
                        # 单条交换的意外不杀死 worker
                        pass
                    break  # 每 tick 只推进一步，保持其他交换响应及时
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(config.WORKER_INTERVAL)


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http, worker_task
    await init_coordinator()
    http = httpx.AsyncClient()
    worker_task = asyncio.create_task(worker_loop())
    yield
    worker_task.cancel()
    try:
        await worker_task
    except BaseException:
        pass
    await http.aclose()
    await db.close()


app = FastAPI(title="Swap Coordinator", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"role": "coordinator", "ok": True}


@app.post("/swap")
async def swap(req: SwapRequest):
    """提交互换。request_id 幂等：同键同参返回原交换当前状态，异参 409。"""
    now = time.time()
    async with db.tx() as conn:
        cur = await conn.execute(
            "SELECT * FROM exchanges WHERE request_id=?", (req.request_id,))
        old = await cur.fetchone()
        if old is not None:
            old = dict(old)
            same = (old["side_a_auth"] == req.side_a_auth
                    and old["side_b_auth"] == req.side_b_auth
                    and old["expected_rev_a"] == req.expected_rev_a
                    and old["expected_rev_b"] == req.expected_rev_b)
            if not same:
                raise HTTPException(
                    409, "request_id already used with different parameters")
        else:
            # 联系站点之前，先持久记录请求
            await conn.execute(
                "INSERT INTO exchanges(request_id, side_a_auth, side_b_auth,"
                " expected_rev_a, expected_rev_b, state, created_at,"
                " updated_at) VALUES(:rid,:aa,:ba,:ra,:rb,'preparing',:t,:t)",
                {"rid": req.request_id, "aa": req.side_a_auth,
                 "ba": req.side_b_auth, "ra": req.expected_rev_a,
                 "rb": req.expected_rev_b, "t": now})

    # 内联短轮询，便于 curl 直接看到结果；未决返回 202
    deadline = time.time() + config.INLINE_WAIT
    while True:
        st = await status_payload(req.request_id)
        if st["state"] in ("committed", "aborted") or time.time() > deadline:
            break
        await asyncio.sleep(0.1)
    st = await status_payload(req.request_id)
    if st["state"] == "committed":
        return JSONResponse(st, status_code=200)
    if st["state"] == "aborted":
        # 版本冲突 / 他方锁冲突等决定撤销：409
        return JSONResponse(st, status_code=409)
    return JSONResponse(st, status_code=202)


@app.get("/swap/{request_id}")
async def get_swap(request_id: str):
    st = await status_payload(request_id)
    if st is None:
        raise HTTPException(404, "unknown request_id")
    return st


@app.post("/swaps/{request_id}/pause")
async def pause_b(request_id: str):
    """暂停该交换在 B 站的提交处理（可在交换创建前预先登记）。

    A 站提交不受影响照常完成；B 挂起期间交换对外为 pending；不相交授权
    的交换照常成功。解除暂停后自动完成 B 提交并公布快照。
    """
    now = time.time()
    st = await status_payload(request_id)
    if st is not None and st["state"] in ("committed", "aborted"):
        raise HTTPException(409, f"swap already {st['state']}")
    await db.execute(
        "INSERT OR IGNORE INTO pause_intents(request_id, created_at)"
        " VALUES(?,?)", (request_id, now))
    payload = await status_payload(request_id)
    if payload is None:
        return {"request_id": request_id, "state": "pending",
                "paused": True, "armed": True}
    payload["paused"] = True
    if payload["state"] not in ("committed", "aborted"):
        payload["state"] = "pending"
    return payload


@app.post("/swaps/{request_id}/resume")
async def resume_b(request_id: str):
    """解除暂停：worker 下一 tick 自动完成 B 提交并公布。幂等。"""
    st = await status_payload(request_id)
    if st is None:
        raise HTTPException(404, "unknown request_id and no pause intent")
    await db.execute("DELETE FROM pause_intents WHERE request_id=?",
                     (request_id,))
    if st["state"] in ("committed", "aborted"):
        return st
    deadline = time.time() + config.INLINE_WAIT
    while time.time() < deadline:
        cur = await status_payload(request_id)
        if cur["state"] in ("committed", "aborted"):
            return cur
        await asyncio.sleep(0.1)
    return await status_payload(request_id)


@app.get("/state")
async def get_state():
    """按协调器已公布快照指定的修订，从两站经 HTTP 取回四项授权。

    任一站不可达（或指定修订取不到）-> 503。
    单站已提交而另一站未提交时快照尚未公布，仍返回旧快照。
    """
    snaps = {r["auth_id"]: r["revision"]
             for r in await db.query_all(
                 "SELECT auth_id, revision FROM snapshot")}
    plan = (("A", "a1"), ("A", "a2"), ("B", "b1"), ("B", "b2"))
    results = await asyncio.gather(*[
        site_get_auth(side, auth, snaps.get(auth, config.INITIAL_REVISION))
        for side, auth in plan
    ], return_exceptions=True)
    if any(isinstance(r, Exception) for r in results):
        raise HTTPException(
            503, "one or more sites unavailable; published snapshot not served")
    return {
        "snapshot": {auth: snaps[auth] for _, auth in plan},
        "auths": results,
    }
