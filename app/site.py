"""站点服务（A、B 同一镜像、同一代码，仅配置不同）。

两阶段提交在站点一侧的语义，每次互换在本站只涉及一项授权：

prepare（协调器在联系站点前已把请求落盘）：
  本地 IMMEDIATE 事务内完成：
    1. 幂等检查：同一 swap_id 同参数 -> 回送已准备/已提交状态；
       同一 swap_id 异参数 -> 409；
    2. 校验授权当前修订等于期望修订，否则 409（版本冲突）；
    3. 授权未被其他交换的存活准备记录锁住（共享任一授权的并发交换
       最多一项成功），否则 409；
    4. 持久化准备记录（state='prepared'）并锁住授权，回送当前 owner，
       供协调器计算互换后的新 owner。

commit：
  prepared -> 在同一本地事务内写授权新 owner、修订 +1、保留旧修订行、
  准备记录置 committed。committed 重复到达幂等返回。协调器决定提交后
  只会重试提交，永不撤销。

abort：
  prepared -> 置 aborted 并释放锁；aborted/无记录重复到达幂等返回。
  committed 之后到达（不应发生）返回 409。

进程中断后：协调器恢复时按 swap_id 重试 commit/abort 完成清理；
站点自身不做单方面决定。
"""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .config import INITIAL_OWNERS, INITIAL_REVISION, SiteConfig
from .db import DB
from .schemas import SiteAbortRequest, SiteCommitRequest, SitePrepareRequest

_ROLE = os.getenv("ROLE", "site-a")
SITE_NAME = "A" if _ROLE == "site-a" else "B"
CFG = SiteConfig(SITE_NAME, ("a1", "a2") if SITE_NAME == "A" else ("b1", "b2"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_site()
    try:
        yield
    finally:
        await db.close()


app = FastAPI(title=f"Site {SITE_NAME}", lifespan=lifespan)
db = DB(CFG.db_file)


SCHEMA = """
CREATE TABLE IF NOT EXISTS auth_revisions (
    auth_id      TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    owner        TEXT NOT NULL,
    committed_at REAL NOT NULL,
    PRIMARY KEY (auth_id, revision)
);
CREATE TABLE IF NOT EXISTS auth_current (
    auth_id   TEXT PRIMARY KEY,
    revision  INTEGER NOT NULL,
    owner     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS preparations (
    swap_id  TEXT PRIMARY KEY,
    auth_id  TEXT NOT NULL,
    expected_revision INTEGER NOT NULL,
    state    TEXT NOT NULL CHECK (state IN ('prepared','committed','aborted'))
);
"""


async def init_site() -> None:
    await db.connect()
    await db.executescript(SCHEMA)
    now = time.time()
    for auth_id in CFG.auth_ids:
        exists = await db.query_one(
            "SELECT auth_id FROM auth_current WHERE auth_id=?", (auth_id,))
        if exists is None:
            owner = INITIAL_OWNERS[auth_id]
            await db.execute(
                "INSERT INTO auth_current(auth_id, revision, owner)"
                " VALUES(?,?,?)", (auth_id, INITIAL_REVISION, owner))
            await db.execute(
                "INSERT INTO auth_revisions(auth_id, revision, owner,"
                " committed_at) VALUES(?,?,?,?)",
                (auth_id, INITIAL_REVISION, owner, now))


def _conflict(detail: str) -> HTTPException:
    return HTTPException(status_code=409, detail=detail)


@app.get("/health")
async def health():
    return {"site": SITE_NAME, "ok": True}


@app.get("/auths/{auth_id}")
async def get_auth(auth_id: str, revision: int):
    """按协调器已公布快照指定的修订号取回授权；全部旧修订保留。"""
    if auth_id not in CFG.auth_ids:
        raise HTTPException(status_code=404, detail=f"unknown auth {auth_id}")
    row = await db.query_one(
        "SELECT auth_id, revision, owner FROM auth_revisions"
        " WHERE auth_id=? AND revision=?", (auth_id, revision))
    if row is None:
        raise HTTPException(404, detail=f"revision {revision} not found")
    return dict(row)


async def _get_prepare(conn, swap_id: str):
    async with conn.execute(
            "SELECT * FROM preparations WHERE swap_id=?", (swap_id,)) as cur:
        return await cur.fetchone()


async def _get_current(conn, auth_id: str):
    async with conn.execute(
            "SELECT auth_id, revision, owner FROM auth_current"
            " WHERE auth_id=?", (auth_id,)) as cur:
        return await cur.fetchone()


@app.post("/internal/prepare")
async def prepare(req: SitePrepareRequest):
    if req.auth_id not in CFG.auth_ids:
        raise HTTPException(
            422, f"auth {req.auth_id} not held by site {SITE_NAME}")

    async with db.tx() as conn:
        prev = await _get_prepare(conn, req.swap_id)
        if prev is not None:
            same = (prev["auth_id"] == req.auth_id
                    and prev["expected_revision"] == req.expected_revision)
            if not same:
                raise _conflict("swap_id reused with different parameters")
            cur = await _get_current(conn, req.auth_id)
            return {
                "site": SITE_NAME, "swap_id": req.swap_id,
                "state": prev["state"], "auth_id": req.auth_id,
                "revision": cur["revision"], "owner": cur["owner"],
            }

        cur = await _get_current(conn, req.auth_id)
        if cur["revision"] != req.expected_revision:
            raise _conflict(
                f"{req.auth_id}: expected revision {req.expected_revision},"
                f" current {cur['revision']}")

        # 该授权已被其他交换的存活准备锁住 -> 拒绝
        locker = await conn.execute_fetchall(
            "SELECT swap_id FROM preparations"
            " WHERE state='prepared' AND auth_id=?", (req.auth_id,))
        if locker:
            raise _conflict(f"auth locked by swap {locker[0]['swap_id']}")

        await conn.execute(
            "INSERT INTO preparations(swap_id, auth_id, expected_revision,"
            " state) VALUES(?,?,?,'prepared')",
            (req.swap_id, req.auth_id, req.expected_revision))
        return {
            "site": SITE_NAME, "swap_id": req.swap_id, "state": "prepared",
            "auth_id": req.auth_id, "revision": cur["revision"],
            "owner": cur["owner"],
        }


@app.post("/internal/commit")
async def commit(req: SiteCommitRequest):
    async with db.tx() as conn:
        prev = await _get_prepare(conn, req.swap_id)
        if prev is None:
            # 协调器决定提交后只会重试提交；无记录说明本站 prepare 未落盘，
            # 不可调和的不一致（在本协议中不会发生：决定提交前两站已持久准备）。
            raise _conflict("no prepared swap")
        if prev["state"] == "committed":
            return {"site": SITE_NAME, "swap_id": req.swap_id,
                    "state": "committed"}
        if prev["state"] == "aborted":
            raise _conflict("swap was aborted")

        cur = await _get_current(conn, prev["auth_id"])
        # 提交前最后校验：修订仍是 prepare 时锁定的版本
        if cur["revision"] != prev["expected_revision"]:
            raise _conflict("revision changed between prepare and commit")
        now = time.time()
        new_rev = prev["expected_revision"] + 1
        await conn.execute(
            "UPDATE auth_current SET owner=?, revision=? WHERE auth_id=?",
            (req.new_owner, new_rev, prev["auth_id"]))
        await conn.execute(
            "INSERT INTO auth_revisions(auth_id, revision, owner,"
            " committed_at) VALUES(?,?,?,?)",
            (prev["auth_id"], new_rev, req.new_owner, now))
        await conn.execute(
            "UPDATE preparations SET state='committed' WHERE swap_id=?",
            (req.swap_id,))
        return {"site": SITE_NAME, "swap_id": req.swap_id,
                "state": "committed", "auth_id": prev["auth_id"],
                "revision": new_rev, "owner": req.new_owner}


@app.post("/internal/abort")
async def abort(req: SiteAbortRequest):
    async with db.tx() as conn:
        prev = await _get_prepare(conn, req.swap_id)
        if prev is None:
            # 幂等：prepare 记录可能尚未持久化（prepare 写入前进程中断），
            # 视为已撤销。
            return {"site": SITE_NAME, "swap_id": req.swap_id,
                    "state": "aborted"}
        if prev["state"] == "committed":
            # 决定提交后不允许撤销
            raise _conflict("swap already committed")
        await conn.execute(
            "UPDATE preparations SET state='aborted' WHERE swap_id=?",
            (req.swap_id,))
        return {"site": SITE_NAME, "swap_id": req.swap_id,
                "state": "aborted"}
