"""SQLite 异步持久化帮助类。

- WAL + NORMAL 同步：提交在崩溃/断电（本题保证卷完整）下仍可恢复；
- 每个角色独立的数据库文件；
- `tx()` 使用 BEGIN IMMEDIATE，在本地事务内加写锁，保证站点本地
  “校验版本 -> 写准备记录 -> 锁住授权”原子完成；
- 站点保留每次提交后的授权旧修订（auth_revisions），协调器可按已公布
  快照指定的修订号取回四项授权。
"""
from __future__ import annotations

import asyncio
import os

import aiosqlite
from contextlib import asynccontextmanager


class DB:
    def __init__(self, path: str):
        self.path = path
        self._conn: aiosqlite.Connection | None = None
        # 单连接上写事务必须串行，避免并发协程交错 BEGIN
        self._tx_lock = asyncio.Lock()

    async def connect(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA busy_timeout=5000")
        await conn.execute("PRAGMA foreign_keys=ON")
        self._conn = conn

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "DB not connected"
        return self._conn

    @asynccontextmanager
    async def tx(self):
        """串行化的本地写事务。

        BEGIN IMMEDIATE 立即获取保留锁，使两个并发 prepare 在同一
        授权上必然串行，第二个会看到已存在的准备锁并得到 409。
        连接级异步锁保证同一进程内的并发协程不会交错开启事务。
        """
        conn = self.conn
        async with self._tx_lock:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                await conn.execute("COMMIT")
            except Exception:
                await conn.execute("ROLLBACK")
                raise

    async def execute(self, sql: str, params: tuple = ()):
        cur = await self.conn.execute(sql, params)
        await self.conn.commit()
        return cur

    async def executescript(self, script: str) -> None:
        await self.conn.executescript(script)
        await self.conn.commit()

    async def query_one(self, sql: str, params: tuple = ()) -> aiosqlite.Row | None:
        async with self.conn.execute(sql, params) as cur:
            return await cur.fetchone()

    async def query_all(self, sql: str, params: tuple = ()) -> list[aiosqlite.Row]:
        async with self.conn.execute(sql, params) as cur:
            return list(await cur.fetchall())
