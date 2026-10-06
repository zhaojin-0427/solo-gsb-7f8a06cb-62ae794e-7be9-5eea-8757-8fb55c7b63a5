"""Pydantic 模型：非法输入由 FastAPI 自动返回 422。"""
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

_SITE_AUTH = {"A": {"a1", "a2"}, "B": {"b1", "b2"}}


# ---------- 客户端 -> 协调器 -----------------------------------------------

class SwapRequest(BaseModel):
    request_id: str = Field(min_length=1, max_length=128, description="幂等键")
    side_a_auth: str = Field(description="A 站授权 ID，a1 或 a2")
    side_b_auth: str = Field(description="B 站授权 ID，b1 或 b2")
    expected_rev_a: int = Field(ge=0)
    expected_rev_b: int = Field(ge=0)

    @field_validator("request_id")
    @classmethod
    def _rid_no_whitespace(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("request_id must not be blank")
        return v

    @field_validator("side_a_auth")
    @classmethod
    def _a_auth(cls, v: str) -> str:
        if v not in _SITE_AUTH["A"]:
            raise ValueError("side_a_auth must be one of: a1, a2")
        return v

    @field_validator("side_b_auth")
    @classmethod
    def _b_auth(cls, v: str) -> str:
        if v not in _SITE_AUTH["B"]:
            raise ValueError("side_b_auth must be one of: b1, b2")
        return v


# ---------- 协调器 -> 站点（HTTP，站点只能经 HTTP 访问） --------------------
# 每次互换在每个站点只涉及本站的一项授权；跨站新 owner 由协调器在 commit
# 阶段下发（两站 prepare 后即知道对方当前 owner）。

class SitePrepareRequest(BaseModel):
    swap_id: str
    auth_id: str
    expected_revision: int


class SiteCommitRequest(BaseModel):
    swap_id: str
    new_owner: str = Field(min_length=1, max_length=64)


class SiteAbortRequest(BaseModel):
    swap_id: str
    reason: str = "coordinator decision: abort"
