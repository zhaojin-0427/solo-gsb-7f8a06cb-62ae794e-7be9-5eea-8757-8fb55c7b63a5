"""运行期配置：同一镜像通过 ROLE 环境变量以三种角色启动。"""
import os
from dataclasses import dataclass

ROLE = os.getenv("ROLE", "coordinator")
DATA_DIR = os.getenv("DATA_DIR", "/data")

# 协调器访问两个站点的基础地址（容器网络内，站点不对外暴露端口）
SITE_A_URL = os.getenv("SITE_A_URL", "http://site-a:8000")
SITE_B_URL = os.getenv("SITE_B_URL", "http://site-b:8000")

# 站点 -> (基础 URL, 该站持有的授权 ID)
SITES = {
    "A": {"url": SITE_A_URL, "auths": ("a1", "a2")},
    "B": {"url": SITE_B_URL, "auths": ("b1", "b2")},
}

# 初始归属：A 的 a1/a2 属甲，B 的 b1/b2 属乙
INITIAL_OWNERS = {
    "a1": "甲", "a2": "甲",
    "b1": "乙", "b2": "乙",
}
INITIAL_REVISION = 0

# 网络调用与协调器后台节拍
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "3.0"))
WORKER_INTERVAL = float(os.getenv("WORKER_INTERVAL", "0.1"))
# 决定前（preparing 阶段）的整体超时：任一站拒绝或超时 -> 持久决定撤销
PREPARE_DEADLINE = float(os.getenv("PREPARE_DEADLINE", "10.0"))
# POST /swap 内联等待终态的最长时间
INLINE_WAIT = float(os.getenv("INLINE_WAIT", "6.0"))


def db_path(role: str) -> str:
    return os.path.join(DATA_DIR, f"{role}.sqlite3")


@dataclass
class SiteConfig:
    name: str
    auth_ids: tuple[str, ...]

    @property
    def db_file(self) -> str:
        return db_path(f"site-{self.name.lower()}")
