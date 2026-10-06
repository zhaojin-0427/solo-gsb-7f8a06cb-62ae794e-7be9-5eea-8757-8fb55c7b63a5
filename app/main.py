"""同一镜像按 ROLE 选择应用：coordinator / site-a / site-b。"""
import os

import uvicorn

ROLE = os.getenv("ROLE", "coordinator")
PORT = int(os.getenv("PORT", "8000"))

if ROLE == "coordinator":
    from .coordinator import app
elif ROLE in ("site-a", "site-b"):
    from .site import app
else:
    raise SystemExit(f"unknown ROLE={ROLE!r}; expect coordinator|site-a|site-b")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
