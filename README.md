# 后厨影像证据服务

本项目接收门店设备事件并维护可追溯的影像片段索引。当前仓库已提供 Gin 服务入口、PostgreSQL 与 Redis 的容器环境，业务包可按领域职责继续拆分。

运行 `docker compose up --build`，访问 `GET /health` 检查进程状态。配置通过环境变量传入，敏感值不得提交。
