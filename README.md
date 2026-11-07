# 妙妙屋 X 独立增量测试仓库

这是当前独立项目的增量测试代码仓库，提供真实的 `/health` 和 `/manage/servers`、`/manage/nodes`、`/manage/plans`、`/manage/bandwidth` 页面。它不是完整原版 UI，也未接入网页更新、完整 panel 登录、自动 Agent 执行或生产部署。

## 前置条件

- Debian 12（或同等 POSIX 环境）
- Python 3.12.x；本项目已用 Python 3.12.14 验证
- 代码目录外的独立 data 目录，不能导入旧 0.2.7 数据库

启动：`python3.12 run.py --data '/srv/miaomiaowu-x-independent-data'`。默认只绑定 `127.0.0.1:45300`，通过 SSH 隧道访问；启动器会输出 token 文件路径，不输出 token。

升级必须停止旧进程、保留旧目录、校验新 manifest/hash，并显式复用同一个外置 data。失败时保留旧构建，回退前停止新构建；没有跨版本迁移证据时不把新 schema 数据交给旧构建。

网页更新尚未接入；当前仓库只提供可审查代码树和发布 artifact 的基础。
