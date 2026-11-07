# Linux 自动检测首次安装

安装器 install-047.1 参考原版妙妙屋的入口组织方式（系统、架构、包管理器、服务管理器检测），下载本项目固定源码，不下载原版官方二进制或 guard。实现 apt/dnf/yum + systemd 的 x86_64/aarch64 首次安装；Debian 12 ARM64 已在专用 VM 实测，其他组合待测。识别 OpenRC、apk/Alpine 和无 init 容器后准确拒绝；这些分支和 Docker/Postgres 部署尚未实现，不宣称全平台通过。

用户不需手工安装 uv/Python。入口自动补 curl/ca-certificates/python3 bootstrap，使用 Astral uv 0.13.7 专用安装与 UV_UNMANAGED_INSTALL，随后通过 UV_PYTHON_INSTALL_DIR 准备专用 Python 3.12 和最终路径 venv，不改 /usr/bin/python3 或 shell profile。uv/Python 是外部 HTTPS 下载依赖，不属于本项目源码 manifest；网络不可达则失败并保留诊断状态。官方参数参考 https://docs.astral.sh/uv/reference/environment/ 。

## 入口和信任范围

在服务器 root 终端首次安装使用以下命令；已有 `curl` 是外层下载前提，安装脚本内部补装无法解决入口工具缺失。普通用户可把末尾 `bash` 改为 `sudo bash`：
```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

只有 wget 时使用替代入口：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```
运行代码固定 commit 74fddc380e19b51650e04220e1d8eb815bd5f21e、build independent-preview-20261107.3，manifest SHA-256 固定 f3297df539462c73a09550c9405a5a47d05aeabe0c815fedc68ab69f95c05af9。归档先在内存核验所有路径、类型、重复项、文件集合、manifest 与每文件 hash，通过后才逐文件写出；不用 tar 解包不可信 metadata。

默认代码 /opt/miaomiaowu-x-independent（root 拥有）；外置 data /var/lib/miaomiaowu-x-independent（mmx-independent 拥有，0700）。systemd 服务同名 miaomiaowu-x-independent.service，非 root 运行，Restart=no。固定端口 loopback 45300。已有目标、data、unit（含 disabled）、服务账号、端口和父级链接冲突会拒绝；路径只允许绝对安全 ASCII 目录名，含空格或注入字符拒绝。本次只清理自身 mktemp 下载目录；已产生代码/data/服务保留，失败非零退出且提供 journalctl 诊断。重跑安装器不是恢复方式。

## 使用和恢复

在自己电脑建立 SSH 转发：
```sh
ssh -N -L 45300:127.0.0.1:45300 user@server
```
浏览器访问 http://127.0.0.1:45300/manage/servers（另有 nodes、plans、bandwidth）；这不是完整原版 UI，也未逐页完成原版对照。管理员 token 仅在自己的服务器 root 终端执行 `cat /var/lib/miaomiaowu-x-independent/admin-token` 后手动填入页面内存，不发给聊天或写进日志。普通用户可在命令前加 `sudo`。

服务命令以 root 执行：`systemctl status/stop/start/restart miaomiaowu-x-independent.service`（选择一个动作）；诊断 `journalctl -u miaomiaowu-x-independent.service -n 80 --no-pager`。普通用户可在命令前加 `sudo`。异常不自动重启，不自动回滚或删数据。未来更新必须停服、验证新构建、保留代码和一致性数据备份并复用外置 data；跨 schema 未验证不得盲回滚数据库。旧0.2.7不碰、不导库。网页更新、panel真实登录、Agent和生产验收仍未完成；041/017保持原边界。

