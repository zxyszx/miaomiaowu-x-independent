# 妙妙屋 X 独立增量版本

本项目基于公开妙妙屋工程结构逐步复用，维护为独立增量测试版本。原版对照项目见 [zxyszx/miaomiaowu](https://github.com/zxyszx/miaomiaowu)，该仓库 fork 自上游 [iluobei/miaomiaowu](https://github.com/iluobei/miaomiaowu)。本仓库不分发原版二进制、不替代原版授权，也不把原版功能列表当作当前实现。

## 功能特性

当前已验证：

- 独立 `/health` 健康检查。
- `/manage/servers`、`/manage/nodes`、`/manage/plans`、`/manage/bandwidth` 四个管理页面及其当前 API。
- `127.0.0.1:45300` loopback 服务、外置 data、Bearer token 内存使用和 systemd 低权限运行。

目标/待接入：完整原版 UI 与 dashboard、逐页原版对照、真实登录接线、网页更新、探针/测速完整产品流程、自动 Agent、生产部署。原版的探针和完整业务能力仅作为后续目标，未在此冒充已实现。

## 安装部署

当前源码构建为 `independent-preview-20261108.1`，包含显式 IPv4 监听及固定浏览器 Origin 参数。随包安装器仍为 `install-047.1`，固定安装旧构建 `independent-preview-20261107.3`；下面的一键入口目前仍只提供旧构建的 loopback 安装。新构建的直接 IP 安装入口尚未发布，不要对已安装系统重复执行首次安装。

Linux 一键入口会自动检测架构、apt/dnf/yum 和运行中的 systemd，准备独立 Python 3.12，不修改系统 Python。Debian 12 ARM64 是当前真实安装基线；其他发行版和 x86_64 仍待实测。OpenRC、Docker、Compose、Postgres 和无 systemd 容器部署未实现，检测到时会拒绝。

root 终端执行：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

普通用户可将末尾 `bash` 改为 `sudo bash`；若入口环境只有 wget：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

代码和 runtime 位于 `/opt/miaomiaowu-x-independent`，外置 data 位于 `/var/lib/miaomiaowu-x-independent`，默认服务名为 `miaomiaowu-x-independent.service`，只监听 `127.0.0.1:45300`。通过 SSH 访问：

```sh
ssh -N -L 45300:127.0.0.1:45300 user@server
```

安装完成后，在服务器 root 终端执行 `cat /var/lib/miaomiaowu-x-independent/admin-token`，再将 token 手工填入页面内存；普通用户可在命令前加 `sudo`。不要把 token 发到聊天、反馈或日志。

## 更新与卸载

网页更新、自动升级和卸载入口当前未实现。重复首次安装会拒绝。已安装用户应等待经过验收的升级包和具体切换步骤：停服后备份 data，将核验的新构建放入新的代码目录，修改 systemd 的代码入口并继续使用原外置 data；保留旧代码目录，核对原记录与 token 后再确认切换。当前不提供尚未发布构建的下载或切换命令；不要删除原 data，也不要重跑首次安装代替升级。未知 schema 不保证回退，旧 0.2.7 数据不导入。故障只保留诊断状态，不自动删除或盲目恢复数据。

手动启动新源码时，`run.py` 默认仍监听 `127.0.0.1:45300`。非 loopback 监听须同时显式提供 `--host`、`--port` 和 `--external-origin`，后者为浏览器实际访问的 `http[s]://IP或域名:端口`，不能填写 `0.0.0.0`。服务只信任这个固定 Origin，不从请求 Host 推导信任。HTTP 不提供传输加密，直接访问前需要限制防火墙来源；当前未提供公网 TLS 或完整登录入口。

## 页面与技术特点

当前页面可通过上述 SSH 隧道访问四个 `/manage` 路径。项目使用 Python 标准库运行时、独立 SQLite data 和 loopback HTTP；不提供公网 TLS、完整登录、官方 Demo 账号或截图承诺。原版截图、Demo 账号、Docker/Compose/Postgres 部署说明不属于本项目交付。

## 免责声明

这是自主维护的增量测试版本，不是完整原版妙妙屋，也不是生产部署承诺。请使用独立目录和测试数据，不导入旧库；测试环境按需开放访问端口并限制允许来源。生产、迁移、跨版本回退和公网安全边界尚未验收。

## 许可证、贡献与反馈

LICENSE 保留既有版权和许可文本，NOTICE 说明本导出的来源边界；本项目不新增第三方授权或扩大归属。贡献或反馈请附版本、Linux 发行版/架构、页面、操作、期望与实际结果及脱敏日志，不要附 token、数据库、私钥或 backup。

## 更新日志

- `independent-preview-20261108.1`：源码增加显式 IPv4 监听、固定外部 Origin 和启动器参数透传；随包安装器仍为 `install-047.1`，新构建安装及升级入口待验收发布。
- `independent-preview-20261107.3`：独立四页面测试包、固定 manifest、外置 data 和 SSH loopback 访问。
- `install-047.1`：Linux 架构/包管理器/systemd 检测、固定源码校验、独立 Python 3.12 和低权限首次安装入口。

