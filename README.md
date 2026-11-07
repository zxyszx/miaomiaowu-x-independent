# 妙妙屋X

自主维护的独立增量测试项目。公开来源：[zxyszx/miaomiaowu-x-independent](https://github.com/zxyszx/miaomiaowu-x-independent)。当前提供可实际使用和反馈的基础管理页面与 API，完整原功能与布局仍在逐项补齐。

| 功能 | 当前状态 |
| --- | --- |
| 服务器、节点、套餐、带宽管理 | 四个已挂载的 /manage 页面及对应已实现 API 可测试 |
| /health | 基础服务健康检查，不代表所有业务验收 |
| 完整原版 UI、dashboard | 未完成 |
| 登录页面与真实身份接线 | 未完成，不启用 panel 参数 |
| 网页更新 | 未实现，不能在网页一键升级 |
| 自动 Agent 执行、生产部署 | 本安装入口不启用 |
| 与原版 UI 逐页实测对照 | 尚未完成；安装实测范围见下节 |

## 安装与系统范围

安装器版本 install-047.1；固定运行版本 independent-preview-20261107.3。
实现 Linux x86_64/aarch64 的 apt、dnf、yum 与运行中 systemd 检测；Debian 12 ARM64 是本轮实测基线，其他发行版和 x86_64 尚未实测。OpenRC、无 systemd 容器、Docker 安装未实现，检测不满足时拒绝。需要 root、HTTPS 出网及 GitHub/Astral 下载可达。

在服务器 root 终端使用下列单命令首次安装。外层下载需要已有 `curl`；脚本内补装 curl 无法解决入口下载工具缺失。普通用户可将末尾 `bash` 改为 `sudo bash`：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

若只有 wget，可用替代入口：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

安装器自动准备 uv 和独立 Python 3.12，不修改系统 Python。代码、runtime、venv 在 /opt/miaomiaowu-x-independent；数据在 /var/lib/miaomiaowu-x-independent。代码由 root 拥有，服务以 mmx-independent 低权限用户运行。已有代码/数据/服务/账号、链接路径和 45300 端口占用会拒绝。旧 0.2.7 不覆盖、不导入数据库。

## 访问页面

默认仅监听 127.0.0.1:45300。在自己电脑终端建立同端口 SSH 转发：

```sh
ssh -N -L 45300:127.0.0.1:45300 user@server
```

保持终端运行，再在自己电脑浏览器访问：

- http://127.0.0.1:45300/manage/servers
- http://127.0.0.1:45300/manage/nodes
- http://127.0.0.1:45300/manage/plans
- http://127.0.0.1:45300/manage/bandwidth

在自己的服务器终端执行以下命令读取管理员 token，手工填入页面 Bearer 输入框；凭据只在页面内存中使用，不要发到聊天、反馈或日志：

```sh
cat /var/lib/miaomiaowu-x-independent/admin-token
```

不要放开公网 45300 代替 SSH 转发；这不是公网 TLS 或完整登录验收。

## 服务与数据

下列服务命令及上文 token 命令以 root 执行；普通用户需要时在命令前加 `sudo`。

```sh
systemctl status miaomiaowu-x-independent.service
systemctl stop miaomiaowu-x-independent.service
systemctl start miaomiaowu-x-independent.service
systemctl restart miaomiaowu-x-independent.service
journalctl -u miaomiaowu-x-independent.service -n 80 --no-pager
/opt/miaomiaowu-x-independent/venv/bin/python /opt/miaomiaowu-x-independent/run.py --version
```

异常退出不自动循环重启。失败后保留已生成代码与数据用于诊断，只清理本次临时下载目录；不要盲删数据或反复运行安装器。外置数据必须保留，正式备份须先停服取得一致性。重复首次安装会拒绝；当前没有自动升级/卸载。未来升级必须显式复用同一外置 data，停服、校验新版本并保留旧构建；未知 schema 不承诺回退，也不能导入旧 0.2.7 库。

## 反馈和来源

反馈请附运行版本、系统/架构、页面、操作、期望/实际和脱敏错误；不要附 token、数据库、私钥或备份。详细安装信任边界见 ONE-CLICK-INSTALL.md。
保留 LICENSE 原版权及 NOTICE；SOURCE-VERSION 只是迁移来源，不是当前 build。参考原版 [iluobei/miaomiaowuX](https://github.com/iluobei/miaomiaowuX) 的安装入口组织方式，本项目运行代码来自当前独立项目，不分发官方二进制，也不新增第三方授权。

