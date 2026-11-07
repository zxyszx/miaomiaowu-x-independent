# 妙妙屋 X Linux 安装与047保留数据升级

本页仅说明方式2（Linux一键安装）；方式1 Docker部署（推荐）见[README](README.md)，使用仓库自带Dockerfile/compose.yaml从源码构建，默认宿主8080→容器45300、持久/app/data，Origin填写实际宿主访问源。不要在容器内运行本页systemd安装脚本，也不要把Linux安装data直接挂给容器。

本项目基于公开妙妙屋工程结构逐步复用，维护为独立增量测试版本；原版对照仓库为 [zxyszx/miaomiaowu](https://github.com/zxyszx/miaomiaowu)，fork 自上游 [iluobei/miaomiaowu](https://github.com/iluobei/miaomiaowu)。当前可用范围是 health、四个 manage 页面及对应 API，完整原版 UI、逐页对照、真实登录接线、网页更新和探针/测速完整流程仍待接入。

源码构建为 `independent-preview-20261108.1`，安装入口为 `install-050.1`。无参数首次安装，显式 `update` 升级未修改的047安装；不会将重装当作升级。

安装器参考原版入口组织方式，检测系统、架构、包管理器、服务管理器，下载本项目固定源码，不下载原版官方二进制或guard。实现apt/dnf/yum与systemd下的x86_64/aarch64分支；Debian12 ARM64已在专用VM实测首次安装和047升级，其他组合待测。OpenRC、apk/Alpine、无systemd容器及Docker/Postgres部署未实现，会拒绝。

首次安装自动准备curl/ca-certificates/python3 bootstrap，使用Astral uv0.13.7及独立目录准备Python3.12和最终路径venv，不替换系统Python或修改shell profile；升级沿用原专用runtime。uv/Python是外部HTTPS依赖，不属于项目manifest。参数参考[uv环境变量](https://docs.astral.sh/uv/reference/environment/)。

## 入口和信任范围

在服务器root终端首次安装；已有curl是外层下载前提。普通用户使用sudo进入root终端后操作，确保下述环境变量传给安装脚本：
```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

只有 wget 时使用替代入口：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```
运行代码固定commit `9d5bad5cc9aef2d30fbcabe9450b5a4a022396ec`、build `independent-preview-20261108.1`，manifest SHA256 `4fb124fd89c0c375667dbdc4562c583ce0d87bc967c8c3dfb11e0dee60071236`。归档先核验全部路径、类型、重复项、文件集合、manifest和每文件hash，再以root拥有的普通文件逐项写出，不解包不可信tar metadata。固定runtime快照中的旧install.sh/047元数据只是该已冻结commit内容，实际入口版本是当前下载的install-050.1；不会执行payload里的旧安装器。当前导出的installer_version与入口同步050.1，不将自身manifest用于自引用pin。

默认代码 `/opt/miaomiaowu-x-independent`（root拥有），data `/var/lib/miaomiaowu-x-independent`（mmx-independent拥有，0700），unit `miaomiaowu-x-independent.service`，非root运行且Restart=no。首次安装拒绝已有目录、unit（含disabled）、账号、端口及符号链接冲突；路径限制为安全绝对路径。失败只清理自身临时下载与锁，已产生代码/data/备份保留，非零退出并提示journalctl。

## 访问源与端口

默认监听0.0.0.0:45300，安装结束输出四个完整URL。`MMX_PORT`优先于`PORT`，默认45300；`MMX_HOST`支持0.0.0.0或127.0.0.1，以保证本机127.0.0.1:port健康检查。推荐在首次安装或升级前显式设置浏览器实际访问源（IP或域名，含协议及对应端口）：

```sh
read -r -p '浏览器访问源（含协议和端口）: ' MMX_EXTERNAL_ORIGIN
export MMX_EXTERNAL_ORIGIN
```

不指定时，只有两个独立HTTPS探测结果一致且为公网IPv4才采用；失败给出上述配置和重试命令，不默用0.0.0.0或私网冒充公网。探测是出口地址观察，入站NAT/防火墙仍须自行配置。显式指定私网Origin可用于内网测试，但不声称公网可达。固定Origin不来自请求Host，其他Origin拒绝。

端口调整前先 `export MMX_PORT=45300` 并使用相应Origin；0.0.0.0只表示监听地址，不能作为浏览器Origin。HTTP不加密，请限制防火墙来源；https Origin仅适用于另行配置TLS终止的环境，安装器不部署TLS或反向代理。

可选SSH模式：安装或升级前 `export MMX_HOST=127.0.0.1`，unset MMX_EXTERNAL_ORIGIN后使用默认loopback源。在自己电脑建立同端口转发：

在自己电脑建立 SSH 转发：
```sh
ssh -N -L 45300:127.0.0.1:45300 user@server
```
默认端口下浏览器访问http://127.0.0.1:45300/manage/servers（另有nodes、plans、bandwidth），自定义端口需同步改变转发与URL。管理员token只在自己的root终端 `cat /var/lib/miaomiaowu-x-independent/admin-token` 后填入页面内存，不发聊天或写日志。

## 已安装047版本升级

先按上文设置访问源，然后root执行：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash -s -- update
```

只有wget时，使用README中的wget入口并在bash后加 `-s -- update`。无需手写unit或删除data。

该版本只升级原install-047.1安装的未修改`.3`代码和原unit，要求旧服务active、无drop-in、外置data/marker/账号与路径一致。未知版本、手工改过的代码/unit、已经升级或残留目标构建均会拒绝；自定义MMX_PREFIX/MMX_DATA安装须传入原配置。高级隔离安装可分别设置MMX_UNIT、MMX_SERVICE_USER（严格限制安全名称），普通安装不必设置。

流程：核验固定新构建和旧安装 → 核对数据/schema实现字节未变 → 准备root-owned0755新目录 → 停服并验证SQLite → root-only一致性备份 → 自动切换低权限unit至新代码并保持原data → 本机health、固定Origin授权读取及token保持检查。新代码目录为原PREFIX追加`-build-20261108-1`，专用Python仍沿用原PREFIX；原代码保留，原data不移动。备份目录为原PREFIX追加`-backup-时间-进程号`，目录0700/归档0600，包含凭据，不上传。

切换失败时自动恢复原unit并启动旧服务；不回灌数据库，不删除data或新旧代码。恢复失败会明确提示，须先诊断，不可用fresh重装恢复。此次回退成立的前提是数据实现未变化，不承诺通用跨schema回退。网页更新、自动升级、卸载、旧0.2.7导入和生产验收仍未实现。

状态：`systemctl status miaomiaowu-x-independent.service`；诊断：`journalctl -u miaomiaowu-x-independent.service -n 80 --no-pager`。只在本机读取日志并脱敏反馈。

