# 妙妙屋 X 独立增量版本

本项目基于公开妙妙屋工程结构逐步复用，维护为独立增量测试版本。原版对照项目见 [zxyszx/miaomiaowu](https://github.com/zxyszx/miaomiaowu)，该仓库 fork 自上游 [iluobei/miaomiaowu](https://github.com/iluobei/miaomiaowu)。本仓库不分发原版二进制、不替代原版授权，也不把原版功能列表当作当前实现。

## 功能特性

当前已验证：

- 独立 `/health` 健康检查。
- `/manage/servers`、`/manage/nodes`、`/manage/plans`、`/manage/bandwidth` 四个管理页面及其当前 API。
- 外置 data、Bearer token 内存使用、systemd低权限运行，以及显式监听地址和固定浏览器Origin。
- Linux首次安装及已安装047版本的保留数据升级入口；Debian12 ARM64已实测。
- Docker源码构建和Compose部署，容器重建保持独立data与token。

目标/待接入：完整原版 UI 与 dashboard、逐页原版对照、真实登录接线、网页更新、探针/测速完整产品流程、自动 Agent、生产部署。原版的探针和完整业务能力仅作为后续目标，未在此冒充已实现。

## 安装部署

两种方式任选一种。当前runtime为 `independent-preview-20261108.1`；HTTP未加密，请限制访问来源。完整登录、TLS、网页更新仍未提供。

### 方式 1：Docker 部署（推荐）

需要已安装[Docker Engine](https://docs.docker.com/engine/install/)；使用Compose时还需要Compose插件。本项目尚无公开预构建镜像，请从本仓库构建：

```sh
git clone https://github.com/zxyszx/miaomiaowu-x-independent.git
cd miaomiaowu-x-independent
read -r -p '浏览器访问源（如 http://实际服务器IP:8080）: ' MMX_EXTERNAL_ORIGIN
export MMX_EXTERNAL_ORIGIN
docker build -t miaomiaowu-x-independent:local .
docker run -d --name miaomiaowu-x \
  --read-only --tmpfs /tmp --security-opt no-new-privileges:true \
  -p "${MMX_HOST_PORT:-8080}:45300" \
  -e MMX_EXTERNAL_ORIGIN \
  -v mmx_data:/app/data \
  miaomiaowu-x-independent:local
```

默认宿主8080映射容器45300，浏览器打开安装日志输出的访问源后追加 `/manage/servers`，另有nodes、plans、bandwidth。`MMX_EXTERNAL_ORIGIN`必须是浏览器真实访问源，不得填0.0.0.0；容器端口固定45300，Origin应使用宿主端口8080，二者无需相同。自定义宿主端口时，先 `export MMX_HOST_PORT=8081` 并输入对应Origin。

也可以在同一源码目录、设置Origin后改用Compose（不要与上面的容器同时占用相同端口）：

```sh
docker compose up --build -d
docker compose logs --tail=30
```

读取token：Docker run方式执行 `docker exec --user 10001 miaomiaowu-x cat /app/data/admin-token`；Compose方式执行 `docker compose exec --user 10001 miaomiaowu cat /app/data/admin-token`。只在自己的终端读取并填入页面，不发聊天或日志。

data持久化到命名卷，Compose为项目独立创建卷。容器前台服务以UID/GID10001运行；入口仅初始化空挂载目录，不递归修改旧数据权限。需要bind挂载时，先 `mkdir -p mmx-data` 创建全新空目录，再用 `--mount "type=bind,src=$PWD/mmx-data,dst=/app/data"` 替换 `-v`；不需要手工chown。不要直接挂载Linux安装数据或其他旧库，跨部署迁移未实现。

`docker stop miaomiaowu-x` 或 `docker compose down` 停止服务后保留卷；删除并重建容器时继续使用同一卷即可保持记录和token。不要使用 `down -v` 或删除卷。当前无容器内自动更新、自动镜像发布或通用跨版本数据回退。

### 方式 2：一键安装（Linux）

安装器 `install-050.1` 校验固定源码commit、manifest及每文件hash，默认监听0.0.0.0:45300。它检测架构、apt/dnf/yum及运行中的systemd，准备独立Python3.12，不修改系统Python。Debian12 ARM64已实测，其他发行版与x86_64待实测；OpenRC、Postgres及无systemd环境不支持。容器请使用方式1，不在容器内运行一键脚本。

root 终端执行：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

普通用户先 `sudo -i` 进入root终端，再配置环境变量并执行命令；若入口环境只有wget：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

代码和专用Python位于 `/opt/miaomiaowu-x-independent`，外置data位于 `/var/lib/miaomiaowu-x-independent`，默认服务名为 `miaomiaowu-x-independent.service`。安装结束会输出四个完整管理URL，例如访问源后追加 `/manage/servers`；另有 `/manage/nodes`、`/manage/plans`、`/manage/bandwidth`。

推荐显式指定访问源，尤其使用域名、NAT或多网卡时。在执行首次安装或下方升级命令前，先在root终端输入浏览器实际使用的 `http://服务器IPv4或域名:端口`：

```sh
read -r -p '浏览器访问源（含协议和端口）: ' MMX_EXTERNAL_ORIGIN
export MMX_EXTERNAL_ORIGIN
```

未指定时，安装器仅在两个HTTPS探测结果一致且为公网IPv4时采用该地址；探测失败会停止并给出配置命令，不用私网地址冒充公网。探测得到的是出口IPv4，不代表入站路由或防火墙已放行。访问时使用安装器输出的完整URL，其他Origin会被拒绝。

端口默认45300，`MMX_PORT`优先于`PORT`；指定Origin的端口须与监听端口一致。需要自定义时先 `export MMX_PORT=45300` 并设置对应Origin。只想通过SSH访问时，在安装或升级前 `export MMX_HOST=127.0.0.1`，不指定Origin则使用同端口loopback访问源；然后在自己电脑执行：

```sh
ssh -N -L 45300:127.0.0.1:45300 user@server
```

安装完成后，在服务器 root 终端执行 `cat /var/lib/miaomiaowu-x-independent/admin-token`，再将 token 手工填入页面内存；普通用户可在命令前加 `sudo`。不要把 token 发到聊天、反馈或日志。

## 更新与卸载

已通过047安装器安装 `.3` 的用户，在root终端执行保留数据升级；可先按上文显式设置Origin和端口：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash -s -- update
```

只有wget时：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash -s -- update
```

该入口仅识别未修改的047安装和原unit，不接管其他版本或自定义unit。它先核验新代码，停服后创建root-only一致性data备份，再将新构建放到 `/opt/miaomiaowu-x-independent-build-20261108-1`，自动切换systemd并继续使用原data、token和低权限账号。原代码保留；备份目录会在终端显示。失败会保留诊断并恢复原unit及旧服务，不回灌、不删除data。请检查退出码及日志；旧服务恢复失败时须先处理故障，不要重跑首次安装或删除数据库。重复升级、残留新构建目录及未知安装均会拒绝。

无参数仍是首次安装，遇到已有目录会拒绝。网页更新、自动升级、卸载及通用跨版本回退尚未实现；旧0.2.7数据不导入。本次升级已核对数据实现未变化，不能外推其他schema版本也可回退。

## 页面与技术特点

当前页面通过部署时配置的访问源打开四个 `/manage` 路径。项目使用Python标准库、独立SQLite data和HTTP；手动运行 `run.py` 仍默认loopback，非回环监听必须显式配置固定external-origin，不能填写0.0.0.0。服务不从请求Host推导信任。不提供公网TLS、完整登录、官方Demo账号或截图承诺；Postgres部署未实现。

## 免责声明

这是自主维护的增量测试版本，不是完整原版妙妙屋，也不是生产部署承诺。请使用独立目录和测试数据，不导入旧库；测试环境按需开放访问端口并限制允许来源。生产、迁移、跨版本回退和公网安全边界尚未验收。

## 许可证、贡献与反馈

LICENSE 保留既有版权和许可文本，NOTICE 说明本导出的来源边界；本项目不新增第三方授权或扩大归属。贡献或反馈请附版本、Linux 发行版/架构、页面、操作、期望与实际结果及脱敏日志，不要附 token、数据库、私钥或 backup。

## 更新日志

- `docker-051.1`：源码构建Docker/Compose、宿主8080映射容器45300、空挂载初始化及低权限运行，重建保留data。
- `install-050.1` / `independent-preview-20261108.1`：固定新版runtime，默认直接访问、明确Origin与端口配置、047保留data升级及失败旧unit恢复。
- `independent-preview-20261107.3`：独立四页面测试包、固定 manifest、外置 data 和 SSH loopback 访问。
- `install-047.1`：Linux 架构/包管理器/systemd 检测、固定源码校验、独立 Python 3.12 和低权限首次安装入口。

