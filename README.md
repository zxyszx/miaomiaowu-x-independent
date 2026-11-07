# 妙妙屋 X 独立增量版本

本项目基于公开妙妙屋工程结构逐步复用，维护为独立增量测试版本。原版对照项目见 [zxyszx/miaomiaowu](https://github.com/zxyszx/miaomiaowu)，该仓库 fork 自上游 [iluobei/miaomiaowu](https://github.com/iluobei/miaomiaowu)。本仓库不分发原版二进制、不替代原版授权，也不把原版功能列表当作当前实现。

## 功能特性

当前已验证：

- 独立 `/health` 健康检查。
- `/manage/servers`、`/manage/nodes`、`/manage/plans`、`/manage/bandwidth` 四个管理页面及其当前 API。
- 外置 data、Bearer token 内存使用、systemd低权限运行，以及显式监听地址和固定浏览器Origin。
- Linux首次安装及已安装047版本的保留数据升级入口；Debian12 ARM64已实测。

目标/待接入：完整原版 UI 与 dashboard、逐页原版对照、真实登录接线、网页更新、探针/测速完整产品流程、自动 Agent、生产部署。原版的探针和完整业务能力仅作为后续目标，未在此冒充已实现。

## 安装部署

当前源码构建为 `independent-preview-20261108.1`，安装器为 `install-050.1`。安装器校验固定源码commit、manifest及每个文件，默认监听 `0.0.0.0:45300`，并绑定一个明确的浏览器访问Origin。HTTP未加密，请在测试环境按需放行端口并限制防火墙来源；公网TLS和完整登录尚未提供。

Linux 一键入口会自动检测架构、apt/dnf/yum 和运行中的 systemd，准备独立 Python 3.12，不修改系统 Python。Debian 12 ARM64 是当前真实安装基线；其他发行版和 x86_64 仍待实测。OpenRC、Docker、Compose、Postgres 和无 systemd 容器部署未实现，检测到时会拒绝。

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

当前页面通过安装器输出的访问源打开四个 `/manage` 路径。项目使用Python标准库、独立SQLite data和HTTP；手动运行 `run.py` 仍默认loopback，非回环监听必须显式配置固定external-origin，不能填写0.0.0.0。服务不从请求Host推导信任。不提供公网TLS、完整登录、官方Demo账号或截图承诺。原版截图、Demo账号、Docker/Compose/Postgres部署说明不属于本项目交付。

## 免责声明

这是自主维护的增量测试版本，不是完整原版妙妙屋，也不是生产部署承诺。请使用独立目录和测试数据，不导入旧库；测试环境按需开放访问端口并限制允许来源。生产、迁移、跨版本回退和公网安全边界尚未验收。

## 许可证、贡献与反馈

LICENSE 保留既有版权和许可文本，NOTICE 说明本导出的来源边界；本项目不新增第三方授权或扩大归属。贡献或反馈请附版本、Linux 发行版/架构、页面、操作、期望与实际结果及脱敏日志，不要附 token、数据库、私钥或 backup。

## 更新日志

- `install-050.1` / `independent-preview-20261108.1`：固定新版runtime，默认直接访问、明确Origin与端口配置、047保留data升级及失败旧unit恢复。
- `independent-preview-20261107.3`：独立四页面测试包、固定 manifest、外置 data 和 SSH loopback 访问。
- `install-047.1`：Linux 架构/包管理器/systemd 检测、固定源码校验、独立 Python 3.12 和低权限首次安装入口。

