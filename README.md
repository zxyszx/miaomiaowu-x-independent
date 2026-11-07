# 妙妙屋 X 独立增量版本

基于公开妙妙屋工程逐步复用的独立测试版本。原版对照：[zxyszx/miaomiaowu](https://github.com/zxyszx/miaomiaowu)，其上游为[iluobei/miaomiaowu](https://github.com/iluobei/miaomiaowu)。

当前支持服务器、节点、套餐、带宽四个管理页，以及Docker和Linux部署。完整原版UI、真实登录、网页更新、自动Agent和生产验收仍待完成。当前使用HTTP，请限制访问来源；token只在自己终端读取，不发聊天或日志。

## 安装部署

两种方式任选一种。

### 方式 1：Docker 部署（推荐）

需要Git、[Docker Engine](https://docs.docker.com/engine/install/)和Compose插件。当前没有预构建镜像，直接从源码构建：

```sh
git clone https://github.com/zxyszx/miaomiaowu-x-independent.git
cd miaomiaowu-x-independent
read -r -p '访问地址（http://服务器IP:8080，不加路径或末尾斜杠）: ' MMX_EXTERNAL_ORIGIN
export MMX_EXTERNAL_ORIGIN
docker compose up --build -d
```

浏览器打开 **`http://服务器IP:8080/manage/servers`**，地址须与刚输入的一致。默认宿主8080映射容器45300；不能用0.0.0.0作为访问地址。

读取token，填入页面：

```sh
docker compose exec --user 10001 miaomiaowu cat /app/data/admin-token
```

数据保存在Docker卷。停止时用 `docker compose down`，不要加 `-v`；重建容器继续使用同一卷即可。

<details>
<summary>替代方式：不用Compose，使用docker run</summary>

在上方进入源码目录并设置访问地址后，用以下命令替代Compose启动，不要同时占用同一端口：

```sh
docker build -t miaomiaowu-x-independent:local .
docker run -d --name miaomiaowu-x \
  --read-only --tmpfs /tmp --security-opt no-new-privileges:true \
  -p "${MMX_HOST_PORT:-8080}:45300" \
  -e MMX_EXTERNAL_ORIGIN \
  -v mmx_data:/app/data \
  miaomiaowu-x-independent:local
```

读取token：

```sh
docker exec --user 10001 miaomiaowu-x cat /app/data/admin-token
```

停止使用 `docker stop miaomiaowu-x`，保留 `mmx_data` 卷。自定义端口或bind目录见[详细指南](ONE-CLICK-INSTALL.md)。

</details>

### 方式 2：一键安装（Linux）

需要root权限和正在运行systemd的Linux服务器；Debian12 ARM64已实测。普通用户先 `sudo -i`。以下命令用于首次安装，已有047安装见下方升级：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

浏览器打开安装结束输出的地址，默认形式为 **`http://服务器IP:45300/manage/servers`**。脚本自动探测访问IP；探测失败时会给出配置命令。域名、内网或自定义端口见[详细指南](ONE-CLICK-INSTALL.md)。

读取token，填入页面：

```sh
cat /var/lib/miaomiaowu-x-independent/admin-token
```

## 已安装047版本升级

仅适用于未修改的 `install-047.1` / `.3` 安装及原服务，不是通用更新入口；已升级050的安装不要重复执行。服务器root终端运行：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash -s -- update
```

升级会停服备份并保留原数据和token，无需重装。失败时保留诊断，不要删库或重跑首次安装。配置和恢复边界见[详细指南](ONE-CLICK-INSTALL.md)。

## 更多说明

四个页面：`/manage/servers`、`/manage/nodes`、`/manage/plans`、`/manage/bandwidth`。端口、域名、SSH、数据位置与排错见[部署详细指南](ONE-CLICK-INSTALL.md)。

当前版本：runtime `independent-preview-20261108.1`、Docker `docker-051.1`、Linux安装器 `install-050.1`。许可与来源见[LICENSE](LICENSE)和[NOTICE](NOTICE)；反馈请附版本、操作及脱敏错误，不附数据库、私钥或备份。
