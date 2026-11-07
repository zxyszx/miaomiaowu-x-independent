# 部署详细指南

快速安装见[README](README.md)。本页说明地址配置、数据保持和排错；Docker与Linux一键安装是两种独立方式，不要在容器内运行systemd安装脚本。

## 1. 准备访问地址

浏览器使用哪个地址，就配置哪个地址，不加路径或末尾斜杠：

| 部署方式 | 默认浏览器地址 | 服务端口 |
| --- | --- | --- |
| Docker / Compose | `http://服务器IP:8080` | 宿主8080 → 容器45300 |
| Linux一键安装 | `http://服务器IP:45300` | 宿主45300 |

可以使用实际IP或域名，不能填写0.0.0.0。HTTP没有传输加密，请限制端口访问来源；HTTPS需要另行配置TLS终止，本项目部署入口不提供TLS。

Docker必须先设置以下变量。Linux使用域名、内网或NAT时也建议在安装或047升级前显式设置；在执行部署命令的同一终端输入：

```sh
read -r -p '浏览器访问地址（含协议和端口，不加路径）: ' MMX_EXTERNAL_ORIGIN
export MMX_EXTERNAL_ORIGIN
```

Linux未设置时会探测公网IPv4，只有两个结果一致才采用。探测失败会停止并提示配置，不会用私网地址冒充公网；出口IP探测也不代表入站网络已放行。Docker不自动探测地址。

## 2. 选择部署方式

### Docker / Compose

按README进入源码目录，设置上面的地址，然后启动：

```sh
docker compose up --build -d
```

打开配置的地址后追加 `/manage/servers`。在同一源码目录读取token：

```sh
docker compose exec --user 10001 miaomiaowu cat /app/data/admin-token
```

### Linux一键安装

需要正在运行的systemd、root权限及apt/dnf/yum之一。普通用户先 `sudo -i`，再设置变量和执行命令；首次安装会自动准备专用Python3.12，不替换系统Python。

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

只有wget时：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash
```

打开安装结束输出的实际地址，默认 `http://服务器IP:45300/manage/servers`。在服务器root终端读取token：

```sh
cat /var/lib/miaomiaowu-x-independent/admin-token
```

token只在自己的终端读取并填入页面，不发送给聊天或写进反馈。

## 3. 修改端口或使用域名

下面的变量要在安装或启动前 `export`，并与实际访问地址保持一致：

| 变量 | 适用方式 | 默认值 / 用途 |
| --- | --- | --- |
| `MMX_EXTERNAL_ORIGIN` | 两种方式 | 浏览器实际访问地址；Docker必填，Linux可自动探测 |
| `MMX_HOST_PORT` | Docker / Compose | 宿主端口，默认8080；容器仍为45300 |
| `MMX_PORT` | Linux | 监听端口，默认45300，优先于`PORT` |
| `PORT` | Linux | 未设置`MMX_PORT`时作为端口值 |
| `MMX_HOST` | Linux | 默认0.0.0.0；127.0.0.1用于仅SSH访问 |

例如Compose改用宿主8081：先 `export MMX_HOST_PORT=8081`，再输入实际的 `http://服务器IP:8081` 并启动。Linux改端口则设置 `MMX_PORT`；Linux的访问地址端口须与监听端口一致，Docker访问地址使用宿主端口。

域名也通过 `MMX_EXTERNAL_ORIGIN` 设置，不省略实际需要的端口。页面必须使用配置的地址访问；换成其他IP、域名或端口可能被拒绝。

<details>
<summary>仅通过SSH访问Linux服务</summary>

在服务器root终端，安装或047升级前设置：

```sh
export MMX_HOST=127.0.0.1
unset MMX_EXTERNAL_ORIGIN
```

在自己电脑建立转发：

```sh
ssh -N -L 45300:127.0.0.1:45300 user@server
```

浏览器打开 `http://127.0.0.1:45300/manage/servers`。修改端口时同步调整转发和访问地址。

</details>

## 4. 停止服务与保留数据

| 方式 | 数据位置 | 停止命令 |
| --- | --- | --- |
| Compose | 项目创建的Docker命名卷，容器内`/app/data` | `docker compose down`，不要加`-v` |
| docker run | README示例的`mmx_data`卷 | `docker stop miaomiaowu-x` |
| Linux | `/var/lib/miaomiaowu-x-independent` | `systemctl stop miaomiaowu-x-independent.service` |

容器删除重建后继续使用原卷，记录和token会保留；不要删除数据卷。Linux代码和专用Python在 `/opt/miaomiaowu-x-independent`，与数据分开。

<details>
<summary>Docker改用本机bind目录</summary>

docker run方式可先 `mkdir -p mmx-data` 创建全新空目录，再将README命令中的 `-v mmx_data:/app/data` 替换为：

```sh
--mount "type=bind,src=$PWD/mmx-data,dst=/app/data"
```

入口只初始化空目录，不需要手工chown；已有容器数据必须保持原权限。未知非空目录会拒绝启动，不会递归修改旧数据。不要直接挂载Linux安装数据或其他旧库。

</details>

## 5. 升级未修改的047安装

只适用于原 `install-047.1` 安装的 `.3` 版本及原服务，要求旧服务正在运行。修改过代码或服务配置、已升级050或残留新构建目录的安装会拒绝，不是通用升级工具。

在服务器root终端，按第1步配置所需地址，再执行：

```sh
curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash -s -- update
```

只有wget时：

```sh
wget -qO- https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash -s -- update
```

无需手写服务配置、删除数据或重新首次安装。升级会停服备份、切换新代码并沿用原数据与token；备份位置会显示在终端。

## 6. 查看状态与排错

| 方式 | 状态与日志 |
| --- | --- |
| Compose | `docker compose ps`；`docker compose logs --tail=50` |
| docker run | `docker ps -a`；`docker logs --tail=50 miaomiaowu-x` |
| Linux | `systemctl status miaomiaowu-x-independent.service`；`journalctl -u miaomiaowu-x-independent.service -n 50 --no-pager` |

无法访问时，先确认服务启动成功、浏览器地址与配置一致、端口映射正确，再检查入站网络规则。地址探测失败时按第1步显式设置；遇到已有目录或未知数据的拒绝，不要删库强行安装。反馈请提供版本、操作和脱敏错误，不附token、数据库或备份。

<details>
<summary>047升级备份与失败恢复边界</summary>

升级先验证原安装及数据实现兼容性，停服后创建一致性备份，再自动切换服务。新代码目录是原代码目录追加`-build-20261108-1`；备份目录追加`-backup-时间-进程号`，权限仅root可读，含凭据，不上传。

原代码和数据保留。切换失败时恢复原服务配置并启动旧服务，不回灌数据库、不删除新旧代码；恢复失败会明确提示，需先诊断，不能用首次安装代替恢复。本次数据实现未变，不承诺通用跨版本或跨数据结构回退。

</details>

<details>
<summary>高级路径、版本及支持边界</summary>

| Linux变量 | 默认值 |
| --- | --- |
| `MMX_PREFIX` | `/opt/miaomiaowu-x-independent` |
| `MMX_DATA` | `/var/lib/miaomiaowu-x-independent` |
| `MMX_UNIT` | `miaomiaowu-x-independent.service` |
| `MMX_SERVICE_USER` | `mmx-independent` |

普通安装无需修改。自定义安装升级时必须传入原配置；路径和名称须满足安装器安全检查。

当前runtime为 `independent-preview-20261108.1`，Docker为 `docker-051.1`，Linux安装器为 `install-050.1`。Linux固定下载commit `9d5bad5cc9aef2d30fbcabe9450b5a4a022396ec`，核验manifest SHA256 `4fb124fd89c0c375667dbdc4562c583ce0d87bc967c8c3dfb11e0dee60071236`及每个文件；固定快照内的旧安装器不会被执行。

Debian12 ARM64的Linux安装、047升级和rootful Docker/Compose已实测；其他发行版、x86_64及rootless Docker未实测。Linux脚本不支持OpenRC、Alpine或无systemd环境；Docker部署按README使用独立入口。Postgres、旧0.2.7导入、跨部署数据迁移、自动更新和通用卸载未实现；产品未完成范围以README为准。

</details>
