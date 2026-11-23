# third_party/redis-win64

本项目**自带的 Redis 二进制**，供答案缓存（AC-15 / AC-17）在 Windows 上开箱即用。

## 为什么把二进制提交进仓库

“跨实例共享缓存”是这套 Agent 的验收项之一。但如果 Redis 需要使用者自己安装，
那么在一台干净的 Windows 上 clone 下来，`cache.backend=redis` 必然连不上，
系统会**静默降级成进程内字典**——功能看不出异常，验收项却已经不成立了。

这类“依赖缺失导致的假通过”，比跑不起来更危险。所以这里选择把 2.4MB 的运行时
和配置一起入库，让 `python code/agent/run_redis.py` 成为唯一前置条件。

## 内容

| 文件 | 用途 |
|---|---|
| `redis-server.exe` | 服务端（1.6 MB） |
| `redis-cli.exe` | 命令行客户端（488 KB），手工排查用 |
| `redis-check-aof.exe` | AOF 校验工具，Redis 自带的配套工具 |
| `EventLog.dll` | 服务端把事件写入 Windows 事件日志时使用 |
| `redis.conf` | **本项目的配置**，非原版。只监听 127.0.0.1、关闭持久化、限 512MB |

## 没带的东西

- `*.pdb`（3.4~6.8 MB 的调试符号）：只有调试 exe 崩溃时需要，运行不需要，能省 15MB。
- `*.docx`：原发行包里的英文说明文档，与使用无关。
- `dump.rdb`：运行时数据，落在 `code/agent/.run/redis/`（已 gitignore）。

## 来源与许可

- 版本：**Redis 3.2.100**（Microsoft Open Technologies 的 Windows 移植版，2016-07 构建）
- 上游：<https://github.com/microsoftarchive/redis/releases/tag/win-3.2.100>
- 许可：**BSD 3-Clause**，与 Redis 本体一致，允许随项目分发（需保留版权声明）。
  原始版权声明见上游仓库 `COPYING`；本目录仅做二进制转发，未做任何修改。
- 注意：该移植版**已停止维护**（Redis 官方自 3.2 起不再支持 Windows），
  存在已知的、上游未修的问题。它在本项目中只承担“本机开发环境下的缓存”这一角色：
  数据可丢、无持久化要求、只监听回环地址。**生产环境请使用 Linux 上的 Redis 5+ 或托管服务。**

## 怎么用

```bash
python code/agent/run_redis.py            # 没有就起，有就复用
python code/agent/run_redis.py --status   # 体检：版本/鉴权/key 数
python code/agent/run_redis.py --stop     # 停止
```

密码不写进 `redis.conf`（那会随仓库公开），由 `REDIS_PASSWORD` 环境变量注入
（`code/agent/.env`，已 gitignore）。未设置时以“仅本机、无密码”启动。

webapp 启动时若发现 Redis 没在跑且配置为本机地址，会自动调用上面的脚本拉起。
