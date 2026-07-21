<div align="center">

# mqgov-cli

**面向人类_和_ AI agent 的受治理消息中间件操作工具。**

一条安全的命令行,统一管理 **Kafka**、**RabbitMQ**、**Pulsar**、**RocketMQ** —— 按后端如实声明能力；客户端无法证明安全的操作一律失败关闭。

[![npm version](https://img.shields.io/npm/v/mqgov-cli.svg)](https://www.npmjs.com/package/mqgov-cli)
[![CI](https://github.com/JiangHe12/mqgov-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/JiangHe12/mqgov-cli/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/mqgov-cli.svg)](LICENSE)
[![signed](https://img.shields.io/badge/release-cosign%20%2B%20npm%20provenance-blue.svg)](#-信任与校验)

[English](README.md) · [简体中文](README_zh.md)

</div>

---

## 🧭 这是什么?(先读这里)

消息中间件 —— **Kafka**、**RabbitMQ**、**Pulsar**、**RocketMQ** —— 是事件驱动系统的骨架。针对它们的操作看似平常,实则危险:**重置消费组 offset** 可能引发重复消费风暴、或静默跳过未处理消息;**清空(purge)或删除 topic** 会销毁数据;**向 `__consumer_offsets` 这类内部 topic 生产**会破坏集群状态。这些错误往往是*静默的* —— 几小时后才发现。

**mqgov-cli 为每一个这样的操作加上护栏。** 把它当成一个谨慎的助手:

- 🔎 **先给你看爆炸半径** —— `--dry-run` / `--plan` 在动手前打印精确的每分区影响(一次 offset 重置会重放/跳过多少条消息)。
- 🛡️ **危险操作没有显式签字就拒绝** —— 高危命令需要确认标志、变更工单、以及该操作专属的 `--allow-*`。
- 👀 **peek/tail 不消费** —— 查看或流式读取消息指纹绝不推进消费者位点、也不清空队列。
- 📜 **一切记入防篡改审计日志** —— 只记 sha256 指纹和计数,**绝不记你的消息体**。
- 🤖 **可安全交给 AI agent** —— agent 可自由读取和预览,但**无法**伪造危险操作所需的人工审批。

它构建在共享的 [`opskit-core`](https://github.com/JiangHe12/opskit-core) 治理引擎之上,是面向 AI agent 的 **opskit** 受治理 CLI 家族的一员。

---

## ✨ 特性

| | |
|---|---|
| 📨 **四个 broker** | **Kafka**(franz-go)、**RabbitMQ**(AMQP + 管理 API)、**Pulsar**(客户端 + admin REST)、**RocketMQ**(rocketmq-client-go/v2)。一套与后端无关的治理模型;按 context 选择或按命令覆盖。 |
| 🧱 **topic / group / message / dlq / acl / schema / fleet** | topic:list · describe · create · alter · delete · purge（按后端能力开放）。消费组:list · lag · reset-offset。消息:非破坏性 peek · tail · 有界 mirror · produce。DLQ:list · peek · redrive · purge,映射各 broker 原生模型。ACL:list · grant · revoke(按后端能力开放)。Schema:list · describe · check · register · delete(按原生 schema registry 能力开放)。Fleet:跨已配置 context 的只读状态与 topic 视图。 |
| 🔐 **R0–R3 治理** | 每个操作由 fail-closed 的 `mqclass` 引擎分级;保护上下文与内部/系统 topic 升一档;AI 调用方永远无法自我授权。 |
| 🎯 **真实爆炸半径预览** | `reset-offset --dry-run` 和 `purge --dry-run` 从实时 broker 计算真实的每分区消息 delta —— 不靠猜。预览只读,绝不变更。 |
| 👀 **非破坏性 peek/tail** | 把消息以指纹形式查看或流式读取,不消费、不移动游标(Kafka 直接分区读取、Pulsar Reader、RabbitMQ 仅 peek 使用 get+requeue)。无法保证非破坏的 broker 上,操作 **失败关闭**而非静默消费。 |
| 🧭 **诚实的能力声明** | broker 各不相同 —— mqgov 如实报告每个 broker 实际支持什么(`capabilities -o json`),其余一律 **`NOT_IMPLEMENTED` 失败关闭**,绝不伪造。 |
| 📜 **防篡改审计** | 每个操作哈希链记录(sha256 指纹 + 计数,**不含消息体/key/header**);`audit verify` 检测篡改。 |
| 🔒 **TLS 证书 TOFU** | Kafka、RabbitMQ、Pulsar 的 TLS 连接首次使用时绑定服务端 leaf 证书 SPKI-SHA256 到 `.mqgov-cli/tls_known_hosts`;后续 SPKI 不一致即连接硬失败。 |
| 🩺 **运维与体验** | 后端绑定的 `ctx` 上下文(密钥经 credstore)、`doctor` 诊断、shell `completion`、OpenTelemetry 链路/指标、处处 JSON 输出。 |
| 🔏 **可信供应链** | 二进制 **cosign 签名**,npm 包带 **provenance**,安装器校验 **SHA-256**。 |

### 各后端能力矩阵

| | Kafka | Pulsar | RabbitMQ | RocketMQ |
|---|:---:|:---:|:---:|:---:|
| topic list / describe / create | ✅ | ✅ | ✅ | ✅ |
| topic delete | ✅ | ✅ | ✅ | ❌ `NOT_IMPLEMENTED`⁴ |
| produce | ✅ | ✅ | ✅ | ✅ |
| **非破坏性 peek** | ✅ | ✅(Reader) | ✅(get+requeue) | ❌ `NOT_IMPLEMENTED`¹ |
| **非破坏性 tail** | ✅ | ✅(Reader) | ❌ `NOT_IMPLEMENTED`² | ❌ `NOT_IMPLEMENTED`¹ |
| **offset lag / reset** | ✅ | ✅(游标) | ❌(无 offset) | ❌ |
| alter 分区 | ✅ | ✅ | ❌ | ❌ |
| purge | ✅ | ✅ | ✅ | ❌ |
| **DLQ list / peek / redrive / purge** | 显式 topic peek/purge ✅;list/redrive ❌ | list/peek ✅ `{topic}-{subscription}-DLQ`;redrive/purge ❌ | ✅ DLX 队列 | list ✅ `%DLQ%group`;其他 ❌ |
| **ACL list / grant / revoke** | ✅ | ✅ namespace/topic permissions | ✅ user-vhost permissions | ❌ `NOT_IMPLEMENTED`³ |
| **schema list / describe / check / register / delete** | ✅ Confluent Schema Registry | ✅ 内建 admin schema API | ❌ `NOT_IMPLEMENTED` | ❌ `NOT_IMPLEMENTED` |

¹ RocketMQ 的 Go v2 `PullConsumer` 会进入消费组生命周期并提交 offset,无法保证非破坏性 peek/tail —— mqgov 选择失败关闭,而非静默推进 offset。² RabbitMQ 没有向前的非破坏性 tail,读取语义是 consume/requeue。不支持的操作一律返回 `NOT_IMPLEMENTED`(exit 12),绝不假装成功。

³ RocketMQ broker ACL 存在 broker 侧 `plain_acl.yml` 中,但 `rocketmq-client-go/v2` 没有公开、cgo-free、干净的 ACL admin API 可读写该配置。mqgov 不 shell out 到 Java `mqadmin`,也不手搓 remoting 命令;在 Go 客户端提供干净 API 前,请通过 broker 配置或官方 mqadmin 带外管理 RocketMQ ACL。

⁴ RocketMQ topic delete 已禁用：上游 v2 admin 客户端忽略 broker 和 NameServer 的响应码，仅看到 NameServer route 消失不足以证明 broker 侧已删除。RocketMQ `--namespace` 同样被拒绝，因为该客户端会对 route lookup/produce 加 namespace，却未对 create/delete/list 一致处理。

---

## 📦 安装

```bash
npm install -g mqgov-cli
```

这会装一个极小的启动器;首次运行时从已签名的 [GitHub Release](https://github.com/JiangHe12/mqgov-cli/releases) 下载对应 OS/arch 的预编译二进制,并在使用前**校验 SHA-256**。安装器需要 Node.js ≥ 14(CLI 本身是自包含的 Go 二进制)。

<details>
<summary>其他安装方式</summary>

- **直接下载** —— 从 [Releases 页](https://github.com/JiangHe12/mqgov-cli/releases)抓取对应平台二进制,用 `checksums.txt`(cosign 签名)校验,放到 `PATH`,重命名为 `mqgov`。
- **从源码** —— `go install github.com/JiangHe12/mqgov-cli@latest`(Go 1.25+)。
- **镜像 / 离线** —— 设 `MQGOV_DOWNLOAD_MIRROR=<base-url>` 从你自己的镜像拉取。旧的 `MQGOV_CLI_DOWNLOAD_MIRROR` 仍兼容但已 deprecated。

验证安装:

```bash
mqgov version
mqgov doctor          # 检查 context、后端可达性、审计日志可写性
```

</details>

---

## 🚀 快速上手(60 秒)

```bash
# 1. 把 mqgov 指向你的 broker（context 控制变更固定为 R3）
mqgov ctx set dev --backend kafka --brokers 127.0.0.1:9092 --yes --ticket OPS-123 --allow-context-change
mqgov ctx use dev --yes --ticket OPS-123 --allow-context-change
mqgov ctx test                       # 经 context ping 一下 broker

# 2. 读 —— 读永远免费(R0),无需任何标志
mqgov topic list -o json
mqgov topic describe orders -o json
mqgov message peek orders --count 5 -o json     # 只返回指纹,什么都不消费
mqgov message tail orders --max-messages 10 -o json

# 3. 预览一个高危操作的爆炸半径 —— 此刻什么都没变
mqgov group reset-offset billing orders --to latest --dry-run -o json   # 显示每分区 delta

# 4. 执行 —— R3 操作需要你的确认 + 工单 + 对应 allow 标志
mqgov group reset-offset billing orders --to latest --yes --ticket OPS-123 --allow-offset-reset

# 5. 看看发生了什么
mqgov audit query --since 1h -o json
```

> 💡 **提示:** 创建生产 context 时打上 `--protected`。mqgov 会自动抬高该 context 下每个危险操作的门槛。

---

## 🔐 治理模型(最重要的部分)

每个命令被 fail-closed 的 `mqclass` 分类器归入四个**风险档**之一。档位越高,需要的人工签字越显式:

| 档 | 涵盖 | 你必须提供 |
|:---:|---|---|
| **R0** | 读与预览(`topic list/describe`、`group list/lag`、`message peek`、`message tail`、`dlq list/peek`、`acl list`、`schema list/describe/check`、`fleet status/topics`、`*-dry-run`、`audit query/verify`、`audit prune` 预览、`doctor`) | 无 —— 但仍会审计 |
| **R1** | 普通写(`message produce`、`message mirror` 目标侧、非 RocketMQ 的 `topic create`、新 subject 的 `schema register`) | `--yes`(或交互确认) |
| **R2** | 升级变更(`topic alter`、RocketMQ `topic create`、`group create/delete`、`acl grant`、已有 subject 的 `schema register`、向**保护** topic produce/mirror) | `--yes` **且**非空 `--ticket` |
| **R3** | 破坏性 / 不可逆操作（`group reset-offset`、topic/DLQ purge、受支持的 topic/schema delete、受支持的 DLQ redrive、宽泛 ACL 变更、内部 topic produce/mirror）、受保护的 RocketMQ topic upsert，及治理控制变更（`ctx set/use/delete/import/migrate-credentials`、`ctx role set/unset`、确认执行的 `audit prune`） | 以上 **再加**该操作专属的 `--allow-*` 标志 |

R3 的 allow 标志:`--allow-offset-reset`、`--allow-topic-purge`、`--allow-topic-delete`、`--allow-topic-upsert`、`--allow-destructive-acl`、`--allow-internal-produce`、`--allow-schema-delete`、`--allow-context-change`、`--allow-context-delete`、`--allow-role-change`、`--allow-audit-prune`。

**保护上下文、保护 topic、内部/系统 topic 都使档位升一级。** 例如向 `__consumer_offsets` 生产被当作破坏性 R3 操作,需要 `--allow-internal-produce`。

三条规则保证安全 —— 尤其对自动化:

1. **爆炸半径来自工具,不靠猜。** 用 `--dry-run` / `--plan` 看精确的每分区影响,绝不靠推理估算。
2. **`mqclass` fail-closed 且结构感知。** 所有 offset 变更、purge、topic delete、ACL revoke、宽泛 ACL grant 钉死 R3;通配/glob 目标升级;未知操作失败关闭到最高档 —— 绝不掉到 R0。
3. **🤖 AI agent 绝不能伪造 `--ticket`、`--allow-*` 或高危 `--yes`。** 这些是*人类*授权输入。agent 应把"此操作需要审批 X"上报给操作者并停下。

授权与审计身份只来自本机 OS 的 `username@hostname`。`--operator`、
`MQGOV_OPERATOR` 与 `MQGOV_CLI_OPERATOR` 仅为兼容输入，不参与身份或
授权。这不能区分同一 OS 账户下的 AI 进程与人工进程；若需要这条边界，
必须使用外部签名批准源或独立受保护的操作者账户。

---

## 📚 命令参考

`mqgov <名词> <动词> [flags]`。加 `-o json` 得到机器可读输出,任意命令加 `--help` 看完整 flag,`mqgov capabilities -o json` 询问绑定后端实际支持什么。

<details open>
<summary><b>topic</b> —— topic / 队列</summary>

```bash
# 读(R0)
mqgov topic list     [--pattern <name|glob>] -o json
mqgov topic describe <topic> -o json

# 写
mqgov topic create <topic> [--partitions N] --yes                                  # 非 RocketMQ R1(保护则 R2)
mqgov topic create <topic> [--partitions N] --yes --ticket <t>                     # RocketMQ R2
mqgov topic create <topic> [--partitions N] --yes --ticket <t> --allow-topic-upsert # 受保护 RocketMQ 为 R3
mqgov topic alter  <topic> --partitions N --yes --ticket <t>                       # R2(Kafka/Pulsar)
mqgov topic purge  <topic> [--dlq] --dry-run                                        # R0 预览
mqgov topic purge  <topic> [--dlq] --yes --ticket <t> --allow-topic-purge          # R3
mqgov topic delete <topic> --yes --ticket <t> --allow-topic-delete                 # 支持该操作的后端 R3；RocketMQ NOT_IMPLEMENTED
```
</details>

<details>
<summary><b>group</b> —— 消费组 / 订阅</summary>

```bash
# 读(R0)
mqgov group list [--pattern <name>] -o json
mqgov group lag  <group> <topic> -o json

# 重置消费组位点
mqgov group reset-offset <group> <topic> --to <target> --dry-run -o json           # R0 预览(真实每分区 delta)
mqgov group reset-offset <group> <topic> --to <target> --yes --ticket <t> --allow-offset-reset   # R3

#   --to:earliest | latest | offset:N | datetime:<RFC3339> | shift:±N
#   (Pulsar reset 只支持可从实时分区 backlog 精确计算的 latest;
#    其他目标及不支持的后端在 mutation intent 前返回 NOT_IMPLEMENTED)
```

offset 是 Kafka 与 Pulsar 的概念。Pulsar reset 只支持 `--to latest`;earliest/datetime/绝对 offset/shift 无法取得可靠 affected count,因此 mqgov 明确拒绝。在 RabbitMQ 与 RocketMQ 上,`group lag` / `reset-offset` 以 `NOT_IMPLEMENTED` 失败关闭。
</details>

<details>
<summary><b>message</b> —— peek、tail、mirror 与 produce</summary>

```bash
mqgov message peek    <topic> [--partition N] [--offset N] [--count N] -o json     # R0,非破坏,仅指纹
mqgov message tail    <topic> [--partition N] [--from earliest|latest|offset:N] [--follow] [--max-messages N] [--timeout 30s] -o json
mqgov message mirror  <source-topic> --to-context <ctx> --to-topic <topic> --limit 100 --dry-run -o json
mqgov message mirror  <source-topic> --to-context <ctx> --to-topic <topic> --limit 100 --yes -o json
mqgov message produce <topic> [--key <k>] [--body <text>] --yes                    # R1(内部 topic 为 R3 + --allow-internal-produce)
```

`peek` 和 `tail` 绝不消费消息、不移动游标,只返回 sha256 指纹(`keySha256`、`bodySha256`、size、可选 timestamp)—— 绝不返回消息体。peek count 必须为正数;结果保持 broker 读取顺序、绝不超过 `--count`,到达当前边界时返回真实的较小数量。RabbitMQ 会先把不同消息作为未确认批次保留,完成指纹计算后再整体 requeue;恢复失败则命令失败。`tail` 受 `--max-messages` 与 `--timeout` 约束;`--follow` 也只会流式读取到这些边界或取消为止。

`message mirror` 是有界一次性拷贝,不是 daemon。它只解析一次源/目标 topic,随后分别按源 context 策略授权非破坏性读取、按持久化的 `--to-context` 策略授权目标 produce;任一失败都发生在消息读取和目标写入之前。源与目标分别审计各自的 context、target、请求/结果指纹和计数,不会记录 Key、Body、Headers。`--dry-run` / `--plan` 只读取/计数,不 produce。Kafka 与 Pulsar 可作为 mirror 源;RabbitMQ 与 RocketMQ 源镜像失败关闭为 `NOT_IMPLEMENTED`。Kafka 支持 `--from earliest|latest|offset:N|timestamp:<RFC3339>` 与 `--partition`;Pulsar 支持 `earliest|latest|timestamp:<RFC3339>` 和全分区读取。
</details>

<details>
<summary><b>dlq</b> —— 死信队列治理</summary>

```bash
mqgov dlq list [--topic <source-or-dlq>] [--group <group-or-sub>] [--pattern <name|glob>] -o json     # R0
mqgov dlq peek <dlq> [--topic <source>] [--group <group-or-sub>] [--count N] -o json                   # R0,仅指纹
mqgov dlq redrive <dlq> --target <live-topic> [--count N] --dry-run -o json                            # R0 预览(RabbitMQ)
mqgov dlq redrive <dlq> --target <live-topic> [--count N] --yes --ticket <t> --allow-internal-produce  # R3(RabbitMQ)
mqgov dlq purge <dlq> --dry-run -o json                                                                 # R0 预览
mqgov dlq purge <dlq> --yes --ticket <t> --allow-topic-purge                                           # R3
```

DLQ 映射保持后端原生且诚实:RabbitMQ redrive 经 publisher confirm 发布,只移除已确认的源消息;Kafka 显式 topic 支持 peek/purge,但无法原子完成精确有界的 copy-and-remove,因此拒绝 redrive;Pulsar 支持 `{topic}-{subscription}-DLQ` 的 list/peek,但 Reader/游标跳过无法提供 redrive/purge 所需的删除语义,因此拒绝二者;RocketMQ 只列出 `%DLQ%{consumerGroup}` topic。不支持的动词返回 `NOT_IMPLEMENTED`,绝不返回“仅复制”或成功 no-op。

Redrive 按 internal-produce 治理:dry-run 是只读预览,真实执行需要 `--allow-internal-produce`。审计仍然只记录指纹/计数,永不记录 message body、key、headers。
</details>

<details>
<summary><b>schema</b> —— schema registry</summary>

```bash
mqgov schema list [--pattern <subject>] -o json
mqgov schema describe <subject-or-topic> [--version latest|N] -o json
mqgov schema check <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO [--version latest] -o json
mqgov schema register <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO --yes -o json
mqgov schema register <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO --yes --ticket <t> -o json
mqgov schema delete <subject-or-topic> [--version N] [--permanent] --yes --ticket <t> --allow-schema-delete -o json
```

`schema list`、`schema describe`、`schema check` 都是 R0 且会审计。`schema register` 对新 subject 是 R1,对已存在 subject 是 R2;向已有 subject 注册新版本就是演进。已有 subject 会先调用后端兼容性检查,不兼容则拒绝注册。`schema delete` 是 R3,必须提供 `--allow-schema-delete`。Kafka 映射到 Confluent Schema Registry,支持 soft delete 与 `--permanent` hard delete。Pulsar 映射到内建 admin schema 端点;由于 Pulsar 没有 soft/hard 区分,本后端只接受永久 subject 删除,soft 或 version delete 返回 `NOT_IMPLEMENTED`。RabbitMQ 与 RocketMQ 失败关闭为 `NOT_IMPLEMENTED`。审计只记录 subject/version 元数据和 schema hash,永不记录 schema 全文或 registry 凭据。
</details>

<details>
<summary><b>fleet</b> —— 跨 context 只读视图</summary>

```bash
mqgov fleet status --all -o json
mqgov fleet topics --contexts dev,staging --pattern orders -o json
```

`fleet status` 对选中的 context 扇出 `Ping`、`Describe`、`Capabilities`。`fleet topics` 扇出 topic list,并在每行标明来源 context。context 选择必须且只能使用 `--all` 或 `--contexts a,b,c` 之一。Fleet 只有 R0 读:每个 context 的每次底层读取仍走和单 context 命令相同的 R0 分类与授权路径,并使用该 context 自己的已存凭据。部分失败会作为该 context 的 `denied`、`unreachable` 或 `error` 数据如实返回,命令整体仍退出 0。
</details>

<details>
<summary><b>acl</b> —— broker 访问控制</summary>

```bash
mqgov acl list [--principal <P>] [--resource-type <T>] [--resource-name <N>] -o json

# Kafka broker ACL
mqgov acl grant --principal User:svc --resource-type topic --resource-name orders \
  --pattern literal --operation read --permission allow --yes --ticket <t>

mqgov acl revoke --principal User:svc --resource-type topic --resource-name orders \
  --pattern literal --operation read --permission allow \
  --yes --ticket <t> --allow-destructive-acl

# RabbitMQ 原生 user-vhost 权限
mqgov acl grant --principal svc --vhost / --resource-type vhost --resource-name '^orders$' \
  --pattern regex --operation read --permission allow --yes --ticket <t>

mqgov acl revoke --principal svc --vhost / --resource-type vhost --resource-name '^orders$' \
  --pattern regex --operation read --permission allow \
  --yes --ticket <t> --allow-destructive-acl

# Pulsar 原生 namespace/topic 权限
mqgov acl grant --principal app-role --resource-type namespace --resource-name public/default \
  --pattern literal --operation produce --permission allow --yes --ticket <t>

mqgov acl revoke --principal app-role --resource-type topic --resource-name orders \
  --pattern literal --operation consume --permission allow \
  --yes --ticket <t> --allow-destructive-acl
```

`acl list` 是 R0 且会审计。普通 `acl grant` 是 R2。宽泛授权(Kafka prefixed pattern、通配 principal、通配 resource、cluster 资源、`all`、`alter`、cluster-action 类操作,`.*`、`.+`、`.`、`orders.*` 这类宽泛 RabbitMQ regex,或 Pulsar `functions`/`sources`/`sinks`/`packages`)以及所有 `acl revoke` 都是 R3,需要 `--allow-destructive-acl`。Kafka 使用 `literal`/`prefixed` broker ACL。RabbitMQ 映射到原生按 user/vhost 的权限 regex(`configure`、`write`、`read`),只支持 `--permission allow` 和 `--pattern regex`。Pulsar 映射到原生 namespace/topic 上的 role 权限,action 为 `produce`、`consume`、`functions`、`sources`、`sinks`、`packages`;只支持 allow 和 `--pattern literal`。RocketMQ 失败关闭为 `NOT_IMPLEMENTED`:broker ACL 需通过 broker 侧 `plain_acl.yml` 或官方 Java `mqadmin` 带外管理,因为 `rocketmq-client-go/v2` 没有公开干净的 ACL admin API。
</details>

<details>
<summary><b>ctx</b>、<b>audit</b>、<b>doctor</b> 等</summary>

```bash
# 后端绑定的上下文(凭据经 credstore,绝不明文)
mqgov ctx set <name> --backend kafka    --brokers <h:p,h:p> [--sasl-mechanism PLAIN] [--tls --ca-cert <f>] [--schema-registry-url <url>] [--schema-registry-username <u>] [--schema-registry-password <p>] [--protected]
mqgov ctx set <name> --backend rabbitmq (--amqp-url <url> | --host <h> --port <p> --vhost </>) --management-url <url> --username <u>
mqgov ctx set <name> --backend pulsar   --service-url pulsar://<h:p> --admin-url http://<h:p> [--tenant public] [--pulsar-namespace default]
mqgov ctx set <name> --backend rocketmq --nameservers <h:p,h:p> [--broker-addr <h:p>]
mqgov ctx use|list|current|delete|export|import|role|migrate-credentials|test
mqgov ctx role set|unset|list <context>
#   密钥:--password <pw|token|secretKey> 与 --schema-registry-password <pw> 都经 --credential-backend <encrypted-file|keychain|...>(必须用非 plain 后端)
#   所有 context 控制变更都可先用 --plan 预览；预览不授权、不写入。
#   set/use/import/migrate 执行需 --yes --ticket <t> --allow-context-change；delete 用 --allow-context-delete；role set/unset 用 --allow-role-change。
#   ctx export 默认脱敏凭据;迁移明文凭据时,先运行 ctx migrate-credentials --dry-run,再执行获批命令。
#   ctx import 会在首次写入前验证全部 context 与凭据后端。配置提交失败时补偿凭据写入;无法安全补偿或补偿不完整时返回明确错误,并在审计中标为 uncertain。
#   文件导出会拒绝 context/审计/spool/锁文件的同路径与别名，并通过同目录私有临时文件、fsync 和原子替换落盘。
#   RabbitMQ:非交互运行优先用 --username + MQGOV_PASSWORD;若要落盘密码,用 --password 配 keychain 或 encrypted-file。
#   如果 --amqp-url 含 userinfo,显式 --username 和密码来源会覆盖它,AMQP 与 management API 使用同一套认证。

# 审计(防篡改,仅指纹)
mqgov audit query  [--since 24h] [--type <t>] [--operator <o>] [--status <s>] [--limit 100] -o json
mqgov audit verify [--strict] -o json
mqgov audit prune  (--before <…> | --older-than <days> | --keep-last <n>)                    # R0 预览
mqgov audit prune  (--before <…> | --older-than <days> | --keep-last <n>) --confirm --yes --ticket <t> --allow-audit-prune

# 确认清理按已持久化的 current context 策略授权且固定为 R3；
# 必须同时提供 --confirm 和完整 R3 授权。
# 确认清理由 opskit-core/v2 完成认证链校验并安全推进 checkpoint；其 intent/outcome 写入 sibling `.<audit-base>-control` 证据日志。
# 所有变更在接触目标前持久化 intent，完成后持久化 outcome。
# outcome 被明确判定为未提交时，mqgov 返回 AUDIT_INCOMPLETE，并把它写入 <audit.log>.outcome-spool；下一条 intent 前必须先回放。
# 回放持久、按序，但刻意采用 at-least-once 语义：若进程在“已追加、未删除 spool”之间崩溃，同一 mutationId 的 outcome 可能重复。
# 遇到 AUDIT_INCOMPLETE 不要盲目重试；先修复审计存储并执行 audit query/verify，只有明确未提交的队列条目才允许自动回放。
# opskit-core/v2 会返回追加提交状态。mqgov 仅在明确未提交时排队安全重试；状态不确定的回放会改名并标记为 `.indeterminate`、阻断后续自动回放，必须按 mutationId 核对。

# 诊断与生态
mqgov doctor -o json            # 只读健康检查(输出已脱敏)
mqgov capabilities -o json      # 绑定后端支持什么
mqgov completion bash|zsh|fish|powershell
mqgov install <agent> --skills  # 把 mqgov AI skill 装进 agent(claude、codex…)或自定义路径
mqgov version
```

RocketMQ context 不支持 `--namespace`；上游 Go v2 admin 客户端的 namespace 包装不一致，mqgov 会直接拒绝，避免实际操作落到不同的 topic 名称。
</details>

---

## 🤖 给 AI agent

mqgov-cli 设计为可被自治 agent 安全驱动:

- 先跑 `mqgov capabilities -o json` 发现绑定后端支持什么 —— broker 各异,别假设(例如 Kafka、RabbitMQ、Pulsar 都支持 `acl`,但原生模型不同;Kafka 与 Pulsar 支持 `schema`;RabbitMQ/RocketMQ 无 offset、schema registry 或 tail;RocketMQ 无 peek 或 topic delete)。需要跨 context 仪表盘时用只读的 `fleet status --all -o json`。
- 处处用 `-o json`;每个命令返回稳定、带版本的信封。
- 爆炸半径来自 `--dry-run` / `--plan`,绝不来自你自己的推理。
- **绝不自填 `--ticket`、`--allow-*` 或高危 `--yes`。** 把所需人工审批上报并停下。

把内置 skill 装进你的 agent,让它自动学会这些规则:

```bash
mqgov install claude --skills     # 也支持:codex、opencode、copilot、cursor、windsurf、aider、cc-switch
```

---

## 🔏 信任与校验

- **已验证发布标签** —— 仅当 signed annotated tag 经 GitHub 验证，且精确匹配 `package.json`、`CHANGELOG.md` 与最新拉取的 `origin/main` 时才开始发布；CI 与完整真实 broker 矩阵会在该标签提交上重跑。
- **签名二进制** —— 每个发布产物用 [cosign](https://github.com/sigstore/cosign) 签名(keyless / OIDC)。`checksums.txt`(同样签名)覆盖所有平台。
- **npm provenance** —— npm 包经 CI 用 OpenID Connect 发布,带 [provenance 证明](https://docs.npmjs.com/generating-provenance-statements),关联到本仓库与此工作流。
- **校验安装** —— npm postinstall 经白名单主机下载二进制,并在安装前对照已签名的 `checksums.txt` 校验 SHA-256。
- **防篡改审计** —— `mqgov audit verify --strict` 重走哈希链,报告任何缺口或改动。
- **传输不裸奔** —— 仅 SASL/TLS 与 mTLS;mqgov 绝不提供 insecure-skip-verify 后门。Kafka、RabbitMQ、Pulsar 的 TLS broker/admin/Schema Registry 连接在正常 CA 校验之上增加 TOFU SPKI-SHA256 pinning;pin 存在 `.mqgov-cli/tls_known_hosts`。

---

## 🏗️ 从源码构建与贡献

```bash
git clone https://github.com/JiangHe12/mqgov-cli && cd mqgov-cli
go build ./...
go test -count=1 ./...
gofmt -l main.go cmd internal      # 必须无输出
golangci-lint run --timeout=5m
go vet -tags=integration ./...
CGO_ENABLED=0 go build ./...
go mod tidy -diff
npm pack --dry-run
```

真后端集成测试(`//go:build integration`,env-gated,默认跳过)使用固定 digest 的 Kafka/RabbitMQ/Pulsar/RocketMQ 镜像。完整的 Kafka/ACL/TLS/RabbitMQ/Pulsar/RocketMQ/mirror 矩阵是发布门禁，并在 nightly 和手动触发时运行。本地用自带的 `docker-compose.*.yml` 跑法见 [`docs/`](docs/)。

完整验证流程见 [CONTRIBUTING.md](CONTRIBUTING.md)，漏洞报告方式与安全边界见
[SECURITY.md](SECURITY.md)。

mqgov-cli 构建在共享的 [`opskit-core`](https://github.com/JiangHe12/opskit-core) 治理引擎之上,是面向 AI agent 的 **opskit** 受治理 CLI 家族的一员 —— 同族还有 [`dbgov-cli`](https://www.npmjs.com/package/dbgov-cli)(数据库)、[`srvgov-cli`](https://www.npmjs.com/package/srvgov-cli)(远程服务器)、[`cfgov-cli`](https://www.npmjs.com/package/cfgov-cli)(配置中心)。

---

## 📄 许可

[MIT](LICENSE) © JiangHe12
