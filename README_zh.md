<div align="center">

# mqgov-cli

**面向人类_和_ AI agent 的受治理消息中间件操作工具。**

一条安全的命令行,统一管理 **Kafka**、**RabbitMQ**、**Pulsar**、**RocketMQ** —— 列出、查看、peek、tail、生产、治理 DLQ、重置 offset、查看/变更 ACL、检视 schema、清空、删除 topic,绝不手滑搞挂生产、也绝不静默清空一个队列。

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
| 🧱 **topic / group / message / dlq / acl / schema / fleet** | topic:list · describe · create · alter · delete · purge。消费组:list · lag · reset-offset。消息:非破坏性 peek · tail · produce。DLQ:list · peek · redrive · purge,映射各 broker 原生模型。ACL:list · grant · revoke(按后端能力开放)。Schema:list · describe · check(按原生 schema registry 能力开放)。Fleet:跨已配置 context 的只读状态与 topic 视图。 |
| 🔐 **R0–R3 治理** | 每个操作由 fail-closed 的 `mqclass` 引擎分级;保护上下文与内部/系统 topic 升一档;AI 调用方永远无法自我授权。 |
| 🎯 **真实爆炸半径预览** | `reset-offset --dry-run` 和 `purge --dry-run` 从实时 broker 计算真实的每分区消息 delta —— 不靠猜。预览只读,绝不变更。 |
| 👀 **非破坏性 peek/tail** | 把消息以指纹形式查看或流式读取,不消费、不移动游标(Kafka 直接分区读取、Pulsar Reader、RabbitMQ 仅 peek 使用 get+requeue)。无法保证非破坏的 broker 上,操作 **失败关闭**而非静默消费。 |
| 🧭 **诚实的能力声明** | broker 各不相同 —— mqgov 如实报告每个 broker 实际支持什么(`capabilities -o json`),其余一律 **`NOT_IMPLEMENTED` 失败关闭**,绝不伪造。 |
| 📜 **防篡改审计** | 每个操作哈希链记录(sha256 指纹 + 计数,**不含消息体/key/header**);`audit verify` 检测篡改。 |
| 🩺 **运维与体验** | 后端绑定的 `ctx` 上下文(密钥经 credstore)、`doctor` 诊断、shell `completion`、OpenTelemetry 链路/指标、处处 JSON 输出。 |
| 🔏 **可信供应链** | 二进制 **cosign 签名**,npm 包带 **provenance**,安装器校验 **SHA-256**。 |

### 各后端能力矩阵

| | Kafka | Pulsar | RabbitMQ | RocketMQ |
|---|:---:|:---:|:---:|:---:|
| topic list / describe / create / delete | ✅ | ✅ | ✅ | ✅ |
| produce | ✅ | ✅ | ✅ | ✅ |
| **非破坏性 peek** | ✅ | ✅(Reader) | ✅(get+requeue) | ❌ `NOT_IMPLEMENTED`¹ |
| **非破坏性 tail** | ✅ | ✅(Reader) | ❌ `NOT_IMPLEMENTED`² | ❌ `NOT_IMPLEMENTED`¹ |
| **offset lag / reset** | ✅ | ✅(游标) | ❌(无 offset) | ❌ |
| alter 分区 | ✅ | ✅ | ❌ | ❌ |
| purge | ✅ | ✅ | ✅ | ❌ |
| **DLQ list / peek / redrive / purge** | list ❌;显式 topic peek/redrive/purge ✅ | ✅ `{topic}-{subscription}-DLQ` | ✅ DLX 队列 | list ✅ `%DLQ%group`;其他 ❌ |
| **ACL list / grant / revoke** | ✅ | ✅ namespace/topic permissions | ✅ user-vhost permissions | ❌ `NOT_IMPLEMENTED`³ |
| **schema list / describe / check** | ✅ Confluent Schema Registry | ✅ 内建 admin schema API | ❌ `NOT_IMPLEMENTED` | ❌ `NOT_IMPLEMENTED` |

¹ RocketMQ 的 Go v2 `PullConsumer` 会进入消费组生命周期并提交 offset,无法保证非破坏性 peek/tail —— mqgov 选择失败关闭,而非静默推进 offset。² RabbitMQ 没有向前的非破坏性 tail,读取语义是 consume/requeue。不支持的操作一律返回 `NOT_IMPLEMENTED`(exit 12),绝不假装成功。

³ RocketMQ broker ACL 存在 broker 侧 `plain_acl.yml` 中,但 `rocketmq-client-go/v2` 没有公开、cgo-free、干净的 ACL admin API 可读写该配置。mqgov 不 shell out 到 Java `mqadmin`,也不手搓 remoting 命令;在 Go 客户端提供干净 API 前,请通过 broker 配置或官方 mqadmin 带外管理 RocketMQ ACL。

---

## 📦 安装

```bash
npm install -g mqgov-cli
```

这会装一个极小的启动器;首次运行时从已签名的 [GitHub Release](https://github.com/JiangHe12/mqgov-cli/releases) 下载对应 OS/arch 的预编译二进制,并在使用前**校验 SHA-256**。安装器需要 Node.js ≥ 14(CLI 本身是自包含的 Go 二进制)。

<details>
<summary>其他安装方式</summary>

- **直接下载** —— 从 [Releases 页](https://github.com/JiangHe12/mqgov-cli/releases)抓取对应平台二进制,用 `checksums.txt`(cosign 签名)校验,放到 `PATH`,重命名为 `mqgov`。
- **从源码** —— `go install github.com/JiangHe12/mqgov-cli@latest`(Go 1.26+)。
- **镜像 / 离线** —— 设 `MQGOV_CLI_DOWNLOAD_MIRROR=<base-url>` 从你自己的镜像拉取。

验证安装:

```bash
mqgov version
mqgov doctor          # 检查 context、后端可达性、审计日志可写性
```

</details>

---

## 🚀 快速上手(60 秒)

```bash
# 1. 把 mqgov 指向你的 broker(存为可复用的 "context")
mqgov ctx set dev --backend kafka --brokers 127.0.0.1:9092
mqgov ctx use dev
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
| **R0** | 读与预览(`topic list/describe`、`group list/lag`、`message peek`、`message tail`、`dlq list/peek`、`acl list`、`schema list/describe/check`、`fleet status/topics`、`*-dry-run`、`audit query/verify`、`doctor`) | 无 —— 但仍会审计 |
| **R1** | 普通写(`message produce`、`topic create`) | `--yes`(或交互确认) |
| **R2** | 升级变更(`topic alter`、`group create/delete`、`acl grant`、向**保护** topic 生产) | `--yes` **且**非空 `--ticket` |
| **R3** | 破坏性 / 不可逆(`group reset-offset`、`topic purge`、`topic delete`、`dlq redrive`、`dlq purge`、宽泛 `acl grant`、`acl revoke`、向**内部/系统** topic 生产) | 以上 **再加**该操作专属的 `--allow-*` 标志 |

R3 的 allow 标志:`--allow-offset-reset`、`--allow-topic-purge`、`--allow-topic-delete`、`--allow-destructive-acl`、`--allow-internal-produce`。

**保护上下文、保护 topic、内部/系统 topic 都使档位升一级。** 例如向 `__consumer_offsets` 生产被当作破坏性 R3 操作,需要 `--allow-internal-produce`。

三条规则保证安全 —— 尤其对自动化:

1. **爆炸半径来自工具,不靠猜。** 用 `--dry-run` / `--plan` 看精确的每分区影响,绝不靠推理估算。
2. **`mqclass` fail-closed 且结构感知。** 所有 offset 变更、purge、topic delete、ACL revoke、宽泛 ACL grant 钉死 R3;通配/glob 目标升级;未知操作失败关闭到最高档 —— 绝不掉到 R0。
3. **🤖 AI agent 绝不能伪造 `--ticket`、`--allow-*` 或高危 `--yes`。** 这些是*人类*授权输入。agent 应把"此操作需要审批 X"上报给操作者并停下。

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
mqgov topic create <topic> [--partitions N] --yes                                  # R1(保护则 R2)
mqgov topic alter  <topic> --partitions N --yes --ticket <t>                       # R2(Kafka/Pulsar)
mqgov topic purge  <topic> [--dlq] --dry-run                                        # R0 预览
mqgov topic purge  <topic> [--dlq] --yes --ticket <t> --allow-topic-purge          # R3
mqgov topic delete <topic> --yes --ticket <t> --allow-topic-delete                 # R3
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
#   (offset:N / shift:N 仅 Kafka 支持;不支持的目标/后端返回清晰错误)
```

offset 是 Kafka 与 Pulsar 的概念。在 RabbitMQ 与 RocketMQ 上,`group lag` / `reset-offset` 以 `NOT_IMPLEMENTED` 失败关闭。
</details>

<details>
<summary><b>message</b> —— peek、tail 与 produce</summary>

```bash
mqgov message peek    <topic> [--partition N] [--offset N] [--count N] -o json     # R0,非破坏,仅指纹
mqgov message tail    <topic> [--partition N] [--from earliest|latest|offset:N] [--follow] [--max-messages N] [--timeout 30s] -o json
mqgov message produce <topic> [--key <k>] [--body <text>] --yes                    # R1(内部 topic 为 R3 + --allow-internal-produce)
```

`peek` 和 `tail` 绝不消费消息、不移动游标,只返回 sha256 指纹(`keySha256`、`bodySha256`、size、可选 timestamp)—— 绝不返回消息体。`tail` 受 `--max-messages` 与 `--timeout` 约束;`--follow` 也只会流式读取到这些边界或取消为止。Tail 支持 Kafka 与 Pulsar;RabbitMQ 与 RocketMQ 上 `tail` 失败关闭(`NOT_IMPLEMENTED`);RocketMQ 上 `peek` 也失败关闭。
</details>

<details>
<summary><b>dlq</b> —— 死信队列治理</summary>

```bash
mqgov dlq list [--topic <source-or-dlq>] [--group <group-or-sub>] [--pattern <name|glob>] -o json     # R0
mqgov dlq peek <dlq> [--topic <source>] [--group <group-or-sub>] [--count N] -o json                   # R0,仅指纹
mqgov dlq redrive <dlq> --target <live-topic> [--count N] --dry-run -o json                            # R0 预览
mqgov dlq redrive <dlq> --target <live-topic> [--count N] --yes --ticket <t> --allow-internal-produce  # R3
mqgov dlq purge <dlq> --dry-run -o json                                                                 # R0 预览
mqgov dlq purge <dlq> --yes --ticket <t> --allow-topic-purge                                           # R3
```

DLQ 映射保持后端原生且诚实:RocketMQ 只列出 `%DLQ%{consumerGroup}` topic;RabbitMQ 把 DLQ 视为 dead-letter exchange 喂入的普通队列;Kafka 没有原生 DLQ,也不自动发现,peek/redrive/purge 只针对用户显式指定的 DLQ topic;Pulsar 使用 `{topic}-{subscription}-DLQ`。不支持的动词返回 `NOT_IMPLEMENTED`。

Redrive 按 internal-produce 治理:dry-run 是只读预览,真实执行需要 `--allow-internal-produce`。审计仍然只记录指纹/计数,永不记录 message body、key、headers。
</details>

<details>
<summary><b>schema</b> —— schema registry</summary>

```bash
mqgov schema list [--pattern <subject>] -o json
mqgov schema describe <subject-or-topic> [--version latest|N] -o json
mqgov schema check <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO [--version latest] -o json
```

`schema list`、`schema describe`、`schema check` 都是 R0 且会审计。`check` 只调用只读兼容性校验端点,绝不注册、删除或演进 schema。Kafka 映射到 Confluent Schema Registry(`GET /subjects`、`GET /subjects/{subject}/versions`、`GET /subjects/{subject}/versions/{version|latest}`、`POST /compatibility/subjects/{subject}/versions/{version}`)。Pulsar 映射到 `/admin/v2/schemas/{tenant}/{namespace}/{topic}` 下的内建 admin schema 端点。RabbitMQ 与 RocketMQ 失败关闭为 `NOT_IMPLEMENTED`。审计只记录 subject/version 元数据和 schema hash,永不记录 schema 全文或 registry 凭据。
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
mqgov ctx set <name> --backend rabbitmq (--amqp-url <url> | --host <h> --port <p> --vhost </>) --management-url <url>
mqgov ctx set <name> --backend pulsar   --service-url pulsar://<h:p> --admin-url http://<h:p> [--tenant public] [--pulsar-namespace default]
mqgov ctx set <name> --backend rocketmq --nameservers <h:p,h:p> [--broker-addr <h:p>]
mqgov ctx use|list|current|delete|test
#   密钥:--password <pw|token|secretKey> 与 --schema-registry-password <pw> 都经 --credential-backend <encrypted-file|keychain|...>(必须用非 plain 后端)

# 审计(防篡改,仅指纹)
mqgov audit query  [--since 24h] [--type <t>] [--operator <o>] [--status <s>] [--limit 100] -o json
mqgov audit verify [--strict] -o json

# 诊断与生态
mqgov doctor -o json            # 只读健康检查(输出已脱敏)
mqgov capabilities -o json      # 绑定后端支持什么
mqgov completion bash|zsh|fish|powershell
mqgov install <agent> --skills  # 把 mqgov AI skill 装进 agent(claude、codex…)或自定义路径
mqgov version
```
</details>

---

## 🤖 给 AI agent

mqgov-cli 设计为可被自治 agent 安全驱动:

- 先跑 `mqgov capabilities -o json` 发现绑定后端支持什么 —— broker 各异,别假设(例如 Kafka、RabbitMQ、Pulsar 都支持 `acl`,但原生模型不同;Kafka 与 Pulsar 支持 `schema`;RabbitMQ/RocketMQ 无 offset、schema registry 或 tail;RocketMQ 无 peek)。需要跨 context 仪表盘时用只读的 `fleet status --all -o json`。
- 处处用 `-o json`;每个命令返回稳定、带版本的信封。
- 爆炸半径来自 `--dry-run` / `--plan`,绝不来自你自己的推理。
- **绝不自填 `--ticket`、`--allow-*` 或高危 `--yes`。** 把所需人工审批上报并停下。

把内置 skill 装进你的 agent,让它自动学会这些规则:

```bash
mqgov install claude --skills     # 也支持:codex、opencode、copilot、cursor、windsurf、aider、cc-switch
```

---

## 🔏 信任与校验

- **签名二进制** —— 每个发布产物用 [cosign](https://github.com/sigstore/cosign) 签名(keyless / OIDC)。`checksums.txt`(同样签名)覆盖所有平台。
- **npm provenance** —— npm 包经 CI 用 OpenID Connect 发布,带 [provenance 证明](https://docs.npmjs.com/generating-provenance-statements),关联到本仓库与此工作流。
- **校验安装** —— npm postinstall 经白名单主机下载二进制,并在安装前对照已签名的 `checksums.txt` 校验 SHA-256。
- **防篡改审计** —— `mqgov audit verify --strict` 重走哈希链,报告任何缺口或改动。
- **传输不裸奔** —— 仅 SASL/TLS 与 mTLS;mqgov 绝不提供 insecure-skip-verify 后门。

---

## 🏗️ 从源码构建与贡献

```bash
git clone https://github.com/JiangHe12/mqgov-cli && cd mqgov-cli
go build ./...
go test -count=1 ./...
gofmt -l main.go cmd internal      # 必须无输出
golangci-lint run --timeout=5m
go vet -tags=integration ./...
```

真后端集成测试(`//go:build integration`,env-gated,默认跳过)在 nightly `integration.yml` 工作流里对活的 Kafka/RabbitMQ/Pulsar/RocketMQ 容器运行;本地用自带的 `docker-compose.*.yml` 跑法见 [`docs/`](docs/)。

mqgov-cli 构建在共享的 [`opskit-core`](https://github.com/JiangHe12/opskit-core) 治理引擎之上,是面向 AI agent 的 **opskit** 受治理 CLI 家族的一员 —— 同族还有 [`dbgov-cli`](https://www.npmjs.com/package/dbgov-cli)(数据库)、[`srvgov-cli`](https://www.npmjs.com/package/srvgov-cli)(远程服务器)、[`cfgov-cli`](https://www.npmjs.com/package/cfgov-cli)(配置中心)。

---

## 📄 许可

[MIT](LICENSE) © JiangHe12
