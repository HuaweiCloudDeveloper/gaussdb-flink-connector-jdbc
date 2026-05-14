<p align="center">
  <h1 align="center">Flink GaussDB CDC Connector 1.17</h1>
  <p align="center">专为 Flink 1.17 设计的 GaussDB CDC 连接器，支持实时捕获数据变更</p>
</p>

## 目录

- [项目介绍](#项目介绍)
- [核心特性](#核心特性)
- [版本兼容性](#版本兼容性)
- [前置条件](#前置条件)
- [快速开始](#快速开始)
- [使用说明](#使用说明)
- [配置参数](#配置参数)
- [CDC 实现原理](#cdc-实现原理)
- [注意事项](#注意事项)
- [常见问题](#常见问题)
- [许可证](#许可证)

## 项目介绍

Flink GaussDB CDC Connector 1.17 是专为 Apache Flink 1.17 版本设计的 GaussDB 变更数据捕获（CDC）连接器，基于 mppdb_decoding 插件解码 WAL 日志，支持实时捕获 GaussDB 数据库的 INSERT、UPDATE、DELETE 操作。

## 核心特性

### 1. 变更数据捕获
- **INSERT**: 实时捕获新插入的数据
- **UPDATE**: 实时捕获更新的数据
- **DELETE**: 实时捕获删除的数据

### 2. WAL 逻辑解码

基于 GaussDB `mppdb_decoding` 插件解码 WAL 日志，毫秒级延迟，要求 `wal_level=logical`。

全量快照（SELECT）→ 增量流式（WAL 解码）两阶段自动衔接。

### 3. WAL 增量模式双通道

WAL 增量阶段支持两种数据通道，Connector 自动选择最优路径：

| 通道 | 实现方式 | 吞吐量 | 输出格式 | 数据流模型 | 环境要求 |
|------|---------|-------|---------|----------|--------|
| **SQL 函数模式**（默认） | pg_logical_slot_peek_changes | ~7,500 rows/s | 固定 JSON | Pull（轮询拉取） | wal_level=logical + 复制槽 |
| **流式复制 API** | JDBC 复制流 | ~19,000 rows/s | binary/json/text | Push + ACK（推送+确认） | 额外需 gs_hba.conf 白名单 + enable_thread_pool=off 或 HA 端口 |

> **说明**：
> - **SQL 函数模式**：通过 `pg_logical_slot_peek_changes` SQL 查询获取 WAL 变更，无需额外数据库配置，兼容性最好。但 `decode-style` 和 `sending-batch` 不被支持，并行解码无实际性能提升（82% 耗时在 JSON 文本传输）
> - **流式复制 API**：通过 JDBC `replication=database` 长连接，GaussDB 主动推送变更到客户端 TCP 缓冲区，客户端用 `readPending()` 非阻塞读取并 `setFlushedLSN + forceUpdateStatus` 确认位点。支持 `decode-style=b`（二进制格式，数据量减半）和 `sending-batch=1`（批量发送），但需要额外配置 gs_hba.conf 白名单

### 4. 模式选择建议

根据数据变更量和运维条件选择合适的模式：

| 场景 | 日变更量 | 推荐模式 | 推荐配置 | 理由 |
|------|---------|---------|---------|------|
| 低频变更 | < 10万行/天 | SQL 函数模式 | 默认即可 | 吞吐量足够，无需额外运维配置 |
| 中频变更 | 10万~100万行/天 | 流式复制 API | `parallel-decode-num=4, decode-style=b, sending-batch=true` | 吞吐量 2.5x 提升，binary 格式数据量减半 |
| 高频变更 | > 100万行/天 | 流式复制 API | `parallel-decode-num=4, decode-style=b, sending-batch=true` | 必须用流式复制 API，SQL 函数模式吞吐量不足 |
| 运维受限 | - | SQL 函数模式 | 默认即可 | 无法配置 gs_hba.conf 白名单时只能用 SQL 函数模式 |
| 首次评估 | - | SQL 函数模式 → 流式复制 API | 先用默认验证功能，再切换 | 先验证功能正确性，再优化性能 |

> **核心结论**：
> - 流式复制 API 吞吐量约为 SQL 函数模式的 **2.5 倍**（~19,000 vs ~7,500 rows/s），主要得益于 Push 模型（无需轮询）+ binary 格式（数据量减半）
> - 并行解码线程数（`parallel-decode-num`）对吞吐量影响不大，**单连接的传输通道才是瓶颈**，推荐值 4 即可
> - 如果运维条件允许（gs_hba.conf 白名单），建议始终优先使用流式复制 API

### 3. 与 JDBC Connector 对比

| 功能 | CDC Connector | JDBC Connector |
|------|--------------|----------------|
| 实时性 | 实时/近实时（Push+ACK 或轮询） | 批处理 |
| 变更类型 | INSERT/UPDATE/DELETE | 仅 INSERT/UPSERT |
| 数据源 | 流式复制 API（推送）/ SQL 函数（轮询） | 被动查询 |
| 资源消耗 | 较高（持续监听） | 较低（按需查询） |

## 版本兼容性

| 连接器版本 | Flink 版本 | GaussDB 版本 | 状态 |
|-----------|-----------|-------------|------|
| flink-connector-gaussdb-cdc-1.17 | 1.17.x | 505.2.1.SPC0800+ | ✅ 推荐 |

## 前置条件

### 系统要求
- **CPU**: 2GHz 或更高
- **RAM**: 4GB 或更大
- **Disk**: 至少 40GB
- **JDK**: 8/11（推荐）

> **说明**：Connector 编译目标为 JDK 8（class file version 52），内置的 GaussDB JDBC 驱动（`gaussdbjdbc-506.0.0.b058-jdk7`）同样兼容 JDK 8+，流式复制 API 可用（PGReplicationStream 编译版本为 JDK 8）。已在 JDK 8（Zulu 1.8.0_482）环境下验证通过。

### 数据库要求

#### WAL 逻辑解码（默认）

**配置 wal_level 为 logical**：

通过 Console 控制台（推荐）
1. 登录 GaussDB Console 控制台
2. 进入「参数管理」→「高危参数」
3. 找到 `wal_level` 参数，修改为 `logical`
4. 重启 GaussDB 实例生效


**流式复制 API 额外配置**（如需并行解码最佳性能）：

GaussDB 的流式复制连接（`replication=database`）需要额外的访问控制配置：

1. **gs_hba.conf 白名单**：需添加 replication 类型的访问规则
   ```
   # 在 gs_hba.conf 中添加（需运维操作，不支持 SQL 修改）
   host    replication    root    <客户端IP>/32    sha256
   ```
2. **enable_thread_pool**：当 `enable_thread_pool=on`（集中式默认）时，复制连接需走 HA 端口（数据端口+1，如 8001）。如需走数据端口 8000，需关闭该参数（需重启实例）

> **说明**：如无法完成以上配置，Connector 会自动回退到 SQL 函数模式，功能完整但并行解码性能受限。

配置完成后，创建复制槽：
```sql
-- 1. 确认用户有复制权限
ALTER USER root REPLICATION;

-- 2. 创建复制槽（使用 GaussDB 原生 mppdb_decoding 插件）
SELECT pg_create_logical_replication_slot('flink_cdc_slot', 'mppdb_decoding');
```

**注意**：GaussDB 使用 `mppdb_decoding` 插件而非 PostgreSQL 的 `pgoutput`，数据以 JSON 格式返回。

### 依赖 JAR 包

GaussDB CDC Connector 已内置 GaussDB JDBC 驱动，部署时只需以下 JAR 包：

1. **Flink Connector Base** (必选)
   - `flink-connector-base-1.17.2.jar`

2. **GaussDB CDC Connector** (必选，已包含 JDBC 驱动)
   - `flink-connector-gaussdb-cdc-1.17-3.3.0-1.20.jar`

> **说明**：Connector jar 已通过 maven-shade-plugin 内置以下依赖，无需单独部署：
> - `gaussdbjdbc-506.0.0.b058-jdk7.jar`（GaussDB JDBC 驱动，兼容 JDK 8/11，支持流式复制 API）

### Maven 依赖

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb.flink</groupId>
    <artifactId>flink-connector-gaussdb-cdc-1.17</artifactId>
    <version>3.3.0-1.20</version>
</dependency>
```

## 快速开始

### 1. 部署 JAR 包

```bash
# 1. Flink Connector Base
cp flink-connector-base-1.17.2.jar $FLINK_HOME/lib/

# 2. GaussDB CDC Connector（已内置 GaussDB JDBC 驱动）
cp flink-connector-gaussdb-cdc-1.17-3.3.0-1.20.jar $FLINK_HOME/lib/
```

重启 Flink：

```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

### 2. SQL 方式使用 CDC Connector

#### SQL 函数模式（默认，零额外配置）

无需 gs_hba.conf 白名单、无需 HA 端口，只需 `wal_level=logical` 和复制槽即可使用。

**1. 配置 GaussDB 数据库**

```sql
-- 1. 确认用户有复制权限
ALTER USER root REPLICATION;

-- 2. 创建复制槽（使用 mppdb_decoding 插件）
SELECT pg_create_logical_replication_slot('flink_cdc_slot', 'mppdb_decoding');
```

> 如果 GaussDB 的 `wal_level` 不是 `logical`，需先修改：
> - **Console 控制台**：参数管理 → 高危参数 → 修改 `wal_level` 为 `logical` → 重启实例
> - **命令行**：`gs_guc reload -Z datanode -N all -I all -c "wal_level=logical"` → 重启 GaussDB

**2. 在 Flink SQL 中配置 CDC 源表**

```sql
-- SQL 函数模式（默认）
CREATE TABLE student_cdc (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database' = 'test',
    'schema' = 'public',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',

    -- ★ WAL 模式开关
    'wal.mode' = 'true',
    'decode.plugin' = 'mppdb_decoding',
    'slot.name' = 'flink_cdc_slot'
);
```

> **注意**：SQL 函数模式下 `parallel-decode-num`、`decode-style`、`sending-batch` 参数均无效。`decode-style` 和 `sending-batch` 会报 `Option unknown` 错误，Connector 会自动跳过。`parallel-decode-num` 可传入但无性能提升（82% 耗时在 JSON 文本传输）。

#### 流式复制 API 模式（并行解码性能最优）

需额外配置 gs_hba.conf 白名单和 enable_thread_pool（见[前置条件](#前置条件)）。

```sql
-- 流式复制 API 模式
CREATE TABLE student_cdc_wal (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database' = 'test',
    'schema' = 'public',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',

    -- ★ WAL 模式开关
    'wal.mode' = 'true',

    -- ★ 并行解码配置（流式复制 API 下生效）
    'parallel-decode-num' = '4',     -- 并行解码线程数，推荐 4
    'decode.plugin' = 'mppdb_decoding',
    'slot.name' = 'flink_cdc_slot',

    -- 以下参数仅流式复制 API 模式生效
    'decode-style' = 'b',            -- binary 格式
    'sending-batch' = 'true'         -- 批量发送
);
```

**并行解码配置速查**

| 参数 | SQL 函数模式 | 流式复制 API 模式 | 说明 |
|------|------------|----------------|------|
| `wal.mode` | `true` | `true` | 必须开启 |
| `parallel-decode-num` | 可传但无提升 | ✅ 生效 | 并行线程数，2~20 为并行 |
| `decode-style` | ❌ 报错自动跳过 | ✅ 生效 | 输出格式：b=binary，j=json，t=text |
| `sending-batch` | ❌ 报错自动跳过 | ✅ 生效 | 批量发送，累积 1MB |
| `slot.name` | 必须与数据库复制槽对应 | 必须与数据库复制槽对应 | 复制槽名称 |

> **性能对比（10万行 INSERT 实测）**：
>
> **测试模型**：`id SERIAL, name VARCHAR(100), age INT, score DECIMAL(10,2), remark VARCHAR(200)`，单行数据约 80~100 字节。实际吞吐量与行大小密切相关，请以实际业务数据为准。
>
> | 模式 | parallel-decode-num | decode-style | sending-batch | 吞吐量 | 数据量 |
> |------|-------------------|-------------|--------------|--------|-------|
> | SQL 函数模式 | 1 | 固定 JSON | 不支持 | ~7,900 rows/s | - |
> | SQL 函数模式 | 4 | 固定 JSON | 不支持 | ~7,500 rows/s | - |
> | SQL 函数模式 | 8 | 固定 JSON | 不支持 | ~7,400 rows/s | - |
> | 流式复制 API | 1 | j | false | **~19,200 rows/s** | 31.92 MB |
> | 流式复制 API | 4 | b | true | **~17,800 rows/s** | binary（约 JSON 一半） |
> | 流式复制 API | 8 | b | true | **~16,700 rows/s** | binary（约 JSON 一半） |
>
> **结论**：流式复制 API 整体吞吐量约为 SQL 函数模式的 2~2.5 倍。并行解码线程数增加对吞吐量提升有限，单连接传输通道是瓶颈。推荐配置 `parallel-decode-num=4` 即可。
>
> ⚠️ **重要**：以上数据基于轻量行（~80 字节/行）的 INSERT 场景。客户实际业务行通常更大（500 字节~数 KB），网络传输占比更高，流式复制 API 的 binary 格式优势会更明显。建议客户基于实际业务数据量自行验证。

### 3. 实时捕获变更示例

```sql
-- 创建 CDC 源表
CREATE TABLE student_cdc (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database' = 'test',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',
    'wal.mode' = 'true',
    'slot.name' = 'flink_cdc_slot'
);

-- 创建目标表
CREATE TABLE student_target (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:gaussdb://localhost:8000/test?sslmode=disable',
    'table-name' = 'student_backup',
    'username' = 'root',
    'password' = 'password',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver'
);

-- 实时同步数据
INSERT INTO student_target SELECT * FROM student_cdc;
```

## 使用说明

### 表结构要求

```sql
CREATE TABLE student (
    id INT PRIMARY KEY,
    name VARCHAR(100)
    -- 无需特殊字段
);
```

## 配置参数

### 通用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| connector | 是 | - | 连接器类型：`gaussdb-cdc` |
| hostname | 是 | - | GaussDB 主机地址 |
| port | 否 | 8000 | GaussDB 端口 |
| database | 是 | - | 数据库名 |
| schema | 否 | public | Schema 名称 |
| table-name | 是 | - | 表名 |
| username | 是 | - | 用户名 |
| password | 是 | - | 密码 |
| sslmode | 否 | prefer | SSL 连接模式，支持：disable、allow、prefer（默认）、require、verify-ca、verify-full |

### CDC 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| wal.mode | 否 | true | 是否启用 WAL 逻辑解码模式 |
| decode.plugin | 否 | mppdb_decoding | 逻辑解码插件名称。支持：mppdb_decoding（默认）、pgoutput |
| slot.name | 否 | flink_cdc_slot | 复制槽名称 |
| snapshot.mode | 否 | true | 是否先读取全量快照 |
| chunk.size | 否 | 1000 | 分块大小 |

### 并行解码参数（WAL 模式）

| 参数 | SQL 函数模式 | 流式复制 API 模式 | 默认值 | 说明 |
|-----|------------|----------------|-------|------|
| parallel-decode-num | 可传但无提升 | ✅ 生效 | 1 | 并行解码线程数，范围 1~20 |
| decode-style | ❌ 报错自动跳过 | ✅ 生效 | b | 输出格式：`b`=binary，`j`=json，`t`=text |
| sending-batch | ❌ 报错自动跳过 | ✅ 生效 | false | 批量发送，true=累积到 1MB 后发送 |

> **重要**：SQL 函数模式下 `decode-style` 和 `sending-batch` 不被 mppdb_decoding 识别（会报 `Option unknown` 错误），Connector 会自动跳过。
> `parallel-decode-num` 可传入但无性能提升（82% 耗时在 JSON 文本传输，解码仅占 18%）。
> 并行解码性能提升需启用流式复制 API 模式（需配置 gs_hba.conf 白名单）。

### SSL 连接模式

Connector 支持通过 `sslmode` 参数配置 SSL 加密连接，参数值会传递到 GaussDB JDBC 驱动的连接 URL 中（`?sslmode=<value>`）。

| 值 | 说明 |
|---|------|
| disable | 不使用 SSL |
| allow | 优先非 SSL，失败时尝试 SSL |
| prefer | 优先 SSL，失败时回退非 SSL（**默认**） |
| require | 必须使用 SSL，但不验证服务器证书 |
| verify-ca | 必须使用 SSL，验证服务器证书由可信 CA 签发 |
| verify-full | 必须使用 SSL，验证服务器证书由可信 CA 签发且 CN 匹配主机名 |

使用示例：

```sql
CREATE TABLE student_cdc (
    id INT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database' = 'test',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',
    'sslmode' = 'require'
);
```

> **说明**：默认值 `prefer` 与 GaussDB JDBC 驱动默认行为一致，无需额外配置。如数据库要求 SSL 连接，请设置为 `require` 或更高安全级别。

### Source 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| scan.fetch-size | 否 | 0 | 每次读取行数 |
| connect.timeout.ms | 否 | 30000 | 连接超时时间 |

### CDC 实现原理

### WAL 逻辑解码

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Flink     │◀────│  逻辑复制流       │◀────│   GaussDB   │
│   CDC Source│     │(mppdb_decoding)  │     │    WAL      │
└─────────────┘     │  并行解码引擎     │     └─────────────┘
        │           └──────────────────┘
        ▼
┌─────────────┐
│ INSERT/     │
│ UPDATE/     │
│ DELETE      │
│ JSON 事件   │
└─────────────┘
```

#### WAL 增量双通道架构

```
                  ┌─────────────────────────┐
                  │  WalReplicationStream    │
                  └────────┬────────────────┘
                           │
              ┌────────────▼────────────┐
              │  尝试流式复制 API        │
              │  (PGReplicationStream)   │
              │  需要 gs_hba.conf 白名单  │
              └────────────┬────────────┘
                     成功   │     失败
              ┌────────────▼────────────┐
              │  回退到 SQL 函数模式     │
              │  pg_logical_slot_peek_   │
              │  changes + parallel-decode│
              └─────────────────────────┘
```

- **流式复制 API 模式**：通过 JDBC `replication=database` 连接，支持 `decode-style`（binary/json/text）和 `sending-batch`（批量发送）参数，**并行解码性能最优**
  - 需求：gs_hba.conf 添加 replication 白名单 + enable_thread_pool=off 或 HA 端口可达
- **SQL 函数模式**：通过 `pg_logical_slot_peek_changes` SQL 查询，仅 `parallel-decode-num` 参数有效，输出固定为 JSON 格式
  - 瓶颈：82% 耗时在 JSON 文本传输，并行解码无性能提升
  - 优势：无需额外配置，兼容性最好

### WAL 数据格式示例

#### 并行解码 JSON 格式（SQL 函数模式默认输出）

```json
{
  "table_name": "public.flink_test_student",
  "op_type": "INSERT",
  "columns_name": ["id", "name", "gender", "age", "updated_at"],
  "columns_type": ["integer", "character varying", "character varying", "integer", "timestamp without time zone"],
  "columns_val": ["1", "'Alice'", "'F'", "20", "'2026-04-09 14:36:02'"],
  "old_keys_name": [],
  "old_keys_type": [],
  "old_keys_val": []
}
```

#### UPDATE 事件示例

```json
{
  "table_name": "public.flink_test_student",
  "op_type": "UPDATE",
  "columns_name": ["id", "name", "gender", "age", "updated_at"],
  "columns_type": ["integer", "character varying", "character varying", "integer", "timestamp without time zone"],
  "columns_val": ["1", "'Bob'", "'M'", "21", "'2026-04-09 14:40:00'"],
  "old_keys_name": ["id"],
  "old_keys_type": ["integer"],
  "old_keys_val": ["1"]
}
```

#### DELETE 事件示例

```json
{
  "table_name": "public.flink_test_student",
  "op_type": "DELETE",
  "columns_name": ["id", "name", "gender", "age", "updated_at"],
  "columns_type": ["integer", "character varying", "character varying", "integer", "timestamp without time zone"],
  "columns_val": ["1", "'Bob'", "'M'", "21", "'2026-04-09 14:40:00'"],
  "old_keys_name": ["id"],
  "old_keys_type": ["integer"],
  "old_keys_val": ["1"]
}
```

| 字段 | 说明 |
|------|------|
| `table_name` | 表名（含 schema） |
| `op_type` | 操作类型：INSERT/UPDATE/DELETE/BEGIN/COMMIT |
| `columns_name` | 列名列表 |
| `columns_type` | 列类型列表 |
| `columns_val` | 列值列表 |
| `old_keys_name` | 旧主键列名列表（UPDATE/DELETE 时有效） |
| `old_keys_type` | 旧主键类型列表 |
| `old_keys_val` | 旧主键值列表 |

## 注意事项

1. **WAL 模式要求**：
   - GaussDB 必须设置 `wal_level = logical`
   - 用户必须有 `REPLICATION` 权限
   - 需要创建逻辑复制槽
   - 流式复制 API 模式需要 gs_hba.conf 配置 replication 白名单（不支持 SQL 修改，需运维操作）

2. **GaussDB 与 PostgreSQL API 差异**：
   - `pg_current_xlog_location()` vs PostgreSQL 的 `pg_current_wal_lsn()` — Connector 自动兼容
   - `pg_replication_slot_advance()` vs PostgreSQL 的 `pg_logical_slot_advance()` — Connector 自动兼容
   - `pg_logical_slot_peek_changes` 返回列名为 `location`（非 `lsn`）— Connector 自动处理
   - `pg_logical_slot_peek_changes` 传入特定 LSN 参数后 `pg_replication_slot_advance` 会返回空结果 — Connector 始终用 NULL 作为 LSN 参数
   - `decode-style` 和 `sending-batch` 选项仅在流式复制 API 模式下有效，SQL 函数模式不支持（报 `Option unknown` 错误）
   - SQL 函数模式下 `parallel-decode-num` 可传入但无性能提升（瓶颈在 JSON 文本传输）
   - 集中式 GaussDB `enable_thread_pool=on` 时，流式复制连接需走 HA 端口（数据端口+1）

3. **类加载器配置**：如果遇到 `ClassNotFoundException`，请确保 Flink 配置为 `parent-first` 类加载策略：
   ```yaml
   # flink-conf.yaml
   classloader.resolve-order: parent-first
   ```

4. **字符编码**：如果遇到乱码问题，可在 JDBC URL 中添加字符编码参数：
   ```
   jdbc:gaussdb://<host>:<port>/<database>?compatibleMode=mysql&characterEncoding=UTF-8
   ```

5. **流式复制 API 环境配置**：Connector 内置的 `gaussdbjdbc-506.0.0.b058-jdk7` 驱动已包含 `PGReplicationStream`（编译版本 JDK 8），JDK 11 即可使用流式复制 API。但流式复制连接（`replication=database`）需要：
   - **gs_hba.conf 白名单**：添加 `host replication <user> <IP>/32 sha256`（需运维操作，不支持 SQL 修改）
   - **enable_thread_pool**：集中式默认 `on`，此时复制连接需走 HA 端口（数据端口+1，如 8001）。若 HA 端口不可达，需关闭 `enable_thread_pool`（postmaster 级参数，需重启实例）
   - 如无法完成以上配置，Connector 自动回退到 SQL 函数模式

### ⚠️ 重要限制

**WAL 模式需要数据库配置支持**

如需使用 WAL 模式，需要将 `wal_level` 设置为 `logical`：

- **Console 控制台**：参数管理 → 高危参数 → 修改 `wal_level` 为 `logical` → 重启实例
- **命令行**：`gs_guc reload -Z datanode -N all -I all -c "wal_level=logical"` → 重启 GaussDB

**注意**：修改后必须重启 GaussDB 才能生效。

**GaussDB 与 PostgreSQL 差异**：
- GaussDB 使用 `mppdb_decoding` 插件，数据以 JSON 格式返回
- PostgreSQL 使用 `pgoutput` 插件，数据以二进制格式返回
- GaussDB 使用 `pg_current_xlog_location()`，PostgreSQL 使用 `pg_current_wal_lsn()`
- GaussDB 使用 `pg_replication_slot_advance()`，PostgreSQL 使用 `pg_logical_slot_advance()`
- Connector 已内置自动兼容逻辑，用户无需手动适配

**并行解码**：
- GaussDB mppdb_decoding 支持 `parallel-decode-num`（1~20）并行解码线程
- 流式复制 API 模式支持 `decode-style`（b/j/t）和 `sending-batch` 参数，**并行解码性能最优**
- SQL 函数模式仅 `parallel-decode-num` 可传入，但因 `decode-style`/`sending-batch` 不支持，并行解码无实际性能提升
- **推荐配置**：`parallel-decode-num=4`，`decode-style=b`，`sending-batch=true`（需流式复制 API 环境）
- `parallel-decode-num=1` 不支持 `decode-style=b`，会报 `Option unknown` 错误

**流式复制 API 流控机制**：
- 流式复制 API 采用 Push + ACK 模型：GaussDB 主动推送变更到客户端，客户端用 `setFlushedLSN + forceUpdateStatus` 确认消费位点后服务端才继续发送
- Connector 使用 `readPending()` 非阻塞读取，不会因等待数据而阻塞线程
- 空读时由上层 `GaussDBSourceReader` 控制 `pollIntervalMs` 等待间隔，再由 Flink 框架调度重试

如无法修改配置，Connector 将使用 SQL 函数模式（功能完整，并行解码性能受限）。

## 常见问题

### Q: CDC 能捕获所有变更吗？

A: WAL 模式可以捕获所有 INSERT/UPDATE/DELETE 变更。

### Q: 如何配置 CDC？

A: 使用 `wal.mode=true` 启用 WAL 逻辑解码：

```sql
CREATE TABLE my_table_cdc (
    id INT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '1.92.120.69',
    'port' = '8000',
    'database' = 'test',
    'table-name' = 'my_table',
    'username' = 'root',
    'password' = 'password',
    'wal.mode' = 'true',
    'parallel-decode-num' = '4'
);
```

### Q: 并行解码和串行解码有什么区别？

A:
- **串行解码** (`parallel-decode-num=1`)：单线程解码，简单可靠，仅支持 JSON 格式（decode-style=b 不被支持）
- **并行解码** (`parallel-decode-num>1`)：多线程并行解码，支持 binary 格式，适合高负载场景
- 并行解码输出格式由 `decode-style` 控制：`b`=binary（推荐，数据量约 JSON 一半）、`j`=json、`t`=text
- **推荐生产环境使用 `parallel-decode-num=4`**，增加线程数对吞吐量提升有限（单连接传输通道是瓶颈）

### Q: 为什么流式复制 API 连接报 `replication should connect HA port in thread_pool`？

A: 集中式 GaussDB 默认开启 `enable_thread_pool=on`，此模式下 `replication=database` 连接必须走 HA 端口（数据端口+1，如 8001），数据端口（8000）不接受复制连接。

解决方案：
- **方案一**：开放 HA 端口（8001），流式复制连接使用该端口
- **方案二**：关闭 `enable_thread_pool`（需修改配置文件后重启实例，影响连接池性能）
- **方案三**：不做配置，Connector 自动回退到 SQL 函数模式

### Q: 为什么流式复制 API 连接报 `No gs_hba.conf entry for replication connection`？

A: GaussDB 的 `gs_hba.conf` 访问控制文件中没有允许 replication 类型连接的规则。

解决方案：需联系运维在 `gs_hba.conf` 中添加白名单规则（不支持 SQL 修改）：
```
host    replication    root    <客户端IP>/32    sha256
```

### Q: SQL 函数模式下并行解码为什么没有性能提升？

A: 经实测分析，SQL 函数模式下 82% 的耗时在 JSON 文本数据传输，服务端解码仅占 18%。多线程解码优化的 18% 部分被传输瓶颈掩盖。

原因：`pg_logical_slot_peek_changes` 是标准 SQL 查询，结果通过单个 ResultSet 返回，输出固定为 JSON 文本格式（每行平均 325 字节），不支持 `decode-style`（二进制格式）和 `sending-batch`（批量发送）。

要发挥并行解码性能，需启用流式复制 API 模式（支持 binary 格式 + 批量发送）。

### Q: decode-style 和 sending-batch 不生效？

A: 这两个参数仅在使用 GaussDB 流式复制 API 模式时有效。SQL 函数模式下 mppdb_decoding 插件不识别这两个参数（会报 `Option "decode-style" = "b" is unknown` 错误），Connector 会自动跳过。

要启用流式复制 API，需确保：
1. gs_hba.conf 配置了 replication 白名单
2. enable_thread_pool=off 或 HA 端口（数据端口+1）可达

### Q: 并行解码 binary 模式增量同步不工作？

A: 已修复。`MppdbBinaryDecoder` 存在偏移计算 bug：mppdb_decoding 的 binary 格式中每条记录后有一个 1 字节分隔符（'P'=更多记录，'F'=batch结束），但 `totalSize` 字段不包含此分隔符。修复后正确处理分隔符，binary 模式增量同步已恢复正常。

### Q: 串行解码（parallel-decode-num=1）增量同步不工作？

A: 已修复。`parallel-decode-num=1` 时 `buildSlotOptions()` 不传 `decode-style` 给 slot，mppdb_decoding 默认输出 JSON 格式，但代码误用 `MppdbBinaryDecoder` 解码。修复后串行模式走 JSON 解析，增量同步已恢复正常。

### Q: CDC 会影响数据库性能吗？

A:
- **SQL 函数模式**：每次 `pg_logical_slot_peek_changes` 查询会产生数据库负载，轮询间隔越短影响越大
- **流式复制 API**：影响最小，GaussDB 主动推送 WAL 变更，客户端非阻塞读取，不产生额外查询负载
- 两种模式都需要维持逻辑复制槽，未消费的 WAL 日志不会回收，需确保消费速度跟上生产速度

### Q: 如何监控 CDC 延迟？

A: 可以通过 Flink 监控查看：
```sql
-- 查看当前消费位置
SHOW CREATE TABLE student_cdc;
```

### Q: 如何更换 GaussDB JDBC 驱动版本？

A: 修改模块 `pom.xml` 中的驱动版本属性，然后重新打包即可：

1. 修改版本号（当前版本为 `506.0.0.b058-jdk7`）：
```xml
<!-- flink-connector-gaussdb-cdc-1.17/pom.xml 第19行 -->
<gaussdb.jdbc.version>506.0.0.b058-jdk7</gaussdb.jdbc.version>
<!-- 改为目标版本，例如 -->
<gaussdb.jdbc.version>505.2.1.SPC0800</gaussdb.jdbc.version>
```

2. 重新打包并部署：
```bash
mvn clean package -pl flink-connector-gaussdb-cdc-1.17 -DskipTests -Dcheckstyle.skip=true
cp flink-connector-gaussdb-cdc-1.17/target/flink-connector-gaussdb-cdc-1.17-3.3.0-1.20.jar $FLINK_HOME/lib/
# 重启 Flink 集群
```

**注意事项：**
- 如目标版本不在 Maven 中央仓库，需先通过 `mvn install:install-file` 安装到本地仓库，或配置私有仓库地址
- 确认新驱动的 JDBC 驱动类名是否仍为 `com.huawei.gaussdb.jdbc.Driver`
- 注意 JDK 兼容性：`jdk7` 后缀表示编译目标为 JDK 7，Flink 1.17 运行在 JDK 11，需确认新驱动在 JDK 11 下可正常工作

## 许可证

本项目基于 Apache License 2.0 开源许可证。
