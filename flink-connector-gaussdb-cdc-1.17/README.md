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

Flink GaussDB CDC Connector 1.17 是专为 Apache Flink 1.17 版本设计的 GaussDB 变更数据捕获（CDC）连接器，支持实时捕获 GaussDB 数据库的 INSERT、UPDATE、DELETE 操作。

## 核心特性

### 1. 变更数据捕获
- **INSERT**: 实时捕获新插入的数据
- **UPDATE**: 实时捕获更新的数据（需 `updated_at` 字段）
- **DELETE**: 实时捕获删除的数据（通过快照对比）

### 2. 两种 CDC 模式

| 模式 | 实现方式 | 延迟 | 数据库要求 |
|------|---------|------|------------|
| **轮询式 (Polling)** | 定时查询数据库 | 秒级 | 无特殊要求 |
| **WAL 式 (Streaming)** | 监听 WAL 日志 + 并行解码 | 毫秒级 | wal_level=logical |

### 3. 并行解码

WAL 模式支持 GaussDB mppdb_decoding 并行逻辑解码，可显著提升解码吞吐量：

| 特性 | 说明 |
|------|------|
| **并行解码线程** | 支持 1~20 个并行解码线程（parallel-decode-num） |
| **输出格式** | 支持 binary(b)、json(j)、text(t) 三种格式 |
| **批量发送** | 支持批量发送模式，累积到 1MB 后发送（sending-batch） |
| **兼容模式** | 自动适配 GaussDB / PostgreSQL API 差异 |

### 3. 与 JDBC Connector 对比

| 功能 | CDC Connector | JDBC Connector |
|------|--------------|----------------|
| 实时性 | 实时/近实时 | 批处理 |
| 变更类型 | INSERT/UPDATE/DELETE | 仅 INSERT/UPSERT |
| 数据源 | 主动推送 | 被动查询 |
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
- **JDK**: 8/11（默认，使用内置 jdk7 兼容版驱动）/ 17+（如需流式复制 API）

> **说明**：Connector 内置的 GaussDB JDBC 驱动（`gaussdbjdbc-506.0.0.b058-jdk7`）兼容 JDK 8/11，可直接使用。如需使用 GaussDB 流式复制 API（更低延迟），需 JDK 17+ 并替换为 `gaussdbjdbc-506.0.0.b058.jar`。

### 数据库要求

#### 轮询式 CDC（默认）
- 无特殊要求
- 需要表有主键
- UPDATE 检测需要 `updated_at` 时间戳字段

#### WAL 式 CDC（推荐）

**配置 wal_level 为 logical**：

方式一：通过 Console 控制台（推荐）
1. 登录 GaussDB Console 控制台
2. 进入「参数管理」→「高危参数」
3. 找到 `wal_level` 参数，修改为 `logical`
4. 重启 GaussDB 实例生效

方式二：命令行（需要运维权限）
```bash
# 在 GaussDB 节点执行
gs_guc reload -Z datanode -N all -I all -c "wal_level=logical"

# 重启 GaussDB 生效
gs_om -t restart
```

配置完成后，创建复制槽：
```sql
-- 1. 确认用户有复制权限
ALTER USER root REPLICATION;

-- 2. 创建复制槽（使用 GaussDB 原生 mppdb_decoding 插件）
SELECT pg_create_logical_replication_slot('flink_cdc_slot', 'mppdb_decoding');
```

**注意**：GaussDB 使用 `mppdb_decoding` 插件而非 PostgreSQL 的 `pgoutput`，数据以 JSON 格式返回。

### 依赖 JAR 包

GaussDB CDC Connector 已内置 GaussDB JDBC 驱动和 PostgreSQL JDBC 驱动，部署时只需以下 JAR 包：

1. **Flink Connector Base** (必选)
   - `flink-connector-base-1.17.2.jar`

2. **GaussDB CDC Connector** (必选，已包含 JDBC 驱动)
   - `flink-connector-gaussdb-cdc-1.17-4.0-SNAPSHOT.jar`

> **说明**：Connector jar 已通过 maven-shade-plugin 内置以下依赖，无需单独部署：
> - `gaussdbjdbc-506.0.0.b058-jdk7.jar`（GaussDB JDBC 驱动，兼容 JDK 8/11）
> - `postgresql-42.6.0.jar`（PostgreSQL JDBC 驱动，WAL 模式使用）
>
> 如果需要使用流式复制 API（需 JDK 17+），可将 Flink lib 目录下的内置驱动替换为 `gaussdbjdbc-506.0.0.b058.jar`（JDK 17 版本）。

### Maven 依赖

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb.flink</groupId>
    <artifactId>flink-connector-gaussdb-cdc-1.17</artifactId>
    <version>4.0-SNAPSHOT</version>
</dependency>
```

## 快速开始

### 1. 部署 JAR 包

```bash
# 1. Flink Connector Base
cp flink-connector-base-1.17.2.jar $FLINK_HOME/lib/

# 2. GaussDB CDC Connector（已内置 GaussDB JDBC 和 PostgreSQL JDBC 驱动）
cp flink-connector-gaussdb-cdc-1.17-4.0-SNAPSHOT.jar $FLINK_HOME/lib/
```

重启 Flink：

```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

### 2. SQL 方式使用 CDC Connector

#### 轮询式 CDC（默认）

```sql
-- 创建 GaussDB CDC 源表（默认轮询模式）
CREATE TABLE student_cdc (
    id INT,
    name STRING,
    gender STRING,
    age INT,
    updated_at TIMESTAMP(3),
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
    'slot.name' = 'flink_cdc_slot',
    'snapshot.mode' = 'true',
    'poll.interval.ms' = '1000'
);

-- 查询 CDC 数据
SELECT * FROM student_cdc;
```

#### WAL 式 CDC + 并行解码（推荐）

**启用并行解码的完整步骤**：

**1. 配置 GaussDB 数据库**（需要运维权限）

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
-- 创建 WAL 模式 CDC 源表（推荐，毫秒级延迟）
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

    -- ★ WAL 模式开关（必须）
    'wal.mode' = 'true',

    -- ★ 并行解码配置（推荐）
    'parallel-decode-num' = '4',     -- 并行解码线程数，1=串行，2~20=并行，推荐 4
    'decode.plugin' = 'mppdb_decoding',
    'slot.name' = 'flink_cdc_slot',

    -- 以下参数仅 JDK 17+ 流式复制 API 模式生效，JDK 8/11 环境下自动忽略
    'decode-style' = 'b',            -- binary 格式
    'sending-batch' = 'true'         -- 批量发送
);

-- 查询 CDC 数据
SELECT * FROM student_cdc_wal;
```

**3. 并行解码配置速查**

| 参数 | 串行解码（默认） | 并行解码（推荐） | 说明 |
|------|----------------|----------------|------|
| `wal.mode` | `true` | `true` | 必须开启 |
| `parallel-decode-num` | `1` | `4` | 并行线程数，2~20 为并行 |
| `decode.plugin` | `mppdb_decoding` | `mppdb_decoding` | GaussDB 原生解码插件 |
| `slot.name` | `flink_cdc_slot` | `flink_cdc_slot` | 与数据库中创建的复制槽对应 |

> **说明**：
> - `parallel-decode-num` 默认值为 1（串行解码），**需要显式设置 >1 才能启用并行解码**
> - `decode-style` 和 `sending-batch` 仅在使用 GaussDB JDBC 流式复制 API（需 JDK 17+ 驱动）时生效
> - 当前 Connector 内置的 `gaussdbjdbc-jdk7` 驱动不支持流式复制 API，WAL 模式自动回退到 SQL 函数模式，此时仅 `parallel-decode-num` 有效，输出为 JSON 格式
> - 如需流式复制 API 全部功能（binary 格式、批量发送），需替换为 `gaussdbjdbc-506.0.0.b058.jar`（需 JDK 17+）

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
    'password' = 'password'
);

-- 创建目标表
CREATE TABLE student_target (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:gaussdb://localhost:8000/test?compatibleMode=mysql',
    'table-name' = 'student_backup',
    'username' = 'root',
    'password' = 'password',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver'
);

-- 实时同步数据
INSERT INTO student_target SELECT * FROM student_cdc;
```

## 使用说明

### CDC 模式选择

| 场景 | 推荐模式 | 说明 |
|------|---------|------|
| 云端 GaussDB | 轮询式 | 通常无 WAL 逻辑复制权限 |
| 自建 GaussDB | WAL 式 | 实时性更好，资源消耗更低 |
| 低频变更 | 轮询式 | 配置较大的 poll interval |
| 高频变更 | WAL 式 | 毫秒级延迟 |

### 表结构要求

#### 轮询式 CDC
```sql
CREATE TABLE student (
    id INT PRIMARY KEY,           -- 必须：用于检测 INSERT/DELETE
    name VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 推荐：用于检测 UPDATE
);
```

#### WAL 式 CDC
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

### CDC 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| wal.mode | 否 | false | 是否启用 WAL 逻辑解码模式。false=轮询式，true=WAL 式 |
| decode.plugin | 否 | mppdb_decoding | 逻辑解码插件名称。支持：mppdb_decoding（默认）、pgoutput |
| slot.name | 否 | flink_cdc_slot | 复制槽名称（WAL 模式） |
| snapshot.mode | 否 | true | 是否先读取全量快照 |
| poll.interval.ms | 否 | 1000 | 轮询间隔（轮询式） |
| chunk.size | 否 | 1000 | 分块大小 |

### 并行解码参数（WAL 模式）

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| parallel-decode-num | 否 | 1 | 并行解码线程数，范围 1~20。1=串行解码，>1=并行解码 |
| decode-style | 否 | b | 解码输出格式：`b`=binary（默认），`j`=json，`t`=text。仅流式复制 API 模式生效 |
| sending-batch | 否 | false | 是否批量发送解码结果。true=累积到 1MB 后发送。仅流式复制 API 模式生效 |

> **注意**：`decode-style` 和 `sending-batch` 仅在使用 GaussDB JDBC 流式复制 API 时生效。当驱动不支持流式复制 API 而回退到 SQL 函数模式时，仅 `parallel-decode-num` 有效，输出格式为 JSON。

### Source 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| scan.fetch-size | 否 | 0 | 每次读取行数 |
| connect.timeout.ms | 否 | 30000 | 连接超时时间 |

## CDC 实现原理

### 轮询式 CDC

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Flink     │────▶│   定时查询   │────▶│   GaussDB   │
│   CDC Source│     │  (INSERT)   │     │   max(id)   │
└─────────────┘     ├─────────────┤     └─────────────┘
                    │  (UPDATE)   │
                    │ updated_at  │
                    ├─────────────┤
                    │  (DELETE)   │
                    │  快照对比   │
                    └─────────────┘
```

### WAL 式 CDC

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

#### WAL 模式双通道架构

```
                  ┌─────────────────────────┐
                  │  WalReplicationStream    │
                  └────────┬────────────────┘
                           │
              ┌────────────▼────────────┐
              │  尝试流式复制 API        │
              │  (PGReplicationStream)   │
              │  需要 JDK 17+ JDBC 驱动  │
              └────────────┬────────────┘
                     成功   │     失败
              ┌────────────▼────────────┐
              │  回退到 SQL 函数模式     │
              │  pg_logical_slot_peek_   │
              │  changes + parallel-decode│
              └─────────────────────────┘
```

- **流式复制 API 模式**：通过 JDBC 流式复制连接，支持 binary/json/text 输出格式，支持 `decode-style` 和 `sending-batch` 参数，延迟最低
- **SQL 函数模式**：通过 `pg_logical_slot_peek_changes` SQL 查询，仅支持 `parallel-decode-num` 参数，输出为 JSON 格式，兼容性最好

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
   - 流式复制 API 模式需要 JDK 17+ 环境

2. **GaussDB 与 PostgreSQL API 差异**：
   - `pg_current_xlog_location()` vs PostgreSQL 的 `pg_current_wal_lsn()` — Connector 自动兼容
   - `pg_replication_slot_advance()` vs PostgreSQL 的 `pg_logical_slot_advance()` — Connector 自动兼容
   - `pg_logical_slot_peek_changes` 返回列名为 `location`（非 `lsn`）— Connector 自动处理
   - `decode-style` 和 `sending-batch` 选项仅在流式复制 API 模式下有效，SQL 函数模式不支持
   - SQL 函数模式下 `parallel-decode-num` 有效，输出 JSON 格式

3. **轮询式限制**：
   - DELETE 检测需要全表扫描，性能较差
   - UPDATE 检测依赖 `updated_at` 字段
   - 两次轮询间的变更可能丢失

4. **类加载器配置**：如果遇到 `ClassNotFoundException`，请确保 Flink 配置为 `parent-first` 类加载策略：
   ```yaml
   # flink-conf.yaml
   classloader.resolve-order: parent-first
   ```

5. **字符编码**：如果遇到乱码问题，可在 JDBC URL 中添加字符编码参数：
   ```
   jdbc:gaussdb://<host>:<port>/<database>?compatibleMode=mysql&characterEncoding=UTF-8
   ```

6. **JDK 版本**：Connector 内置的 GaussDB JDBC 驱动为 `gaussdbjdbc-506.0.0.b058-jdk7`（兼容 JDK 8/11），在 JDK 8/11 环境下可直接使用。此驱动不支持流式复制 API，WAL 模式将自动回退到 SQL 函数模式（功能完整，仅延迟略高）。如需流式复制 API，需替换为 `gaussdbjdbc-506.0.0.b058.jar`（需 JDK 17+）。

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
- 流式复制 API 模式支持 `decode-style`（b/j/t）和 `sending-batch` 参数
- SQL 函数模式仅支持 `parallel-decode-num`，输出 JSON 格式
- 推荐配置：`parallel-decode-num=4`，`decode-style=b`，`sending-batch=true`

如无法修改配置，默认使用轮询式 CDC。

## 常见问题

### Q: CDC 能捕获所有变更吗？

A: 取决于 CDC 模式：
- **WAL 模式**: 可以捕获所有变更，包括 DDL
- **轮询式**: 可能丢失两次轮询间的快速变更（同一行多次变更）

### Q: 如何切换 CDC 模式？

A: 通过 `wal.mode` 参数显式切换：
- `'wal.mode' = 'false'`（默认）：轮询式 CDC
- `'wal.mode' = 'true'`：WAL 逻辑解码 CDC

```sql
-- WAL 模式 + 并行解码示例
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
- **串行解码** (`parallel-decode-num=1`)：单线程解码，简单可靠
- **并行解码** (`parallel-decode-num>1`)：多线程并行解码，吞吐量更高，适合高负载场景
- 并行解码输出格式为 JSON 或 binary，由 `decode-style` 控制
- 推荐生产环境使用 `parallel-decode-num=4`

### Q: 为什么 WAL 模式报 UnsupportedClassVersionError？

A: GaussDB JDBC 驱动 `gaussdbjdbc-506.0.0.b058.jar` 编译于 Java 17（class file version 61.0），在 JDK 11 环境下无法加载。

解决方案：
- **方案一**：升级运行环境到 JDK 17+（推荐，支持流式复制 API 全部功能）
- **方案二**：使用兼容版驱动 `gaussdbjdbc-506.0.0.b058-jdk7.jar`（JDK 11 可用，但 WAL 模式回退到 SQL 函数模式，仅 `parallel-decode-num` 有效）

### Q: decode-style 和 sending-batch 不生效？

A: 这两个参数仅在 GaussDB JDBC 流式复制 API 模式下有效。如果 JDBC 驱动不支持流式复制 API（或使用了 JDK7 兼容版驱动），Connector 会自动回退到 SQL 函数模式，此时仅 `parallel-decode-num` 有效，输出格式为 JSON。

### Q: CDC 会影响数据库性能吗？

A: 
- **WAL 模式**: 影响很小，被动接收 WAL 记录
- **轮询式**: 有一定影响，特别是 DELETE 检测需要全表扫描

### Q: 如何监控 CDC 延迟？

A: 可以通过 Flink 监控查看：
```sql
-- 查看当前消费位置
SHOW CREATE TABLE student_cdc;
```

## 许可证

本项目基于 Apache License 2.0 开源许可证。
