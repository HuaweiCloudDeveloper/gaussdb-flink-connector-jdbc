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
|------|---------|------|-----------|
| **轮询式 (Polling)** | 定时查询数据库 | 秒级 | 无特殊要求 |
| **WAL 式 (Streaming)** | 监听 WAL 日志 | 毫秒级 | wal_level=logical |

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
- **JDK**: 11/17

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
将以下 JAR 包放置到 Flink 安装目录的 `lib/` 文件夹下：

1. **Flink Connector Base** (必选)
   - `flink-connector-base-1.17.2.jar`

2. **GaussDB CDC Connector** (必选)
   - `flink-connector-gaussdb-cdc-1.17-4.0-SNAPSHOT.jar`

3. **GaussDB JDBC 驱动** (必选，根据 JDK 版本选择其一)
   - JDK 11: `gaussdbjdbc-506.0.0.b058-jdk7.jar`（兼容 JDK 7/8/11）
   - JDK 17+: `gaussdbjdbc-506.0.0.b058.jar`（仅 JDK 17+）

4. **PostgreSQL JDBC 驱动** (WAL 模式需要)
   - `postgresql-42.6.0.jar`

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

# 2. GaussDB CDC Connector
cp flink-connector-gaussdb-cdc-1.17-4.0-SNAPSHOT.jar $FLINK_HOME/lib/

# 3. GaussDB JDBC 驱动
cp gaussdbjdbc.jar $FLINK_HOME/lib/

# 4. PostgreSQL JDBC 驱动 (WAL 模式)
cp postgresql-42.6.0.jar $FLINK_HOME/lib/
```

重启 Flink：

```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

### 2. SQL 方式使用 CDC Connector

```sql
-- 创建 GaussDB CDC 源表
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
| slot.name | 否 | flink_cdc_slot | 复制槽名称（WAL 模式） |
| snapshot.mode | 否 | true | 是否先读取全量快照 |
| poll.interval.ms | 否 | 1000 | 轮询间隔（轮询式） |
| chunk.size | 否 | 1000 | 分块大小 |

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
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Flink     │◀────│  逻辑复制流  │◀────│   GaussDB   │
│   CDC Source│     │(mppdb_decoding)│   │    WAL      │
└─────────────┘     └─────────────┘     └─────────────┘
        │
        ▼
┌─────────────┐
│ INSERT/     │
│ UPDATE/     │
│ DELETE      │
│ JSON 事件   │
└─────────────┘
```

### WAL 数据格式示例

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

| 字段 | 说明 |
|------|------|
| `table_name` | 表名（含 schema） |
| `op_type` | 操作类型：INSERT/UPDATE/DELETE |
| `columns_name` | 列名列表 |
| `columns_type` | 列类型列表 |
| `columns_val` | 列值列表 |
| `old_keys_*` | 主键信息（UPDATE/DELETE 时有效） |

## 注意事项

1. **WAL 模式要求**：
   - GaussDB 必须设置 `wal_level = logical`
   - 用户必须有 `REPLICATION` 权限
   - 需要创建逻辑复制槽

2. **轮询式限制**：
   - DELETE 检测需要全表扫描，性能较差
   - UPDATE 检测依赖 `updated_at` 字段
   - 两次轮询间的变更可能丢失

3. **类加载器配置**：如果遇到 `ClassNotFoundException`，请确保 Flink 配置为 `parent-first` 类加载策略：
   ```yaml
   # flink-conf.yaml
   classloader.resolve-order: parent-first
   ```

4. **字符编码**：如果遇到乱码问题，可在 JDBC URL 中添加字符编码参数：
   ```
   jdbc:gaussdb://<host>:<port>/<database>?compatibleMode=mysql&characterEncoding=UTF-8
   ```

### ⚠️ 重要限制

**WAL 模式需要数据库配置支持**

如需使用 WAL 模式，需要将 `wal_level` 设置为 `logical`：

- **Console 控制台**：参数管理 → 高危参数 → 修改 `wal_level` 为 `logical` → 重启实例
- **命令行**：`gs_guc reload -Z datanode -N all -I all -c "wal_level=logical"` → 重启 GaussDB

**注意**：修改后必须重启 GaussDB 才能生效。

**GaussDB 与 PostgreSQL 差异**：
- GaussDB 使用 `mppdb_decoding` 插件，数据以 JSON 格式返回
- PostgreSQL 使用 `pgoutput` 插件，数据以二进制格式返回
- 两者 API 不兼容，Connector 已针对 GaussDB 优化

如无法修改配置，默认使用轮询式 CDC。

## 常见问题

### Q: CDC 能捕获所有变更吗？

A: 取决于 CDC 模式：
- **WAL 模式**: 可以捕获所有变更，包括 DDL
- **轮询式**: 可能丢失两次轮询间的快速变更（同一行多次变更）

### Q: 如何切换 CDC 模式？

A: 目前通过配置自动选择：
- 如果数据库支持逻辑复制，自动使用 WAL 模式
- 否则使用轮询式

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
