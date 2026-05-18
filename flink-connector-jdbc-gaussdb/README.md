<p align="center">
  <h1 align="center">Flink GaussDB JDBC Connector（2.x / 3.x）</h1>
  <p align="center">适用于 Flink 2.0+ 的 GaussDB JDBC 连接器，支持 Source / Sink / UPSERT / Catalog</p>
</p>

## 目录

- [核心特性](#核心特性)
- [版本兼容性](#版本兼容性)
- [前置条件](#前置条件)
- [快速开始](#快速开始)
- [配置参数](#配置参数)
- [数据类型映射](#数据类型映射)
- [注意事项](#注意事项)
- [许可证](#许可证)

## 核心特性

### 1. 完整功能

| 功能 | 支持 |
|------|------|
| Dialect 支持 | ✅ |
| Source (SELECT) | ✅ |
| Sink (INSERT/UPSERT) | ✅ |
| Catalog 支持 | ✅ |
| `sink.ignore-null-when-update` | ✅ |
| CDC DELETE/UPDATE | ✅ |

### 2. 统一 UPSERT 语法

在所有 GaussDB 兼容模式下统一使用 `ON DUPLICATE KEY UPDATE`，无需额外配置。

### 3. 内置 GaussDB JDBC 驱动

Connector JAR 已内置 GaussDB JDBC 驱动，无需单独部署。

## 版本兼容性

| 连接器版本 | Flink 版本 | GaussDB 版本 | CDC 配套 |
|-----------|-----------|-------------|---------|
| flink-connector-jdbc-gaussdb-3.3.0-1.20 | 2.x / 3.x | 505.2.1.SPC0800+ | CDC 2.4.x / CDC 3.6.x |

## 前置条件

### 依赖 JAR 包

将以下 JAR 放置到 Flink `lib/` 目录：

1. **Flink JDBC Connector**（必选）
   - `flink-connector-jdbc-3.1.2-1.17.jar`（Flink 1.17）或对应的 Flink 2.x 版本

2. **GaussDB JDBC Connector**（必选，已内置 GaussDB JDBC 驱动）
   - `flink-connector-jdbc-gaussdb-3.3.0-1.20.jar`

> Connector JAR 已通过 maven-shade-plugin 内置 `gaussdbjdbc` 驱动，无需单独部署 GaussDB JDBC 驱动。

## 快速开始

### 1. 部署 JAR

```bash
cp flink-connector-jdbc-gaussdb-3.3.0-1.20.jar $FLINK_HOME/lib/
```

重启 Flink：
```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

### 2. SQL 使用

```sql
-- 创建 GaussDB Sink 表
CREATE TABLE student_sink (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb',
    'url' = 'jdbc:gaussdb://localhost:8000/postgres?sslmode=disable',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver'
);

-- 插入 / UPSERT
INSERT INTO student_sink VALUES (1, 'Alice', 20);
INSERT INTO student_sink VALUES (1, 'Alice Updated', 21);  -- UPSERT

-- 查询
SELECT * FROM student_sink;
```

### 3. 与 CDC Connector 配合

作为 CDC 的目标 Sink 表：
```sql
INSERT INTO student_sink SELECT * FROM student_cdc;
```

## 配置参数

### 通用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| connector | 是 | — | 连接器类型：`gaussdb`（Source/Sink 均用此值） |
| url | 是 | — | JDBC URL，格式 `jdbc:gaussdb://host:port/db` |
| table-name | 是 | — | 表名 |
| username | 是 | — | 用户名 |
| password | 是 | — | 密码 |
| driver | 否 | `com.huawei.gaussdb.jdbc.Driver` | 驱动类名 |

### JDBC URL 参数

| 参数名 | 说明 | 可选值 |
|-------|------|-------|
| compatibleMode | GaussDB 兼容模式（连接层） | `mysql` / `postgresql` / `oracle` / `td` |
| characterEncoding | 字符编码 | `UTF-8`（推荐） |
| sslmode | SSL 加密模式 | `disable` / `allow` / `prefer`（默认）/ `require` / `verify-ca` / `verify-full` |

> **sslmode**：内网环境可使用 `disable`，云环境建议 `require` 或更高。默认 `prefer` 优先 SSL，失败回退非 SSL。

### Sink 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| sink.buffer-flush.max-rows | 否 | 100 | 缓冲最大行数 |
| sink.buffer-flush.interval | 否 | 1s | 缓冲刷新间隔 |
| sink.max-retries | 否 | 3 | 最大重试次数 |
| sink.ignore-null-when-update | 否 | false | UPSERT 时是否忽略 NULL 字段 |

### Source 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| scan.fetch-size | 否 | 0 | 每次读取行数 |
| scan.partition.column | 否 | — | 分区列名 |
| scan.partition.num | 否 | — | 分区数量 |

## 数据类型映射

| Flink SQL 类型 | GaussDB 类型 |
|---------------|-------------|
| INT | INTEGER |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| BOOLEAN | BOOLEAN |
| VARCHAR | VARCHAR |
| STRING | TEXT |
| DECIMAL | DECIMAL |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| BYTES | BYTEA |
| ARRAY | ARRAY |

## 注意事项

1. **主键要求**：使用 UPSERT 时，Flink 表定义中必须声明 `PRIMARY KEY`，且 GaussDB 表中必须有对应主键约束。
2. **类加载器**：如遇 `NoClassDefFoundError`，设置 `classloader.resolve-order: parent-first`。
3. **驱动类名**：建议在 `WITH` 中显式指定 `'driver' = 'com.huawei.gaussdb.jdbc.Driver'`。
4. **与 PostgreSQL JDBC 共存**：`flink-connector-jdbc-gaussdb` 和 PostgreSQL JDBC driver 可安全共存于 `lib/`，类路径无冲突。

## 许可证

Apache License 2.0
