<p align="center">
  <h1 align="center">Flink GaussDB JDBC Connector 1.17</h1>
  <p align="center">专为 Flink 1.17 设计的 GaussDB 连接器，支持 Source 读取和 Sink 写入（含 UPSERT）</p>
</p>

## 目录

- [项目介绍](#项目介绍)
- [核心特性](#核心特性)
- [版本兼容性](#版本兼容性)
- [前置条件](#前置条件)
- [快速开始](#快速开始)
- [使用说明](#使用说明)
- [配置参数](#配置参数)
- [数据类型映射](#数据类型映射)
- [注意事项](#注意事项)
- [常见问题](#常见问题)
- [许可证](#许可证)

## 项目介绍

Flink GaussDB JDBC Connector 1.17 是专为 Apache Flink 1.17 版本设计的 GaussDB 数据库连接器，支持从 GaussDB 读取数据（Source）和写入数据（Sink），并提供 UPSERT 功能。

## 核心特性

### 1. 双模式 UPSERT 支持
- **MySQL 兼容模式**: `ON DUPLICATE KEY UPDATE` - 适用于 GaussDB B 兼容模式
- **PostgreSQL 原生模式**: `ON CONFLICT ... DO UPDATE` - 适用于 GaussDB 原生模式
- 通过 URL 参数 `compatibleMode=mysql` 自动切换

### 2. 数据源支持
- **Source (读取)**: 支持从 GaussDB 查询数据
- **Sink (写入)**: 支持 INSERT 和 UPSERT 操作
- 使用 `PostgresRowConverter` 实现 Source 功能（GaussDB 基于 PostgreSQL）

### 3. 与 2.0+ 版本的区别

| 功能 | 1.17 版本 | 2.0+ 版本 |
|-----|----------|----------|
| Dialect 支持 | ✅ | ✅ |
| Source (SELECT) | ✅ | ✅ |
| Sink (INSERT/UPSERT) | ✅ | ✅ |
| Catalog 支持 | ❌ | ✅ |
| `sink.ignore-null-when-update` | ❌ | ✅ |
| CDC DELETE/UPDATE | ❌ | ✅ |

**说明**：1.17 版本是精简实现，依赖 Flink 原生 JDBC Connector 提供基础功能。

## 版本兼容性

| 连接器版本 | Flink 版本 | GaussDB 版本 | 状态 |
|-----------|-----------|-------------|------|
| flink-connector-jdbc-gaussdb-1.17 | 1.17.x | 505.2.1.SPC0800+ | ✅ 推荐 |

## 前置条件

### 系统要求
- **CPU**: 2GHz 或更高
- **RAM**: 4GB 或更大
- **Disk**: 至少 40GB
- **JDK**: 8/11/17

### 依赖 JAR 包
将以下 JAR 包放置到 Flink 安装目录的 `lib/` 文件夹下：

1. **Flink Connector Base** (必选)
   - `flink-connector-base-1.17.2.jar`

2. **Flink JDBC Connector** (必选)
   - `flink-connector-jdbc-3.1.2-1.17.jar`

3. **GaussDB Connector** (必选)
   - `flink-connector-jdbc-gaussdb-1.17-4.0-SNAPSHOT.jar`

4. **GaussDB JDBC 驱动** (必选)
   - JDK 8/11: `gaussdbjdbc-506.0.0.b058-jdk7.jar`
   - JDK 17+: `gaussdbjdbc-506.0.0.b058.jar`

#### MRS 环境说明

本 JAR 包已排除 `flink-connector-base` 和 `flink-connector-jdbc` 依赖，避免与 MRS 自带的包冲突。

**MRS 环境要求**：
- MRS 集群必须已安装 `flink-connector-base` 和 `flink-connector-jdbc` 组件
- 只需将 `flink-connector-jdbc-gaussdb-1.17-4.0-SNAPSHOT.jar` 和 GaussDB JDBC 驱动放入 MRS 的 lib 目录

### Maven 依赖

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb.flink</groupId>
    <artifactId>flink-connector-jdbc-gaussdb-1.17</artifactId>
    <version>4.0-SNAPSHOT</version>
</dependency>
```

## 快速开始

### 1. 部署 JAR 包

```bash
# 1. Flink Connector Base（MRS 环境可跳过）
cp flink-connector-base-1.17.2.jar $FLINK_HOME/lib/

# 2. Flink 官方 JDBC Connector（MRS 环境可跳过）
cp flink-connector-jdbc-3.1.2-1.17.jar $FLINK_HOME/lib/

# 3. GaussDB Connector（必须）
cp flink-connector-jdbc-gaussdb-1.17-4.0-SNAPSHOT.jar $FLINK_HOME/lib/

# 4. GaussDB JDBC 驱动（必须）
cp gaussdbjdbc.jar $FLINK_HOME/lib/
```

重启 Flink：

```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

**适用场景**：
- ✅ Flink SQL 客户端
- ✅ Flink 作业提交（`flink run`）
- ✅ 支持完整的 Source + Sink 功能

### 2. SQL 方式使用 GaussDB Connector

```sql
-- 创建 GaussDB Sink 表
CREATE TABLE student_sink (
    id INT,
    name STRING,
    gender STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb',
    'url' = 'jdbc:gaussdb://localhost:8000/postgres?compatibleMode=mysql',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver'
);

-- 插入数据
INSERT INTO student_sink VALUES (1, 'Alice', 'F', 20), (2, 'Bob', 'M', 25);

-- 查询数据
SELECT * FROM student_sink ORDER BY id;
```

### 3. UPSERT 示例

```sql
-- 首次插入
INSERT INTO student_sink VALUES (1, 'Alice', 'F', 20);

-- 更新（主键存在则更新，不存在则插入）
INSERT INTO student_sink VALUES (1, 'Alice Updated', 'F', 21), (3, 'Charlie', 'M', 30);
```

## 使用说明

### 连接器类型选择

| 场景 | Connector 类型 | 说明 |
|------|---------------|------|
| Sink (写入) | `gaussdb` | 使用 GaussDB 专用 Sink，支持 UPSERT |
| Source (读取) | `jdbc` | 使用标准 JDBC Source |

### UPSERT 语法模式选择

| GaussDB 模式 | URL 参数 | UPSERT 语法 |
|-------------|---------|------------|
| B 兼容模式 (MySQL) | `compatibleMode=mysql` | `ON DUPLICATE KEY UPDATE` |
| 原生模式 | 无 | `ON CONFLICT ... DO UPDATE` |

## 配置参数

### 通用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| connector | 是 | - | 连接器类型：`gaussdb` 或 `jdbc` |
| url | 是 | - | JDBC URL |
| table-name | 是 | - | 表名 |
| username | 是 | - | 用户名 |
| password | 是 | - | 密码 |
| driver | 否 | com.huawei.gaussdb.jdbc.Driver | 驱动类名 |

### JDBC URL 参数

| 参数名 | 说明 | 可选值 |
|-------|------|-------|
| compatibleMode | 选择 UPSERT 语法模式 | `mysql` - 使用 `ON DUPLICATE KEY UPDATE`<br>不设置或 `postgresql` - 使用 `ON CONFLICT ... DO UPDATE` |
| characterEncoding | 字符编码 | `UTF-8`（推荐） |

### Sink 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| sink.buffer-flush.max-rows | 否 | 100 | 缓冲最大行数 |
| sink.buffer-flush.interval | 否 | 1s | 缓冲刷新间隔 |
| sink.max-retries | 否 | 3 | 最大重试次数 |

### Source 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| scan.fetch-size | 否 | 0 | 每次读取行数 |
| scan.partition.column | 否 | - | 分区列名 |
| scan.partition.num | 否 | - | 分区数量 |

## 数据类型映射

| Flink SQL 类型 | GaussDB 类型 |
|---------------|-------------|
| INT | INTEGER |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| BOOLEAN | BOOLEAN |
| VARCHAR | VARCHAR |
| CHAR | CHAR |
| STRING | TEXT |
| DECIMAL | DECIMAL |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| BYTES | BYTEA |
| ARRAY | ARRAY |

## 注意事项

1. **部署方式**：
   - 推荐将 JAR 包放在 `lib` 目录，支持所有使用场景（SQL 客户端 + Flink 作业）
   - 仅 SQL 客户端交互式使用时，可选择 `ADD JAR` 方式

2. **主键要求**：使用 UPSERT 功能时，必须在 Flink 表定义中声明 `PRIMARY KEY`，且 GaussDB 表中必须有对应的主键约束。

3. **类加载器配置**：如果遇到 `NoClassDefFoundError`，请确保 Flink 配置为 `parent-first` 类加载策略：
   ```yaml
   # flink-conf.yaml
   classloader.resolve-order: parent-first
   ```

4. **驱动类名**：必须在 `WITH` 子句中显式指定 `'driver' = 'com.huawei.gaussdb.jdbc.Driver'`。

5. **字符编码**：如果遇到乱码问题，可在 JDBC URL 中添加字符编码参数：
   ```
   jdbc:gaussdb://<host>:<port>/<database>?compatibleMode=mysql&characterEncoding=UTF-8
   ```

### ⚠️ 重要限制

**`sink.ignore-null-when-update` 参数不支持**

由于 Flink 1.17 的类加载器架构限制，自定义 `StatementExecutorFactory` 无法在 TaskManager 中正确反序列化。因此，1.17 版本使用 Flink 原生的 `JdbcDynamicTableSink`，不支持 `sink.ignore-null-when-update` 参数。

如果需要此功能，请升级到 **Flink 2.0+** 版本。

## 常见问题

### Q: 为什么 INSERT 成功但 UPSERT 失败？

A: 请检查：
1. Flink 表定义中是否声明了 `PRIMARY KEY`
2. GaussDB 表中是否有主键约束
3. JDBC URL 中的 `compatibleMode` 参数是否与 GaussDB 兼容模式匹配

### Q: 如何查看生成的 UPSERT SQL？

A: 开启 Flink DEBUG 日志：
```yaml
# log4j.properties
logger.jdbc.name = org.apache.flink.connector.jdbc
logger.jdbc.level = DEBUG
```

### Q: 使用 `ADD JAR` 和 `lib` 目录有什么区别？

A: 
- `ADD JAR`：仅在 Flink SQL 客户端中可用，Connector 类加载在 SQL 客户端 JVM 中
- `lib` 目录：支持所有场景（SQL 客户端 + `flink run`），Connector 类加载在 TaskManager JVM 中

## 许可证

本项目基于 Apache License 2.0 开源许可证。
