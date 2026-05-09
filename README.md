<p align="center">
  <h1 align="center">Flink GaussDB Connector</h1>
  <p align="center">GaussDB 连接器全家桶 — JDBC Sink/Source + CDC 实时变更捕获，覆盖 Flink 1.17 ~ 2.0+</p>
</p>

## 目录

- [项目介绍](#项目介绍)
- [模块一览](#模块一览)
- [版本兼容性](#版本兼容性)
- [前置条件](#前置条件)
- [快速开始](#快速开始)
  - [JDBC Connector（Sink/Source）](#jdbc-connectorsinksource)
  - [CDC Connector（实时变更捕获）](#cdc-connector实时变更捕获)
- [配置参数](#配置参数)
  - [JDBC Connector 参数](#jdbc-connector-参数)
  - [CDC Connector 参数](#cdc-connector-参数)
- [驱动适配说明](#驱动适配说明)
- [注意事项](#注意事项)
- [获取帮助](#获取帮助)
- [如何贡献](#如何贡献)

## 项目介绍

[Flink GaussDB Connector](https://github.com/HuaweiCloudDeveloper/gaussdb-flink-connector-jdbc) 是一组开源的 GaussDB 连接器，提供 JDBC 写入/读取和 CDC 实时变更捕获能力，适配 Flink 1.17 ~ 2.0+ 多个版本。

## 模块一览

| 模块 | 类型 | Flink 版本 | 说明 |
|------|------|-----------|------|
| `flink-connector-jdbc-gaussdb` | JDBC Sink/Source | 2.0+ | 基于 Flink 2.0+ JDBC Connector |
| `flink-connector-jdbc-gaussdb-1.17` | JDBC Sink/Source | 1.17.x | 专为 Flink 1.17 适配 |
| `flink-connector-gaussdb-cdc-1.17` | CDC | 1.17.x | 基于 SourceFunction 的 CDC 连接器 |
| `flink-connector-gaussdb-cdc-2.4.x` | CDC | 1.17.x ~ 1.20.x | 基于 FLIP-27 Source API 的 CDC 连接器 |
| `flink-connector-gaussdb-cdc-3.6.x` | CDC | 1.18+ | 基于 Flink CDC 3.6.x 的 CDC 连接器，支持 mppdb_decoding 串行/并行解码 |

## 版本兼容性

| Flink 版本 | JDBC Connector | CDC 1.17 | CDC 2.4.x | CDC 3.6.x |
|-----------|---------------|----------|-----------|-----------|
| 1.17.x | ✅ `flink-connector-jdbc-gaussdb-1.17` | ✅ | ✅ | ❌ |
| 1.18.x ~ 1.19.x | — | — | ⚠️ 未充分验证 | ⚠️ 未充分验证 |
| 1.20.x | — | — | ✅ | ✅ 推荐 |
| 2.0+ | ✅ `flink-connector-jdbc-gaussdb` | — | — | ⚠️ 未充分验证 |

## 前置条件

### 系统要求
- **CPU**: 2GHz 或更高
- **RAM**: 4GB 或更大
- **Disk**: 至少 40GB
- **JDK**: 8 / 11 / 17（CDC 3.6.x 推荐 JDK 11）

### GaussDB 要求
- `wal_level=logical`
- 逻辑复制槽（CDC Connector 会自动创建，也可手动预创建）
- `gs_hba.conf` 中配置复制连接白名单

### 依赖 JAR 包

所有 Connector JAR 已内置 GaussDB JDBC 驱动（`gaussdbjdbc-506.0.0.b058-jdk7`，兼容 JDK 8/11），无需额外部署。

**JDBC Connector 还需要**：Flink 官方 `flink-connector-jdbc` 对应版本 JAR。

**CDC Connector 无需额外 JAR**：fat JAR 已包含所有依赖（Debezium、GaussDB 驱动等），直接放入 `$FLINK_HOME/lib/` 即可。

> **注意**：CDC 3.6.x 在某些 Flink 环境下可能需要将 GaussDB JDBC 驱动 JAR 单独放入 `$FLINK_HOME/lib/` 以解决 ServiceLoader 类加载冲突。

## 快速开始

### JDBC Connector（Sink/Source）

```sql
-- 创建 GaussDB Sink 表
CREATE TABLE gaussdb_sink (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb',
    'url' = 'jdbc:gaussdb://localhost:8000/postgres?compatibleMode=mysql',
    'table-name' = 'users',
    'username' = 'gaussdb',
    'password' = 'password',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver',
    'sink.ignore-null-when-update' = 'true'
);

-- 插入/更新数据（UPSERT）
INSERT INTO gaussdb_sink VALUES (1, 'Alice', 20), (2, 'Bob', 25);
```

#### UPSERT 语法模式

| GaussDB 模式 | URL 参数 | UPSERT 语法 |
|-------------|---------|------------|
| B 兼容模式 (MySQL) | `compatibleMode=mysql` | `ON DUPLICATE KEY UPDATE` |
| 原生模式 | 无 | `ON CONFLICT ... DO UPDATE` |

#### 使用 Catalog

```sql
CREATE CATALOG gaussdb_catalog WITH (
    'type' = 'jdbc',
    'base-url' = 'jdbc:gaussdb://localhost:8000',
    'default-database' = 'postgres',
    'username' = 'gaussdb',
    'password' = 'password'
);
USE CATALOG gaussdb_catalog;
SHOW TABLES;
```

### CDC Connector（实时变更捕获）

CDC Connector 基于 GaussDB 的 `mppdb_decoding` 逻辑解码插件，实时捕获 INSERT / UPDATE / DELETE 变更事件。

#### 串行解码模式（parallel-decode-num=1）

```sql
CREATE TABLE test_cdc_source (
    id INT NOT NULL,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database-name' = 'postgres',
    'schema-name' = 'public',
    'table-name' = 'test_cdc',
    'username' = 'root',
    'password' = 'password',
    'slot.name' = 'flink_cdc_slot',
    'decoding.plugin.name' = 'mppdb_decoding'
);
```

#### 并行解码 + Binary 模式（parallel-decode-num>1, decode-style=b）

```sql
CREATE TABLE test_cdc_source (
    id INT NOT NULL,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database-name' = 'postgres',
    'schema-name' = 'public',
    'table-name' = 'test_cdc',
    'username' = 'root',
    'password' = 'password',
    'slot.name' = 'flink_cdc_slot',
    'decoding.plugin.name' = 'mppdb_decoding',
    'parallel-decode-num' = '4',
    'decode-style' = 'b',
    'sending-batch' = 'true'
);

-- 将 CDC 数据写入 Print Sink 验证
CREATE TABLE test_cdc_sink (
    id INT NOT NULL, name STRING, age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' = 'print');

INSERT INTO test_cdc_sink SELECT * FROM test_cdc_source;
```

> **参数依赖**：`parallel-decode-num=1` 时底层强制 JSON 输出，`decode-style` 和 `sending-batch` 不生效；仅 `parallel-decode-num>1` 时才可选择 binary（`b`）、json（`j`）、text（`t`）格式。

#### 部署 CDC JAR

```bash
# 打包
mvn clean package -pl flink-connector-gaussdb-cdc-3.6.x -DskipTests

# 部署到 Flink lib 目录
cp flink-connector-gaussdb-cdc-3.6.x/target/flink-connector-gaussdb-cdc-3.6.x-*.jar $FLINK_HOME/lib/

# 启动 Flink 集群
$FLINK_HOME/bin/start-cluster.sh
```

#### 使用 SQL Client 查看 CDC 数据

> **重要**：使用 CDC 3.6.x 模块时，在 Flink SQL Client 中 SELECT CDC 表必须设置 TABLEAU 结果模式，否则默认 TABLE 模式下数据无法显示。详见 [注意事项](#注意事项)。

**交互模式**（实时查看变更）：

```bash
$FLINK_HOME/bin/sql-client.sh
```

```sql
Flink SQL> SET 'execution.checkpointing.interval' = '10s';
Flink SQL> SET 'sql-client.execution.result-mode' = 'TABLEAU';  -- 必须！

Flink SQL> CREATE TABLE test_cdc_source (
           >     id INT NOT NULL, name STRING, age INT,
           >     PRIMARY KEY (id) NOT ENFORCED
           > ) WITH (
           >     'connector' = 'gaussdb-cdc',
           >     'hostname' = 'localhost',
           >     'port' = '8000',
           >     'database-name' = 'postgres',
           >     'schema-name' = 'public',
           >     'table-name' = 'test_cdc',
           >     'username' = 'root',
           >     'password' = 'password',
           >     'slot.name' = 'flink_cdc_slot',
           >     'decoding.plugin.name' = 'mppdb_decoding'
           > );

Flink SQL> SELECT * FROM test_cdc_source;
-- 结果将持续打印到终端（Ctrl+C 停止）
-- +I 表示 INSERT，-U/+U 表示 UPDATE，-D 表示 DELETE
```

**非交互模式**（SQL 脚本执行）：

```sql
-- cdc_query.sql
SET 'execution.checkpointing.interval' = '10s';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE test_cdc_source (
    id INT NOT NULL, name STRING, age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database-name' = 'postgres',
    'schema-name' = 'public',
    'table-name' = 'test_cdc',
    'username' = 'root',
    'password' = 'password',
    'slot.name' = 'flink_cdc_slot',
    'decoding.plugin.name' = 'mppdb_decoding'
);

SELECT * FROM test_cdc_source;
```

```bash
$FLINK_HOME/bin/sql-client.sh -f cdc_query.sql
```

**写入 Sink 模式**（不受 TABLE/TABLEAU 限制）：

```sql
-- INSERT INTO 写入目标表，数据流在 Flink 集群内部传输，无需经过 collect sink
INSERT INTO target_table SELECT * FROM test_cdc_source;
```

## 配置参数

### JDBC Connector 参数

#### 通用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| connector | 是 | - | 连接器类型：`gaussdb` 或 `jdbc` |
| url | 是 | - | JDBC URL |
| table-name | 是 | - | 表名 |
| username | 是 | - | 用户名 |
| password | 是 | - | 密码 |
| driver | 否 | com.huawei.gaussdb.jdbc.Driver | 驱动类名 |

#### Sink 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| sink.ignore-null-when-update | 否 | false | 更新时忽略 NULL 值 |
| sink.buffer-flush.max-rows | 否 | 100 | 缓冲最大行数 |
| sink.buffer-flush.interval | 否 | 1s | 缓冲刷新间隔 |
| sink.max-retries | 否 | 3 | 最大重试次数 |

#### Source 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| scan.fetch-size | 否 | 0 | 每次读取行数 |
| scan.partition.column | 否 | - | 分区列名 |
| scan.partition.num | 否 | - | 分区数量 |

### CDC Connector 参数

#### 必需参数

| 参数 | 说明 |
|------|------|
| `hostname` | GaussDB 主机地址 |
| `port` | 端口，默认 `8000` |
| `database-name` | 数据库名 |
| `schema-name` | Schema 名，默认 `public` |
| `table-name` | 监控的表名，支持正则匹配多表 |
| `username` | 用户名 |
| `password` | 密码 |
| `slot.name` | 逻辑复制 slot 名 |

#### 可选参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `decoding.plugin.name` | `mppdb_decoding` | 逻辑解码插件 |
| `changelog-mode` | `all` | `all` = retract 流；`upsert` = upsert 流 |
| `scan.startup.mode` | `initial` | 启动模式：`initial` / `latest-offset` / `committed-offset` |
| `scan.incremental.snapshot.enabled` | `true` | 是否启用增量快照 |
| `scan.incremental.snapshot.chunk.size` | `8096` | 快照分片大小 |
| `heartbeat.interval.ms` | `30s` | 心跳间隔，追踪复制 slot 进度 |
| `parallel-decode-num` | `1` | 并行解码线程数（1~20）。`1` 为串行解码，**>1 时 decode-style 和 sending-batch 才生效** |
| `decode-style` | `b` | 解码格式：`b`=binary，`j`=json，`t`=text。**串行模式只能用 `j`** |
| `sending-batch` | `false` | `true` 时累积到 1MB 后批量发送，减少网络交互 |

> **参数依赖关系**：
> - `parallel-decode-num > 1` 时，`decode-style`（'b'/'j'/'t'）和 `sending-batch` 才生效
> - `parallel-decode-num = 1` 时，底层强制 JSON 输出，`decode-style='b'` 不生效

## 驱动适配说明

Connector 基于 Debezium PostgreSQL Connector 构建，通过以下方式适配 GaussDB：

1. **GaussDB JDBC 驱动内置**：fat JAR 已包含 `gaussdbjdbc`，无需额外放置驱动
2. **Shade Relocation**：打包时将对 `org.postgresql` 的类引用重定向到 `com.huawei.gaussdb.jdbc`
3. **版本校验绕过**：GaussDB JDBC 兼容性报告版本号为 `9.2.4`，Connector 内部自动跳过 Debezium 的 `>= 9.4` 版本校验
4. **mppdb_decoding 适配**：CDC Connector 替换 Debezium 的 PgOutput 解码器为 `MppdbDecodingMessageDecoder`（JSON）或 `MppdbBinaryMessageDecoder`（Binary），并通过 `slot.stream.params` 将并行解码参数传递给 GaussDB

## 注意事项

1. **驱动版本选择**：根据 JDK 版本选择对应的 GaussDB 驱动，否则可能导致任务提交失败
2. **类加载器配置**：如遇到 `ClassNotFoundException`，请在 `flink-conf.yaml` 中设置 `classloader.resolve-order: parent-first`
3. **连接器版本匹配**：flink-connector-jdbc 和 flink-connector-jdbc-gaussdb 版本需要与 Flink 版本匹配
4. **字符编码**：GaussDB B 模式建议使用 UTF-8 编码，URL 中添加 `characterEncoding=UTF-8`
5. **CDC 需要开启 Checkpoint**：Flink CDC 任务必须配置 checkpoint，否则 offset 无法提交，复制 slot 的 WAL 不会被回收
6. **REPLICA IDENTITY**：Binary 模式下 DELETE 和 UPDATE 的 before image 取决于表的 REPLICA IDENTITY 设置（DEFAULT 仅含主键列，FULL 含全部列）
7. **SQL Client 查询 CDC 3.6.x 数据**：使用 CDC 3.6.x 模块时，在 Flink SQL Client 中 SELECT CDC 表必须设置 `TABLEAU` 结果模式，否则默认 `TABLE` 模式下 collect sink 的版本握手机制会导致数据无法显示
   ```sql
   -- 在执行 SELECT 之前添加：
   SET 'sql-client.execution.result-mode' = 'TABLEAU';
   ```
   > **原因**：Flink 的 `CollectResultFetcher.isJobTerminated()` 方法对所有异常（包括 `InterruptedException`）都返回 `true`，导致流式 CDC 查询的结果拉取被过早终止。目前仅在 CDC 3.6.x + Flink 1.20.3 上发现此问题。

## 各模块详细文档

- [CDC 1.17 详细文档](./flink-connector-gaussdb-cdc-1.17/README.md)
- [CDC 2.4.x 详细文档](./flink-connector-gaussdb-cdc-2.4.x/README.md)
- [CDC 3.6.x 详细文档](./flink-connector-gaussdb-cdc-3.6.x/README.md)

## 获取帮助

- **GitHub Issues**: [提交问题](https://github.com/HuaweiCloudDeveloper/gaussdb-flink-connector-jdbc/issues)
- **文档**: [Wiki](https://github.com/HuaweiCloudDeveloper/gaussdb-flink-connector-jdbc/wiki)

## 如何贡献

1. Fork 此存储库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 许可证

本项目基于 Apache License 2.0 开源许可证。
