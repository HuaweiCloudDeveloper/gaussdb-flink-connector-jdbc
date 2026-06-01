<p align="center">
  <h1 align="center">Flink GaussDB CDC Connector 3.6.x</h1>
  <p align="center">基于 Flink CDC 3.6.x（FLIP-27 Source API）的 GaussDB CDC 连接器</p>
</p>

## 核心功能

- 基于 Flink CDC 3.6.x 的 FLIP-27 Source API，支持无锁增量快照
- WAL 逻辑解码（`mppdb_decoding`）流式变更捕获
- INSERT / UPDATE / DELETE 全变更事件捕获
- 全量快照 → 增量流式自动衔接（Hybrid 模式）
- 并行快照分片读取（`scan.incremental.snapshot.enabled`）
- GaussDB 并行解码参数（`parallel-decode-num`、`decode-style`、`sending-batch`）
- 自动使用 GaussDB JDBC 驱动（内置，无需额外配置）

## 参数说明

### 连接参数

| 参数 | 是否必填 | 默认值 | 可选值 | 说明 |
|------|---------|--------|--------|------|
| `hostname` | ✅ | — | — | GaussDB 主机地址 |
| `port` | ❌ | `8000` | 1~65535 | GaussDB 主业务端口（普通 JDBC 连接、表发现、快照、slot 元数据查询、心跳等均走此端口） |
| `replication.port` | ❌ | 同 `port` | 1~65535 | 逻辑复制（WAL 流式捕获）专用端口。仅在 GaussDB 启用 `enable_thread_pool=on` 导致协议按端口分离（主业务端口禁 replication、HA 端口禁普通 JDBC）时才需要显式设置，指向 HA（replication）监听端口（常见为主业务端口 + 1，例如主 `8000`、复制 `8001`）。未设置时与 `port` 相同 |
| `database-name` | ✅ | — | — | 数据库名 |
| `schema-name` | ❌ | `public` | — | Schema 名 |
| `table-name` | ✅ | — | — | 监控的表名，支持正则匹配多表（如 `public\.user_.*`） |
| `username` | ✅ | — | — | GaussDB 用户名 |
| `password` | ✅ | — | — | GaussDB 密码 |
| `slot.name` | ✅ | — | — | 逻辑复制 slot 名。Connector 启动时若 slot 不存在会自动创建 |

### 快照与启动参数

| 参数 | 是否必填 | 默认值 | 可选值 | 说明 |
|------|---------|--------|--------|------|
| `scan.startup.mode` | ❌ | `initial` | `initial` / `snapshot` / `latest-offset` / `committed-offset` | 启动模式：<br>- `initial`：先全量快照，再增量流式（默认）<br>- `snapshot`：仅全量快照，不同步增量<br>- `latest-offset`：从最新 LSN 开始流式，不读快照<br>- `committed-offset`：从上次提交的 checkpoint LSN 恢复 |
| `scan.incremental.snapshot.enabled` | ❌ | `true` | `true` / `false` | 是否启用增量快照。`true` 时使用无锁快照分片读取；`false` 时退化为单线程全表扫描 |
| `scan.incremental.snapshot.chunk.size` | ❌ | `8096` | ≥1 | 快照分片大小（行数）。分片越大，快照阶段产生的 split 越少，但单次读取数据量越大 |
| `connection.pool.size` | ❌ | `20` | ≥1 | JDBC 连接池大小，用于快照阶段并行分片读取 |
| `connect.timeout` | ❌ | `30s` | — | JDBC 连接超时 |
| `connect.max-retries` | ❌ | `3` | ≥0 | 连接失败重试次数 |
| `heartbeat.interval.ms` | ❌ | `30s` | — | 心跳间隔，用于追踪复制 slot 进度，防止 slot 因长时间无活动而被回收 |

### 逻辑解码参数（mppdb_decoding）

| 参数 | 是否必填 | 默认值 | 可选值 | 说明 |
|------|---------|--------|--------|------|
| `decoding.plugin.name` | ❌ | `mppdb_decoding` | `mppdb_decoding` / `pgoutput` | 逻辑解码插件。GaussDB 必须使用 `mppdb_decoding` |
| `parallel-decode-num` | ❌ | `1` | 1~20 | 并行解码线程数。<br>- `1` = 串行解码（默认）<br>- `>1` = 并行解码，此时 `decode-style` 和 `sending-batch` 才生效 |
| `decode-style` | ❌ | `b` | `b` / `j` / `t` | 解码输出格式（**仅 parallel-decode-num > 1 时生效**）：<br>- `b` = binary（推荐，性能最好）<br>- `j` = json<br>- `t` = text<br>串行模式（parallel-decode-num=1）时底层强制 JSON，此参数无效 |
| `sending-batch` | ❌ | `false` | `true` / `false` | 是否批量发送解码结果（**仅 parallel-decode-num > 1 时生效**）。`true` 时结果累积到 1MB 后批量发送，减少网络交互 |

> **参数依赖关系**
>
> 1. `parallel-decode-num > 1` 时，`decode-style`（`b`/`j`/`t`）和 `sending-batch`（`true`/`false`）才会被注入到 GaussDB 的 `START_REPLICATION` 命令中
> 2. `parallel-decode-num = 1` 时，mppdb_decoding 强制输出 JSON，不支持 binary 格式
> 3. 这些参数在底层通过 `slot.stream.params` 自动传递给 GaussDB，用户**无需手动拼接**

### Changelog 与高级参数

| 参数 | 是否必填 | 默认值 | 可选值 | 说明 |
|------|---------|--------|--------|------|
| `changelog-mode` | ❌ | `all` | `all` / `upsert` | Changelog 编码模式：<br>- `all`：输出完整的 retract 流（`+I` `-U` `+U` `-D`），适用于需要完整变更语义的场景（默认）<br>- `upsert`：输出 upsert 流（`+I` `+U` `-D`），要求表必须有主键，适用于直接写入支持 upsert 的 Sink |
| `scan.lsn-commit.checkpoints-num-delay` | ❌ | `3` | ≥0 | LSN 提交延迟的 checkpoint 数量。流式阶段每 N 个 checkpoint 才向 GaussDB 确认一次 LSN，避免频繁确认影响性能 |
| `table-id.include-database` | ❌ | `true` | `true` / `false` | Table ID 是否包含数据库名。`true` 时格式为 `(database, schema, table)` |

### Debezium 透传参数

CDC 3.6.x 基于 Debezium，任何 Debezium PostgreSQL Connector 支持的参数都可以通过 `debezium.` 前缀透传。

| 常见场景 | 参数写法 | 说明 |
|---------|---------|------|
| SSL 模式 | `'debezium.database.sslmode' = 'disable'` | 禁用 SSL。可选 `disable` / `allow` / `prefer` / `require` / `verify-ca` / `verify-full` |
| SSL 根证书 | `'debezium.database.sslrootcert' = '/path/to/ca.crt'` | 指定 CA 证书路径 |
| 其他 Debezium 参数 | `'debezium.xxx.yyy' = 'zzz'` | 参考 Debezium PostgreSQL Connector 文档 |

> **注意**：CDC 3.6.x **没有 `walmode` 参数**（CDC 2.4.x 中用于切换 WAL/轮询模式）。CDC 3.6.x 始终使用 WAL 逻辑解码，不支持轮询模式。

### GaussDB 线程池模式下的端口分离（replication.port）

当 GaussDB 实例启用 `enable_thread_pool=on` 时，协议会按端口隔离：

- **主业务端口**（例如 `8000`）：仅接受普通 `gsql/JDBC` 客户端连接，拒绝 replication 协议连接，错误示例：
  `FATAL: replication should connect HA port in thread_pool`
- **HA / replication 端口**（例如 `8001`）：仅接受带 `replication=database` 的连接，拒绝普通 `gsql/JDBC` 客户端连接，错误示例：
  `FATAL: the local listen ip and port is not for the gsql client`

此时必须在 CDC 配置里同时指定两个端口，否则 snapshot 或 stream 任一阶段会因端口协议不匹配而失败：

```sql
'port'             = '8000',  -- 主业务端口：表发现 / snapshot / slot 元数据 / drop slot / 心跳
'replication.port' = '8001'   -- HA 端口：WAL 流式复制（START_REPLICATION）
```

Connector 运行时会自动路由：
- `snapshot` 阶段、`discoverDataCollections`、`readReplicationSlotInfo`、`maybeDropSlotForBackFillReadTask`、heartbeat → `port`
- Debezium `PostgresReplicationConnection.startStreaming` / `CREATE_REPLICATION_SLOT` / `IDENTIFY_SYSTEM` / `START_REPLICATION` → `replication.port`

若 GaussDB 未启用线程池（单端口同时支持两种协议），省略 `replication.port` 即可，默认与 `port` 相同。

### 完整参数示例（Flink SQL）

```sql
CREATE TABLE student_cdc (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    -- 基础连接（必填）
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    -- 仅 enable_thread_pool=on 时需要，指向 HA/replication 端口
    'replication.port' = '8001',
    'database-name' = 'test',
    'schema-name' = 'public',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',
    'slot.name' = 'flink_cdc_slot',

    -- 解码插件（默认 mppdb_decoding，一般不用显式写）
    'decoding.plugin.name' = 'mppdb_decoding',

    -- 启动模式（默认 initial）
    'scan.startup.mode' = 'initial',

    -- 增量快照（默认 true）
    'scan.incremental.snapshot.enabled' = 'true',

    -- 并行解码（串行模式 parallel-decode-num=1 时，decode-style 无效）
    'parallel-decode-num' = '4',
    'decode-style' = 'b',
    'sending-batch' = 'true',

    -- SSL（通过 Debezium 透传）
    'debezium.database.sslmode' = 'disable'
);
```

## 快速开始

### 部署 JAR 包

```bash
# 打包
mvn clean package -pl flink-connector-gaussdb-cdc-3.6.x -DskipTests

# 部署到 Flink lib 目录
cp flink-connector-gaussdb-cdc-3.6.x/target/flink-connector-gaussdb-cdc-3.6.x-*.jar $FLINK_HOME/lib/
```

### Flink SQL 使用

```sql
CREATE TABLE student_cdc (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database-name' = 'test',
    'schema-name' = 'public',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',
    'slot.name' = 'flink_cdc_slot',
    'decoding.plugin.name' = 'mppdb_decoding',

    -- 增量快照（默认开启）
    'scan.incremental.snapshot.enabled' = 'true',

    -- 并行解码
    'parallel-decode-num' = '4',
    'decode-style' = 'b',
    'sending-batch' = 'true'
);

SELECT * FROM student_cdc;
```

> **⚠️ SQL Client 查询 CDC 数据必须使用 TABLEAU 模式**
>
> 在 Flink SQL Client 中 SELECT CDC 表时，默认 TABLE 结果模式下数据无法显示（已在 Flink 1.20.3 上验证）。执行 SELECT 前必须先设置：
> ```sql
> SET 'sql-client.execution.result-mode' = 'TABLEAU';
> ```
> - **交互模式**（`sql-client.sh`）：TABLEAU 模式下结果持续打印到终端，`Ctrl+C` 停止
> - **非交互模式**（`sql-client.sh -f xxx.sql`）：SQL 文件开头添加上述 SET 语句
> - **INSERT INTO 写入 Sink**：不受此限制影响，数据流在 Flink 集群内部传输
>
> 根因：Flink `CollectResultFetcher.isJobTerminated()` 对所有异常返回 `true`，导致流式 CDC 查询结果拉取过早终止（Flink 框架级问题）

### DataStream API 使用

```java
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

GaussDBSource<String> source = GaussDBSource.<String>builder()
    .hostname("localhost")
    .port(8000)
    // 仅 enable_thread_pool=on 时需要；未调用时与 port 相同
    .replicationPort(8001)
    .databaseList("test")
    .schemaList("public")
    .tableList("test.student")
    .username("root")
    .password("password")
    .slotName("flink_cdc_slot")
    .decodingPluginName("mppdb_decoding")
    .deserializer(new JsonDebeziumDeserializationSchema())
    .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.fromSource(source, WatermarkStrategy.noWatermarks(), "GaussDB CDC Source")
   .print();
env.execute("GaussDB CDC Job");
```

## 前置条件

- Flink 1.20+
- GaussDB `wal_level=logical`
- 逻辑复制槽（Connector 会自动创建，也可手动预创建）
- gs_hba.conf 中配置复制连接白名单

## Maven 依赖

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb.flink</groupId>
    <artifactId>flink-connector-gaussdb-cdc-3.6.x</artifactId>
    <version>3.3.0-1.20</version>
</dependency>
```

## 兼容性

| Flink 版本 | 兼容性 | 说明 |
|-----------|-------|------|
| 1.20.x | ✅ | FLIP-27 Source API，推荐版本 |
| 1.19.x | ⚠️ | 未充分验证 |
| 1.18.x | ⚠️ | 未充分验证 |
| 1.17.x | ❌ | CDC 3.6.x 依赖 Flink 1.20+ API |
| 2.x | ❌ | 未验证 |

## 驱动适配说明

本 Connector 基于 Debezium PostgreSQL Connector 构建，通过以下方式适配 GaussDB：

1. **GaussDB JDBC 驱动内置**：fat JAR 已包含 `gaussdbjdbc`，无需额外放置驱动到 `lib/` 目录
2. ** Shade Relocation**：打包时自动将 Debezium 中对 `org.postgresql` 的类引用重定向到 `com.huawei.gaussdb.jdbc`，确保运行时实际使用 GaussDB 驱动
3. **版本校验绕过**：GaussDB JDBC 兼容性报告版本号为 `9.2.4`，Connector 内部自动跳过 Debezium 的 `>= 9.4` 版本校验

## 性能参考

以下为实验室环境下的端到端 CDC 同步测试结果，测试模型为 6 列、单行约 200 字节的 `perf_cdc_test` 表，共 200,000 行数据。

> **注意**：实际业务数据量更大、结构更复杂，建议客户基于自身数据自行验证。

| 配置 | parallel-decode-num | decode-style | 同步行数 | 数据完整性 | 备注 |
|------|---------------------|-------------|---------|-----------|------|
| pd1 | 1 | j | 200,000 | ✅ 全部同步 | 串行解码，JSON 输出 |
| pd4 | 4 | j | 200,000 | ✅ 全部同步 | 并行解码，JSON 输出 |
| pd4 | 4 | b | 200,000 | ✅ 全部同步 | 并行解码，Binary 输出 |
| pd8 | 8 | j | 200,000 | ✅ 全部同步 | 并行解码，JSON 输出 |
| pd8 | 8 | b | 200,000 | ✅ 全部同步 | 并行解码，Binary 输出 |

**测试环境**：Flink 1.20.3（1 TaskManager），GaussDB 单节点，Sink=Print。

## Binary 解码器说明

`decode-style=b`（Binary）模式下，Connector 使用自定义的 `MppdbBinaryMessageDecoder` 解析 mppdb_decoding 的二进制 WAL 输出：

- 二进制协议格式：4字节 totalSize + 8字节 LSN + 1字节类型(B/C/I/U/D) + 数据体
- 支持完整的 INSERT / UPDATE / DELETE 事件捕获
- `parallel-decode-num` 和 `decode-style` 参数通过 `slot.stream.params` 自动传递给 GaussDB 复制协议
- **REPLICA IDENTITY**：DELETE 和 UPDATE 的 before image 取决于表的 REPLICA IDENTITY 设置（DEFAULT 仅含主键列，FULL 含全部列）

## 已知问题与修复记录

| 问题 | 根因 | 修复 | 影响 |
|------|------|------|------|
| Binary 模式下大量 `Unknown WAL record type` WARN，TIMESTAMP 全为 null，消费吞吐骤降（5 万条流式场景下 113s 仅消费到 26716 行） | `MppdbBinaryMessageDecoder.decodeBatch` 不使用 `totalSize` 字段强制对齐下一条记录边界，完全依赖各 `decodeXxx` 方法自身读到末尾；但 `decodeBegin/Commit` 不消费尾部 `P`/`F` 分隔符，一次错位即触发整批级联错位，后续把时间戳字符串字节（如 `0x32362d30352d3131` = ASCII `"26-05-11"`）误当 LSN/record header 解析 | 在 `decodeBatch` while 循环中用 `recordStartPos + 4 + totalSize` 计算 `bodyEndPos`，无论各子解码器读了多少都强制 `buf.position(bodyEndPos)` 重对齐，再在 batch 层统一消费 `P`/`F` 分隔符（参照 2.4.x `MppdbBinaryDecoder` 实现） | 5 万条流模式验证下 Unknown WAL WARN 归零，数据 0 丢失 |
| TIMESTAMP 字段 100% 变 null（无任何 ERROR/WARN） | `MppdbColumn.getValue()` 对所有类型统一返回原始字符串；Debezium `PostgresValueConverter` 对 TIMESTAMP(1114)/TIMESTAMPTZ(1184)/DATE(1082) 期望 `LocalDateTime`/`OffsetDateTime`/`LocalDate` 等 Java 时间对象或 Long 微秒数，传入字符串（如 `"2026-05-11 15:30:07.4848"`）时静默 fallback 为 null | 在 `MppdbColumn.getValue()` 中按 `typeOid` 预解析：TIMESTAMP → `LocalDateTime.parse`，TIMESTAMPTZ → `OffsetDateTime`，DATE → `Date.valueOf(LocalDate.parse)`；formatter 使用 `DateTimeFormatterBuilder + appendFraction(NANO_OF_SECOND, 0, 9, true)`，兼容 GaussDB 尾零裁剪产生的 0~9 位可变小数精度（避开 2.4.x `SSSSSS` 同款硬位数陷阱） | 5 万条流模式验证下 TIMESTAMP 微秒精度正确，0 null 0 parse fail |

## 测试

### 单元测试

```bash
mvn test -pl flink-connector-gaussdb-cdc-3.6.x
```

### 端到端验证（需真实 GaussDB 实例）

详细验证步骤请参见 [TEST.md](./TEST.md)。

```bash
# 1. 打包部署
mvn clean package -pl flink-connector-gaussdb-cdc-3.6.x -DskipTests
cp target/flink-connector-gaussdb-cdc-3.6.x-*.jar $FLINK_HOME/lib/

# 2. 启动 Flink 集群
$FLINK_HOME/bin/start-cluster.sh

# 3. SQL Client 提交 CDC 任务（注意：SQL 文件中需设置 result-mode=TABLEAU）
$FLINK_HOME/bin/sql-client.sh -f test_cdc.sql
```

## 瘦包打包

默认打包产物为 fat JAR，Connector JAR 内部已包含 GaussDB JDBC 驱动（`gaussdbjdbc-506.0.0.b058-jdk7`），部署时无需单独放置驱动 JAR。

如果环境中已统一管理 GaussDB JDBC 驱动（如已放入 `$FLINK_HOME/lib/`），或需要灵活切换驱动版本，可打出不含驱动的瘦包（thin JAR）。

### 打包瘦包

修改 `flink-connector-gaussdb-cdc-3.6.x/pom.xml`，将 `gaussdbjdbc` 依赖的 scope 改为 `provided`：

```xml
<!-- GaussDB JDBC Driver (replaces PostgreSQL driver) -->
<dependency>
    <groupId>com.huaweicloud.gaussdb</groupId>
    <artifactId>gaussdbjdbc</artifactId>
    <version>${gaussdb.jdbc.version}</version>
    <scope>provided</scope>  <!-- 添加这一行 -->
</dependency>
```

> **注意**：maven-shade-plugin 中的 `org.postgresql` → `com.huawei.gaussdb.jdbc` relocation 和 Debezium 过滤器是 CDC 功能必需的，**不可移除**。改为 `provided` 后，shade 插件会自动排除 GaussDB JDBC 驱动 JAR，其余 shade 配置无需修改。

重新打包：

```bash
mvn clean package -pl flink-connector-gaussdb-cdc-3.6.x -am -DskipTests
```

### 瘦包部署

打出瘦包后，需将 GaussDB JDBC 驱动单独放入 Flink `lib/` 目录：

```bash
# JDK 8/11 兼容版（推荐）
curl -o $FLINK_HOME/lib/gaussdbjdbc.jar \
  "https://repo1.maven.org/maven2/com/huaweicloud/gaussdb/gaussdbjdbc/506.0.0.b058-jdk7/gaussdbjdbc-506.0.0.b058-jdk7.jar"
```

### 两种打包方式对比

| 方式 | JAR 大小 | 内置驱动 | 适用场景 |
|------|---------|---------|---------|
| fat JAR（默认） | ~30 MB | ✅ 内置 | 推荐，部署简单 |
| thin JAR | ~数 MB | ❌ 不包含 | 已有驱动管理规范，需灵活切换驱动版本 |

## 许可证

本项目基于 Apache License 2.0 开源许可证。
