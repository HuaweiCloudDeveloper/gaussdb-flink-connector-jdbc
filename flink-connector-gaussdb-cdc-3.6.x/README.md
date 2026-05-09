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

### 必需参数

| 参数 | 说明 |
|------|------|
| `hostname` | GaussDB 主机地址 |
| `port` | 端口，默认 `8000` |
| `database-name` | 数据库名 |
| `schema-name` | Schema，默认 `public` |
| `table-name` | 监控的表名，支持正则匹配多表 |
| `username` | 用户名 |
| `password` | 密码 |
| `slot.name` | 逻辑复制 slot 名 |

### 可选参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `decoding.plugin.name` | `mppdb_decoding` | 逻辑解码插件，GaussDB 需用 `mppdb_decoding` |
| `changelog-mode` | `all` | `all` = retract 流（默认）；`upsert` = upsert 流 |
| `scan.startup.mode` | `initial` | 启动模式：`initial` / `snapshot` / `latest-offset` / `committed-offset` |
| `scan.incremental.snapshot.enabled` | `true` | 是否启用增量快照 |
| `scan.incremental.snapshot.chunk.size` | `8096` | 快照分片大小 |
| `connection.pool.size` | `20` | JDBC 连接池大小 |
| `connect.timeout` | `30s` | JDBC 连接超时 |
| `heartbeat.interval.ms` | `30s` | 心跳间隔，用于追踪复制 slot 进度 |
| `parallel-decode-num` | `1` | 并行解码线程数（1~20）。`1` 为串行解码，**只有 >1 时 decode-style 和 sending-batch 才生效** |
| `decode-style` | `b` | 解码输出格式：`b`=binary，`j`=json，`t`=text。**parallel-decode-num=1 时只能用 `j`** |
| `sending-batch` | `false` | `true` 时解码结果累积到 1MB 后批量发送，减少网络交互 |

> **参数依赖关系**：
> - `parallel-decode-num > 1` 时，`decode-style`（'b'/'j'/'t'）和 `sending-batch`（true/false）才生效
> - `parallel-decode-num = 1` 时，底层强制使用 JSON 输出，不支持 `decode-style='b'`

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
    <version>4.0-SNAPSHOT</version>
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

### Binary 解码器说明

`decode-style=b`（Binary）模式下，Connector 使用自定义的 `MppdbBinaryMessageDecoder` 解析 mppdb_decoding 的二进制 WAL 输出：

- 二进制协议格式：4字节 totalSize + 8字节 LSN + 1字节类型(B/C/I/U/D) + 数据体
- 支持完整的 INSERT / UPDATE / DELETE 事件捕获
- `parallel-decode-num` 和 `decode-style` 参数通过 `slot.stream.params` 自动传递给 GaussDB 复制协议
- **REPLICA IDENTITY**：DELETE 和 UPDATE 的 before image 取决于表的 REPLICA IDENTITY 设置（DEFAULT 仅含主键列，FULL 含全部列）

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

## 许可证

本项目基于 Apache License 2.0 开源许可证。
