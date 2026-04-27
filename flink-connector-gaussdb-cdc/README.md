<p align="center">
  <h1 align="center">Flink GaussDB CDC Connector (1.13~1.17)</h1>
  <p align="center">基于 SourceFunction API 的 GaussDB CDC 连接器，兼容 Flink 1.13~1.17</p>
</p>

## 核心功能

- WAL 逻辑解码（mppdb_decoding）双通道：SQL 函数模式 + 流式复制 API
- INSERT / UPDATE / DELETE 变更捕获
- 全量快照 → 增量流式自动衔接
- 并行解码参数（parallel-decode-num, decode-style, sending-batch）
- **并行快照**：多 subtask 按 id 范围分片并行读取全量数据（设置 parallelism > 1 即可生效）
- **WAL 单实例**：快照阶段多 subtask 并行，增量阶段仅 subtask-0 读取 WAL 变更

## 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hostname` | 必填 | GaussDB 主机地址 |
| `port` | 8000 | 端口 |
| `database` | 必填 | 数据库名 |
| `schema` | `public` | Schema |
| `table-name` | 必填 | 监控的表名 |
| `username` | 必填 | 用户名 |
| `password` | 必填 | 密码 |
| `wal.mode` | `false` | `true` 启用 WAL 逻辑解码，`false` 使用轮询 |
| `decode.plugin` | `mppdb_decoding` | 逻辑解码插件，GaussDB 需用 `mppdb_decoding` |
| `slot.name` | `flink_cdc_slot` | 逻辑复制 slot 名 |
| `parallel-decode-num` | `1` | 并行解码线程数（1~20）。`1` 为串行解码，**只有 >1 时 decode-style 和 sending-batch 才生效** |
| `decode-style` | `b` | 解码输出格式：`b`=binary，`j`=json，`t`=text。**parallel-decode-num=1 时只能用 `j`** |
| `sending-batch` | `false` | `true` 时解码结果累积到 1MB 后批量发送，减少网络交互 |

> **参数依赖关系**：
> - `parallel-decode-num > 1` 时，`decode-style`（'b'/'j'/'t'）和 `sending-batch`（true/false）才生效
> - `parallel-decode-num = 1` 时，底层强制使用 JSON 输出，不支持 `decode-style='b'`
> - SQL 函数模式（`pg_logical_slot_peek_changes`）不支持 `decode-style` 和 `sending-batch`，只有 streaming replication API 支持

## 快速开始

### 部署 JAR 包

```bash
cp flink-connector-gaussdb-cdc-4.0-SNAPSHOT.jar $FLINK_HOME/lib/
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
    'database' = 'test',
    'schema' = 'public',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',

    -- WAL 模式
    'wal.mode' = 'true',
    'decode.plugin' = 'mppdb_decoding',
    'slot.name' = 'flink_cdc_slot',

    -- 并行解码（流式复制 API 模式生效）
    'parallel-decode-num' = '4',
    'decode-style' = 'b',
    'sending-batch' = 'true'
);
```

### DataStream API 使用

```java
GaussDBCDCSourceFunction source = GaussDBCDCSourceFunction.builder()
    .hostname("localhost")
    .port(8000)
    .database("test")
    .schema("public")
    .tableName("student")
    .username("root")
    .password("password")
    .walMode(true)
    .slotName("flink_cdc_slot")
    .decodePlugin("mppdb_decoding")
    .parallelDecodeNum(4)
    .decodeStyle("b")
    .sendingBatch(true)
    .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.addSource(source).print();
env.execute("GaussDB CDC Job");
```

## 前置条件

- `wal_level=logical`
- 逻辑复制槽
- 流式复制 API 需 gs_hba.conf 白名单 + enable_thread_pool=off 或 HA 端口

## Maven 依赖

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb.flink</groupId>
    <artifactId>flink-connector-gaussdb-cdc</artifactId>
    <version>4.0-SNAPSHOT</version>
</dependency>
```

## 兼容性

| Flink 版本 | 兼容性 | 说明 |
|-----------|-------|------|
| 1.13.x | ✅ | SourceFunction API 稳定 |
| 1.14.x | ✅ | SourceFunction API 稳定 |
| 1.15.x | ✅ | SourceFunction API 稳定 |
| 1.16.x | ✅ | SourceFunction API 稳定 |
| 1.17.x | ✅ | SourceFunction API 稳定 |
| 1.18+ | ⚠️ | 未验证，SourceFunction 在 1.18+ 标记为 @Deprecated 但仍可用 |
| 2.x | ❌ | SourceFunction API 已移除 |

> **说明**：Flink 从 1.18 开始推荐使用 FLIP-27 Source API，`SourceFunction` 被标记为 `@Deprecated` 但仍可运行。Flink 2.x 已移除 SourceFunction API，不兼容。

## 测试

### 单元测试

```bash
mvn test -pl flink-connector-gaussdb-cdc -Dcheckstyle.skip=true
```

当前覆盖率：213 个测试，行覆盖率 83%。

### 集成测试（需真实 GaussDB 实例）

```bash
mvn test -pl flink-connector-gaussdb-cdc \
    -Dtest=GaussDBCDCSourceFunctionITCase \
    -Dgaussdb.test.enabled=true \
    -Dcheckstyle.skip=true
```

集成测试验证项：
- JDBC 连接和驱动加载
- 逻辑复制 slot 创建和管理
- WalReplicationStream 流式变更捕获
- LSN 函数兼容性（pg_current_xlog_location）
- pg_logical_slot_peek_changes SQL 函数
- 并行快照分片读取
- MppdbBinaryDecoder 二进制解码

### 性能测试

测试 `decode-style='b'` 在不同 `parallel-decode-num` 下的解码效率（真实 GaussDB 实例）：

```bash
mvn test -pl flink-connector-gaussdb-cdc \
    -Dtest=GaussDBCDCBinaryDecodePerfITCase \
    -Dgaussdb.test.enabled=true \
    -Dcheckstyle.skip=true
```

**测试条件**：
- GaussDB 实例：1.92.120.69:8000
- 数据规模：10,000 条 INSERT
- 解码插件：mppdb_decoding
- 读取模式：streaming replication API

**结果**：

| parallel-decode-num | decode-style | 读取时间 | 吞吐量 |
|---------------------|--------------|----------|--------|
| 1（串行基准） | `j` (JSON) | 274 ms | ~36.5K changes/sec |
| 4 | `b` (binary) | 302 ms | ~33.2K changes/sec |
| 8 | `b` (binary) | 174 ms | ~57.6K changes/sec |

> **说明**：`parallel-decode-num=1` 时底层强制使用 JSON，不支持 binary。8 线程 binary 相比串行 JSON 提升约 **58%**。实际收益与数据规模、网络延迟、GaussDB 实例负载有关。

## 许可证

本项目基于 Apache License 2.0 开源许可证。
