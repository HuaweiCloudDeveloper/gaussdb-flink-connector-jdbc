<p align="center">
  <h1 align="center">Flink GaussDB CDC Connector (1.13~1.17)</h1>
  <p align="center">基于 SourceFunction API 的 GaussDB CDC 连接器，兼容 Flink 1.13~1.17</p>
</p>

## 与 `flink-connector-gaussdb-cdc-1.17` 的区别

| 特性 | `flink-connector-gaussdb-cdc` (本模块) | `flink-connector-gaussdb-cdc-1.17` |
|------|--------------------------------------|-----------------------------------|
| Flink 兼容范围 | **1.13, 1.14, 1.15, 1.16, 1.17** | 仅 1.17.x |
| Source API | `RichSourceFunction` + `CheckpointedFunction` (Flink 1.0+) | FLIP-27 `Source` + `SourceReader` + `SplitEnumerator` (Flink 1.12+) |
| TableSource | `SourceFunctionProvider` | `SourceProvider` |
| 快照并行 | 不支持（单线程快照） | 支持（SplitEnumerator 分片） |
| 核心功能 | 完全一致 | 完全一致 |

> **选择建议**：
> - 如需 **Flink 1.13~1.16 兼容**，使用本模块 `flink-connector-gaussdb-cdc`
> - 如需 **快照并行读取**（大表全量阶段加速），使用 `flink-connector-gaussdb-cdc-1.17`
> - 其余场景两个模块均可，本模块兼容范围更广

## 核心功能

与 `flink-connector-gaussdb-cdc-1.17` 完全一致：
- WAL 逻辑解码（mppdb_decoding）双通道：SQL 函数模式 + 流式复制 API
- INSERT / UPDATE / DELETE 变更捕获
- 全量快照 → 增量流式自动衔接
- 并行解码参数（parallel-decode-num, decode-style, sending-batch）

详细功能说明请参考 [flink-connector-gaussdb-cdc-1.17/README.md](../flink-connector-gaussdb-cdc-1.17/README.md)。

## 快速开始

### 部署 JAR 包

```bash
cp flink-connector-gaussdb-cdc-4.0-SNAPSHOT.jar $FLINK_HOME/lib/
cp flink-connector-base-<flink-version>.jar $FLINK_HOME/lib/
```

> **注意**：`flink-connector-base` 的版本必须与 Flink 版本一致（如 Flink 1.13.6 对应 `flink-connector-base-1.13.6.jar`）。

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

与 `flink-connector-gaussdb-cdc-1.17` 相同：
- `wal_level=logical`
- 逻辑复制槽
- 流式复制 API 需 gs_hba.conf 白名单 + enable_thread_pool=off 或 HA 端口

详见 [flink-connector-gaussdb-cdc-1.17/README.md](../flink-connector-gaussdb-cdc-1.17/README.md#前置条件)。

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

> **说明**：Flink 从 1.18 开始推荐使用 FLIP-27 Source API，`SourceFunction` 被标记为 `@Deprecated` 但仍可运行。如需 Flink 2.x 支持，请使用 `flink-connector-gaussdb-cdc-1.17` 模块的架构并适配新版 API。

## 许可证

本项目基于 Apache License 2.0 开源许可证。
