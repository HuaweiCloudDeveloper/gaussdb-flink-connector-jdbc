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
| `replication.port` | （未设） | WAL 流式复制独立端口（HA 端口）。仅在 `wal.mode=true` 且 GaussDB `enable_thread_pool=on` 时必须设置为 HA 端口（通常为主端口+1，如 8001）。未设置时 replication 连接复用 `port`。 |
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
| `sslmode` | `prefer` | SSL 模式：`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full` |

> **参数依赖关系**：
> - `parallel-decode-num > 1` 时，`decode-style`（'b'/'j'/'t'）和 `sending-batch`（true/false）才生效
> - `parallel-decode-num = 1` 时，底层强制使用 JSON 输出，不支持 `decode-style='b'`
> - SQL 函数模式（`pg_logical_slot_peek_changes`）不支持 `decode-style` 和 `sending-batch`，只有 streaming replication API 支持

## 快速开始

### 部署 JAR 包

```bash
cp flink-connector-gaussdb-cdc-2.4.x-3.3.0-1.20.jar $FLINK_HOME/lib/
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
    'replication.port' = '8001',  -- 可选：当 GaussDB enable_thread_pool=on 时必须设置为 HA 端口
    'database' = 'test',
    'schema' = 'public',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'password',

    -- WAL 模式
    'wal.mode' = 'true',
    'decode.plugin' = 'mppdb_decoding',
    'slot.name' = 'flink_cdc_slot',

    -- SSL
    'sslmode' = 'disable',

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
    .replicationPort(8001)   // 可选：enable_thread_pool=on 环境必填为 HA 端口
    .database("test")
    .schema("public")
    .tableName("student")
    .username("root")
    .password("password")
    .walMode(true)
    .slotName("flink_cdc_slot")
    .decodePlugin("mppdb_decoding")
    .sslMode("disable")
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
- `parallel-decode-num=1` 时底层输出 JSON 格式（不支持 `decode-style='b'`），`decode-style` 和 `sending-batch` 仅在 `parallel-decode-num > 1` 时生效

## GaussDB 线程池模式下的端口分离（replication.port）

GaussDB 集中式实例开启 `enable_thread_pool=on` 后，端口协议被严格隔离：

| 端口 | 协议 | 连接方式 |
|------|------|---------|
| 主端口（如 8000） | 仅普通 JDBC | `jdbc:gaussdb://host:8000/db` |
| HA 端口（如 8001） | 仅 replication=database | `jdbc:gaussdb://host:8001/db?replication=database` |

如果用 `wal.mode=true`（流式复制）但只配 `port=8000`，WAL 复制连接会被内核拒绝并报错：

```
FATAL: replication should connect HA port in thread_pool
```

**解决方式**：同时配置 `port` 和 `replication.port`：

```sql
'port' = '8000',              -- 主端口：Snapshot + 轮询 + SQL 函数
'replication.port' = '8001',  -- HA 端口：仅用于 replication=database 流式复制
```

连接器会自动路由：
- 全量快照、SQL 函数模式 → `port`（8000）
- WAL 流式复制（`wal.mode=true` 时 `buildReplicationUrl` 自动重写 host:port）→ `replication.port`（8001）

> 若 GaussDB `enable_thread_pool=off`（或使用分布式 CN 节点），主端口同时支持普通 JDBC 和 replication，**可省略 `replication.port`**。

## 注意事项

### 复制槽独占

GaussDB 复制槽**同一时间只能被一个连接使用**。CDC 源表被多个 Flink 作业同时查询时（如 `INSERT INTO ... SELECT` 和 `SELECT * FROM` 同时执行），会报 `replication slot "xxx" is already active`。

**解决方案**：
- 不同作业使用不同的 `slot.name`
- 或先取消旧作业再执行新查询

### 轮询模式与 WAL 模式

| 模式 | `wal.mode` | 复制槽 | 适用场景 |
|------|-----------|--------|---------|
| 轮询 | `false` | 不需要 | 简单场景，表有自增主键 |
| WAL 流式 | `true` | **需要** | 实时性要求高，需捕获 DELETE/UPDATE |

> 轮询模式通过 JDBC 轮询检测变更，不支持 DELETE 捕获；WAL 模式通过逻辑解码实时推送变更。

## 已知问题与修复记录

| 问题 | 根因 | 修复 | 影响 |
|------|------|------|------|
| 串行解码 readPending 返回 0 条变更 | `parallel-decode-num=1` 时不传 `decode-style`，mppdb_decoding 默认输出 JSON，但代码用 MppdbBinaryDecoder 解码 | 判断条件改为 `parallelDecodeNum > 1 && "b".equals(decodeStyle)` 才走 binary 解码，否则走 JSON 解析 | 串行模式增量同步恢复 |
| MppdbBinaryDecoder 偏移错位 | binary 格式每条记录后有 1 字节分隔符（'P'/'F'），totalSize 不含该字节 | bodyEndPos 位置检查分隔符，有则 nextRecordPos = bodyEndPos + 1 | 并行解码 binary 模式增量同步恢复 |
| readPending 首次返回 null | forceUpdateStatus 后服务器需时间推送数据，首次调用返回 null 后直接 break | 首次 null 时等 100ms 重试一次 + forceUpdateStatus 后等 50ms | 首次读取不再丢失数据 |
| compatibleMode=mysql 导致连接关闭 | GaussDB 流式复制不支持 MySQL 兼容模式 | 移除 compatibleMode=mysql，改为 sslmode=disable | 流式复制连接不再被服务端关闭 |
| transient running 反序列化后为 false | Java transient 字段不保留初始值 | open() 中显式设置 this.running = true | WAL streaming loop 不再跳过 |
| 增量同步捕获其他表变更 | WAL 解码捕获数据库所有表变更 | 添加目标表名过滤，跳过非目标表 | 类型转换错误不再发生 |
| 轮询模式 `Column "xxx" does not exist` | `ChangeDataPoller` 硬编码了旧测试表的 7 个列名 | 改为通过 `DatabaseMetaData.getColumns()` 动态获取列名 | 轮询模式适配任意表结构 |
| 不支持 `sslmode` 配置 | JDBC URL 硬编码 `sslmode=disable` | 新增 `sslmode` 选项，默认 `prefer` | 可配置 SSL 加密传输 |
| TIMESTAMP 列解析失败 | `MppdbBinaryDecoder` 硬编码 `DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]")` 要求小数部分恰好 6 位；GaussDB 会去除尾零（如 `.4848`、`.48497`），只要实际位数不是 6，就抛 `could not be parsed, unparsed text found at index 19` | 改用 `DateTimeFormatterBuilder + appendFraction(NANO_OF_SECOND, 0, 9, true)`，支持 0~9 位可变精度 | 50000 条流模式验证下 TIMESTAMP 零丢失（之前约 10% 失败） |

## Maven 依赖

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb.flink</groupId>
    <artifactId>flink-connector-gaussdb-cdc-2.4.x</artifactId>
    <version>3.3.0-1.20</version>
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
mvn test -pl flink-connector-gaussdb-cdc-2.4.x -Dcheckstyle.skip=true
```

当前覆盖率：213 个测试，行覆盖率 83%。

### 集成测试（需真实 GaussDB 实例）

```bash
mvn test -pl flink-connector-gaussdb-cdc-2.4.x \
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
mvn test -pl flink-connector-gaussdb-cdc-2.4.x \
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
