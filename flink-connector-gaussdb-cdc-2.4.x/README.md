# Flink GaussDB CDC Connector 2.4.x

GaussDB CDC 连接器，支持 Flink 2.4.x，基于 WAL 逻辑解码（mppdb_decoding）实时捕获数据变更。

## 核心特性

- **多表捕获**：支持正则匹配多张表，同时捕获异构表结构（不同 schema 的表）
- **三种 JSON 输出格式**：Debezium、Canal、HE（自定义 Canal 变体）
- **WAL 双通道**：流式复制 API（高吞吐）+ SQL 函数模式（零额外配置）
- **轮询模式**：wal.mode=false 时使用基于轮询的 CDC，无需 WAL 逻辑解码
- **Exactly-Once**：checkpoint 完成后才确认 WAL LSN，保证数据不丢
- **SQL 注入防护**：所有标识符自动引用，兼容 M-compatible 模式

## 版本兼容性

| 连接器版本 | Flink 版本 | GaussDB 版本 | JAR 包 |
|-----------|-----------|-------------|--------|
| flink-connector-gaussdb-cdc-2.4.x | 2.4.x | 505.2.1.SPC0800+ | fat (1.7MB) / thin (100KB) |

## 前置条件

### 系统要求
- JDK 8/11
- Flink 2.4.x

### 数据库要求

#### WAL 逻辑解码模式（wal.mode=true）
- GaussDB `wal_level = logical`
- 用户有 REPLICATION 权限
- 创建逻辑复制槽：`SELECT pg_create_logical_replication_slot('flink_cdc_slot', 'mppdb_decoding');`
- 流式复制 API 模式额外需要 gs_hba.conf 白名单（见 CDC 1.17 README 详述）

#### 轮询模式（wal.mode=false，默认）
- 无需 WAL 配置，通过定期查询表数据捕获变更
- 表必须有主键

## JAR 包说明

### fat 包（推荐）
`flink-connector-gaussdb-cdc-2.4.x-3.3.0-1.20-fat.jar`（1.7MB）
- 内置 GaussDB JDBC 驱动，部署时只需此一个 JAR + Flink 自带的 connector-base

### thin 包
`flink-connector-gaussdb-cdc-2.4.x-3.3.0-1.20-thin.jar`（100KB）
- 不含 JDBC 驱动，需额外部署 `gaussdbjdbc-506.0.0.b058-jdk7.jar`

```bash
# fat 包部署
cp flink-connector-gaussdb-cdc-2.4.x-3.3.0-1.20-fat.jar $FLINK_HOME/lib/

# thin 包部署
cp flink-connector-gaussdb-cdc-2.4.x-3.3.0-1.20-thin.jar $FLINK_HOME/lib/
cp gaussdbjdbc-506.0.0.b058-jdk7.jar $FLINK_HOME/lib/
```

## 配置参数

### 通用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| connector | 是 | - | 连接器类型：`gaussdb-cdc` |
| hostname | 是 | - | GaussDB 主机地址 |
| port | 否 | 8000 | GaussDB 端口 |
| replication.port | 否 | - | WAL 流式复制专用端口（HA 端口） |
| database | 是 | - | 数据库名 |
| schema | 否 | public | Schema 名称 |
| table-name | 是 | - | 表名，支持正则匹配多表 |
| username | 是 | - | 用户名 |
| password | 是 | - | 密码 |
| sslmode | 否 | prefer | SSL 连接模式 |

### CDC 参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| wal.mode | 否 | false | 是否启用 WAL 逻辑解码模式；false 时使用轮询模式 |
| decode.plugin | 否 | mppdb_decoding | 逻辑解码插件名称 |
| plugin.name | 否 | - | `decode.plugin` 的废弃别名，不要同时配置 |
| slot.name | 否 | flink_cdc_slot | 复制槽名称 |
| snapshot.mode | 否 | true | 是否先读取全量快照 |
| chunk.size | 否 | 1000 | 快照/轮询 JDBC fetch 大小 |
| poll.interval.ms | 否 | 1000 | 轮询间隔（毫秒） |
| connect.timeout.ms | 否 | 30000 | 连接超时（毫秒） |

### 输出格式参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| output.format | 否 | raw | 输出类型：`raw`（固定字段 RowData）/ `json`（单列 JSON 字符串） |
| output.json.format | 否 | debezium | `output.format=json` 时生效，见下方详细说明 |

#### output.format=raw（默认）
按源表字段输出 RowData，要求所有表结构相同（同构表）。

#### output.format=json
输出单列 STRING 类型 JSON，支持异构表（不同 schema 的表）。CDC 源表需定义为单个 STRING 字段。

`output.json.format` 支持三种格式：

**debezium**（默认）— Debezium 标准 JSON，兼容 Flink CDC debezium-json 格式
```json
{"before":null,"after":{"id":"1","name":"Alice"},
 "source":{"connector":"gaussdb","db":"postgres","schema":"sqlbuilder1","table":"orders","ts_ms":1784336396664},
 "op":"c","ts_ms":1784336396664}
```

**canal** — Alibaba Canal 标准 JSON
```json
{"data":{"id":"1","name":"Alice"},"old":null,
 "database":"postgres","table":"orders","type":"INSERT",
 "pkNames":["id"],"es":1784336549776,"ts":1784336549776,
 "isDdl":false,"sqlType":{},"mysqlType":{}}
```

**he** — 基于 Canal 的自定义格式
```json
{"database":"postgres","table":"orders",
 "optType":"INSERT","pkNames":["id"],"pkValues":"1",
 "es":1784336578232,"ts":1784336578232,
 "data":{"id":"1","name":"Alice"},"old":null}
```

三种格式对比：

| 字段 | debezium | canal | he |
|------|----------|-------|-----|
| 操作类型字段 | `op`（c/u/d） | `type`（INSERT/UPDATE/DELETE） | `optType`（INSERT/UPDATE/DELETE） |
| 变更后数据 | `after` | `data` | `data` |
| 变更前数据 | `before` | `old` | `old` |
| 主键值 | 无 | `pkNames` | `pkNames` + `pkValues` |
| DDL 标记 | 无 | `isDdl` | 无 |
| 类型映射 | 无 | `sqlType`/`mysqlType` | 无 |

> `he` 格式将 `type` 改为 `optType` 避免与业务字段名 `type` 冲突，并增加 `pkValues` 字段。

### 并行解码参数（WAL 模式）

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| parallel-decode-num | 1 | 并行解码线程数，1~20 |
| decode-style | b | 解码格式：b=binary，j=json，t=text（仅流式复制 API 模式生效） |
| sending-batch | false | 批量发送，true=累积到 1MB 后发送（仅流式复制 API 模式生效） |

## 多表捕获示例

### 同构多表（raw 格式）

```sql
CREATE TABLE orders_cdc (
    id INT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database' = 'test',
    'schema' = 'public',
    'table-name' = 'orders_.*',
    'username' = 'root',
    'password' = 'password',
    'wal.mode' = 'true',
    'slot.name' = 'flink_cdc_slot'
);
```

### 异构多表（json 格式）

```sql
CREATE TABLE cdc_events (
    data STRING
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'database' = 'test',
    'schema' = 'public',
    'table-name' = 'orders_.*|products_.*',
    'username' = 'root',
    'password' = 'password',
    'wal.mode' = 'true',
    'slot.name' = 'flink_cdc_slot',
    'output.format' = 'json',
    'output.json.format' = 'he'
);
```

## REPLICA IDENTITY 与 old 字段

默认情况下，UPDATE/DELETE 的 `old`/`before` 字段只包含主键列。获取完整旧值需：

```sql
ALTER TABLE my_table REPLICA IDENTITY FULL;
```

## 注意事项

1. **table-name 正则匹配**：含正则元字符（`.*+?^$[]()|\\`）时按正则匹配，否则按精确匹配（向后兼容）
2. **复合主键**：不支持复合主键，会报错提示
3. **非数字主键**：自动回退到单 subtask 快照，不做并行范围切分
4. **Schema 变更**：WAL 重连后检测到表结构变化会报错，防止输出错误数据
5. **Flink blob 缓存**：替换 JAR 后需完整重启集群（stop-cluster.sh + start-cluster.sh）
6. **事务缓冲延迟**：WAL 事件在 COMMIT 读取后才输出，单条 INSERT 可能延迟到下次 WAL 读取周期

## 许可证

Apache License 2.0
