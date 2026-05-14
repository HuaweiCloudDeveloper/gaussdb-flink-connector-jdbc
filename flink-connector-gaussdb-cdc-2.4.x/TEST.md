# GaussDB CDC Connector 2.4.x 手动验证文档

## 环境信息

- **Flink 版本**: 1.17.2
- **GaussDB 版本**: 506.0.0.b058
- **CDC Connector**: flink-connector-gaussdb-cdc-2.4.x-3.3.0-1.20.jar
- **GaussDB 驱动**: gaussdbjdbc-506.0.0.b058.jar（内置）

## 前置条件

### 1. GaussDB 配置

#### 设置 wal_level 为 logical
通过 GaussDB Console 控制台配置：
1. 登录 GaussDB Console 控制台
2. 进入「参数管理」→「高危参数」
3. 找到 `wal_level` 参数，修改为 `logical`
4. 重启 GaussDB 实例生效

#### 创建复制槽
```sql
ALTER USER root REPLICATION;
SELECT pg_create_logical_replication_slot('flink_cdc_slot', 'mppdb_decoding');
```

#### 流式复制 API 额外配置（如需并行解码）

1. **gs_hba.conf 白名单**：添加 replication 类型访问规则
   ```
   host    replication    root    <客户端IP>/32    sha256
   ```
2. **enable_thread_pool**：集中式默认 `on`，复制连接需走 HA 端口（数据端口+1，如 8001）。如需走数据端口 8000，需关闭该参数

### 2. 准备测试表

```sql
CREATE SCHEMA IF NOT EXISTS test_cdc;
CREATE TABLE test_cdc.flink_cdc_test (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INT
);

INSERT INTO test_cdc.flink_cdc_test (name, age) VALUES ('Alice', 30);
INSERT INTO test_cdc.flink_cdc_test (name, age) VALUES ('Bob', 25);
```

### 3. Flink 部署

```bash
cp flink-connector-gaussdb-cdc-2.4.x-3.3.0-1.20.jar $FLINK_HOME/lib/
$FLINK_HOME/bin/stop-cluster.sh
sleep 3
$FLINK_HOME/bin/start-cluster.sh
```

## 验证步骤

### 测试 1: 串行解码模式（parallel-decode-num=1，默认 JSON 输出）

```sql
CREATE TABLE gaussdb_cdc_source (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '1.92.120.69',
    'port' = '8000',
    'username' = 'root',
    'password' = 'GaussDB123',
    'database' = 'postgres',
    'schema' = 'test_cdc',
    'table-name' = 'flink_cdc_test',
    'slot.name' = 'flink_cdc_serial',
    'wal.mode' = 'true',
    'decode.plugin' = 'mppdb_decoding',
    'parallel-decode-num' = '1'
);

SELECT * FROM gaussdb_cdc_source;
```

**预期结果**: 显示快照数据（Alice, Bob），增量阶段插入新数据后也能实时捕获。

**验证增量同步**:
```sql
-- 在 GaussDB 中执行
INSERT INTO test_cdc.flink_cdc_test (name, age) VALUES ('Charlie', 35);
```
Flink SQL 查询结果中应出现 Charlie。

### 测试 2: 并行解码 binary 模式（parallel-decode-num=4 + decode-style=b）

```sql
CREATE TABLE gaussdb_cdc_binary (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '1.92.120.69',
    'port' = '8000',
    'username' = 'root',
    'password' = 'GaussDB123',
    'database' = 'postgres',
    'schema' = 'test_cdc',
    'table-name' = 'flink_cdc_test',
    'slot.name' = 'flink_cdc_binary',
    'wal.mode' = 'true',
    'decode.plugin' = 'mppdb_decoding',
    'parallel-decode-num' = '4',
    'decode-style' = 'b',
    'sending-batch' = 'true'
);

SELECT * FROM gaussdb_cdc_binary;
```

**预期结果**: 显示快照数据，增量阶段插入新数据后也能实时捕获。

**验证增量同步**:
```sql
-- 在 GaussDB 中执行
INSERT INTO test_cdc.flink_cdc_test (name, age) VALUES ('Diana', 28);
```
Flink SQL 查询结果中应出现 Diana。

### 测试 3: SQL 函数模式（wal.mode=false）

```sql
CREATE TABLE gaussdb_cdc_poll (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '1.92.120.69',
    'port' = '8000',
    'username' = 'root',
    'password' = 'GaussDB123',
    'database' = 'postgres',
    'schema' = 'test_cdc',
    'table-name' = 'flink_cdc_test',
    'slot.name' = 'flink_cdc_poll'
);

SELECT * FROM gaussdb_cdc_poll;
```

**预期结果**: 显示快照数据。

## 测试结果

| 测试项 | 状态 | 备注 |
|--------|------|------|
| 串行解码快照 | ✅ 通过 | 成功读取全量数据 |
| 串行解码增量同步 | ✅ 通过 | INSERT 变更实时捕获 |
| 并行解码 binary 快照 | ✅ 通过 | 成功读取全量数据 |
| 并行解码 binary 增量同步 | ✅ 通过 | INSERT 变更实时捕获 |
| SQL 函数模式快照 | ✅ 通过 | 成功读取全量数据 |

## 已修复问题

| 问题 | 根因 | 修复 |
|------|------|------|
| 串行解码 readPending 返回 0 条变更 | `parallel-decode-num=1` 时 mppdb_decoding 默认输出 JSON，代码误用 MppdbBinaryDecoder | 判断条件改为 `parallelDecodeNum > 1 && "b".equals(decodeStyle)` 才走 binary 解码 |
| MppdbBinaryDecoder 偏移错位 | binary 格式每条记录后有 1 字节分隔符（'P'/'F'），totalSize 不含 | bodyEndPos 位置检查分隔符，有则 nextRecordPos = bodyEndPos + 1 |
| readPending 首次返回 null | forceUpdateStatus 后服务器需时间推送数据 | 首次 null 时等 100ms 重试 + forceUpdateStatus 后等 50ms |
| compatibleMode=mysql 导致连接关闭 | GaussDB 流式复制不支持 MySQL 兼容模式 | 移除 compatibleMode=mysql，改为 sslmode=disable |
| transient running 反序列化后为 false | Java transient 字段不保留初始值 | open() 中显式设置 this.running = true |
| 增量同步捕获其他表变更 | WAL 解码捕获数据库所有表变更 | 添加目标表名过滤，跳过非目标表 |

## 注意事项

1. 每次更换 slot 名称或重启测试前，需清理 GaussDB 中残留的复制槽：
   ```sql
   SELECT pg_drop_replication_slot('slot_name');
   ```
2. `parallel-decode-num=1` 时 `decode-style` 和 `sending-batch` 参数无效，mppdb_decoding 默认输出 JSON
3. `sending-batch=1` 会累积到 1MB 后批量发送，低频变更场景可能有延迟
4. 集中式 GaussDB `enable_thread_pool=on` 时，流式复制连接需走 HA 端口（数据端口+1）
