# GaussDB CDC Connector 手动验证文档

## 环境信息

- **Flink 版本**: 1.17.2
- **GaussDB 版本**: 505.2.1.SPC0800
- **CDC Connector**: flink-connector-gaussdb-cdc-1.17-4.0-SNAPSHOT.jar
- **GaussDB 驱动**: gaussdbjdbc-JRE7.jar

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
-- 授予复制权限
ALTER USER root REPLICATION;

-- 创建复制槽（使用 GaussDB 原生 mppdb_decoding 插件）
SELECT pg_create_logical_replication_slot('test_slot', 'mppdb_decoding');
```

### 2. 准备测试表

在 GaussDB 中创建测试表：
```sql
CREATE TABLE cdc_test (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入初始数据
INSERT INTO cdc_test (id, name, age) VALUES (1, 'Alice', 20);
INSERT INTO cdc_test (id, name, age) VALUES (2, 'liujia', 20);
```

### 3. Flink 部署

#### 3.1 放置 JAR 包

将 JAR 包放置到 Flink lib 目录：
```bash
cp flink-connector-gaussdb-cdc-1.17-4.0-SNAPSHOT.jar $FLINK_HOME/lib/
cp gaussdbjdbc-JRE7.jar $FLINK_HOME/lib/
```

#### 3.2 重启 Flink 集群（重要）

**必须重启 Flink 集群**，否则 lib 目录下的新 JAR 包不会被加载到类路径：

```bash
cd /usr/local/flink-1.17.2

# 停止集群
./bin/stop-cluster.sh

# 等待几秒
sleep 3

# 启动集群
./bin/start-cluster.sh
```

## 验证步骤

### 步骤 1: 启动 Flink SQL Client

```bash
cd /usr/local/flink-1.17.2
./bin/sql-client.sh
```

### 步骤 2: 创建 CDC Source 表

```sql
CREATE TABLE cdc_test_source (
    id INT,
    name STRING,
    age INT,
    updated_at TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '1.92.120.69',
    'port' = '8000',
    'username' = 'root',
    'password' = 'GuassDB123',
    'database' = 'test',
    'table-name' = 'cdc_test',
    'slot.name' = 'test_slot'
);
```

**预期结果**: `CREATE TABLE` 成功，无报错。

### 步骤 3: 查询 CDC 数据

```sql
SELECT * FROM cdc_test_source;
```

**预期结果**: 显示 `cdc_test` 表中的初始数据。

### 步骤 4: 创建 JDBC Sink 表（用于写入测试）

```sql
CREATE TABLE cdc_test_sink (
    id INT,
    name STRING,
    age INT,
    updated_at TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:gaussdb://1.92.120.69:8000/test?compatibleMode=mysql',
    'table-name' = 'cdc_test',
    'username' = 'root',
    'password' = 'GuassDB123'
);
```

### 步骤 5: 插入新数据测试 CDC 实时捕获

```sql
INSERT INTO cdc_test_sink (id, name, age) VALUES (3, '刘大壮', 18);
```

**预期结果**: 
```
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 3a3367b8f7f889f31c101a461003b5dc
```

### 步骤 6: 验证 CDC 捕获到 INSERT 数据

再次查询 CDC Source：
```sql
SELECT * FROM cdc_test_source;
```

**预期结果**: 能看到刚插入的 "刘大壮" 数据。

### 步骤 7: 在 GaussDB 中执行 UPDATE 测试 CDC 捕获

由于 Flink 1.17 SQL 客户端不支持 UPDATE 语句，直接在 GaussDB 中执行：

```sql
-- 在 GaussDB 客户端执行
UPDATE cdc_test SET age = 30, updated_at = CURRENT_TIMESTAMP WHERE id = 3;
```

**预期结果**: UPDATE 执行成功，影响 1 行。

### 步骤 8: 验证 CDC 捕获到 UPDATE 数据

在 Flink SQL Client 中查询：
```sql
SELECT * FROM cdc_test_source;
```

**预期结果**: 能看到 id=3 的数据 age 已更新为 30。

### 步骤 9: 在 GaussDB 中执行 DELETE 测试 CDC 捕获

由于 Flink 1.17 SQL 客户端不支持 DELETE 语句，直接在 GaussDB 中执行：

```sql
-- 在 GaussDB 客户端执行
DELETE FROM cdc_test WHERE id = 3;
```

**预期结果**: DELETE 执行成功，影响 1 行。

### 步骤 10: 验证 CDC 捕获到 DELETE 数据

在 Flink SQL Client 中查询：
```sql
SELECT * FROM cdc_test_source;
```

**预期结果**: id=3 的数据已消失（DELETE 事件已被 CDC 捕获并处理）。

## 测试结果

| 测试项 | 状态 | 备注 |
|--------|------|------|
| lib 目录部署 | ✅ 通过 | 重启 Flink 集群后 lib 目录 JAR 包被正确加载 |
| CDC Source 表创建 | ✅ 通过 | 成功创建表，参数识别正常 |
| 初始数据读取 | ✅ 通过 | 成功读取 cdc_test 表中的 2 条初始数据 |
| JDBC Sink 表创建 | ✅ 通过 | 成功创建 Sink 表 |
| INSERT 数据写入 | ✅ 通过 | 成功插入新数据，Job ID: 3a3367b8f7f889f31c101a461003b5dc |
| CDC 捕获 INSERT | ✅ 通过 | CDC Source 能实时捕获到 INSERT 事件 |
| UPDATE 数据修改 | ✅ 通过 | 在 GaussDB 中执行 UPDATE 成功 |
| CDC 捕获 UPDATE | ✅ 通过 | CDC Source 能实时捕获到 UPDATE 事件 |
| DELETE 数据删除 | ✅ 通过 | 在 GaussDB 中执行 DELETE 成功 |
| CDC 捕获 DELETE | ✅ 通过 | CDC Source 能实时捕获到 DELETE 事件 |

## 测试结论

✅ **GaussDB CDC Connector 功能验证通过**

- 支持动态表结构获取，无需硬编码列名
- 支持全量快照读取
- 支持实时 CDC 数据捕获（INSERT/UPDATE/DELETE）
- 与 JDBC Sink 配合可实现完整的数据流转

### 变更事件支持情况

| 变更类型 | 支持状态 | 测试方式 |
|----------|----------|----------|
| INSERT | ✅ 支持 | Flink JDBC Sink 写入，CDC 实时捕获 |
| UPDATE | ✅ 支持 | GaussDB 客户端执行 UPDATE，CDC 实时捕获 |
| DELETE | ✅ 支持 | GaussDB 客户端执行 DELETE，CDC 实时捕获 |

## 已知限制

1. **Flink 1.17 流模式不支持 DELETE/UPDATE**: 这是 Flink SQL 引擎的限制，不是 Connector 的问题
2. **CDC 仅支持 Source**: `gaussdb-cdc` 只能作为 Source，不能作为 Sink
3. **必须重启 Flink 集群**: 添加新的 JAR 包到 lib 目录后，必须重启 Flink 集群才能生效

## 附录

### 测试环境详细信息

```
GaussDB 表结构:
- id (int4) - 主键
- name (varchar)
- age (int4)
- updated_at (timestamp)

初始数据:
- id=1, name='Alice', age=20
- id=2, name='liujia', age=20

复制槽:
- slot_name: test_slot
- plugin: mppdb_decoding
- slot_type: logical
```

### 常见问题

**Q: 为什么添加了 JAR 包到 lib 目录后还是找不到类？**
A: **必须重启 Flink 集群**。Flink 只在启动时扫描 lib 目录构建类路径，运行时添加的 JAR 包不会被加载。

**Q: CDC 支持哪些变更类型？**
A: 支持 INSERT、UPDATE、DELETE 三种变更类型的捕获。

**Q: 为什么 Flink 1.17 不支持 DELETE/UPDATE？**
A: 这是 Flink 1.17 SQL 引擎在流模式下的固有限制，升级到 1.18+ 可能会有改善。
