# GaussDB JDBC Connector 1.17 手动验证文档

## 环境信息

- **Flink 版本**: 1.17.2
- **GaussDB 版本**: 505.2.1.SPC0800
- **JDBC Connector**: flink-connector-jdbc-gaussdb-1.17-4.0-SNAPSHOT.jar
- **GaussDB 驱动**: gaussdbjdbc-JRE7.jar

## 当前支持能力

### 1. 执行模式支持

| 模式 | 支持状态 | 说明 |
|------|---------|------|
| **批模式 (Batch)** | ✅ 支持 | 一次性读取/写入全量数据 |
| **流模式 (Streaming)** | ✅ 支持 | 持续增量读取（需配合分区或轮询配置） |

### 2. 数据操作语义

| 操作类型 | 支持状态 | 说明 |
|---------|---------|------|
| **INSERT** | ✅ 支持 | 插入新数据 |
| **UPSERT** | ✅ 支持 | 主键存在则更新，不存在则插入 |
| **SELECT** | ✅ 支持 | 支持投影、过滤、聚合等标准 SQL |
| **DELETE** | ❌ 不支持 | Flink 1.17 流模式限制 |
| **UPDATE** | ❌ 不支持 | Flink 1.17 流模式限制 |

### 3. 数据类型支持

| GaussDB 类型 | Flink 类型 | 支持状态 |
|-------------|-----------|---------|
| INT / INTEGER | INT | ✅ |
| BIGINT | BIGINT | ✅ |
| VARCHAR / TEXT | STRING | ✅ |
| DECIMAL / NUMERIC | DECIMAL | ✅ |
| DATE | DATE | ✅ |
| TIMESTAMP | TIMESTAMP | ✅ |
| BOOLEAN | BOOLEAN | ✅ |
| DOUBLE / FLOAT | DOUBLE | ✅ |
| 数组类型 | ARRAY | ✅ |

## 验证场景与参数配置

### 场景 1: 批模式全量读取 (Source)

**场景描述**: 一次性读取 GaussDB 表全量数据

**必需参数**:
```sql
'connector' = 'gaussdb',           -- 固定值
'url' = 'jdbc:gaussdb://<host>:<port>/<database>?compatibleMode=mysql',
'table-name' = '<table_name>',     -- 表名
'username' = '<username>',         -- 用户名
'password' = '<password>'          -- 密码
```

**可选参数**:
```sql
'scan.fetch-size' = '100',         -- 每次获取行数，默认 0
'scan.auto-commit' = 'true'        -- 自动提交，默认 true
```

---

### 场景 2: 流模式增量读取 (Source)

**场景描述**: 持续轮询读取增量数据

**必需参数**: 同场景 1

**可选参数**:
```sql
'scan.partition.column' = 'id',    -- 分区列名（用于并行读取）
'scan.partition.num' = '4',        -- 分区数量
'scan.partition.lower-bound' = '1', -- 分区下界
'scan.partition.upper-bound' = '1000' -- 分区上界
```

> **注意**: 流模式实际为周期性批处理，需配合分区或增量字段实现"增量"效果

---

### 场景 3: 批量写入 (Sink)

**场景描述**: 批量写入数据到 GaussDB

**必需参数**:
```sql
'connector' = 'gaussdb',
'url' = 'jdbc:gaussdb://<host>:<port>/<database>?compatibleMode=mysql',
'table-name' = '<table_name>',
'username' = '<username>',
'password' = '<password>'
```

**可选参数**:
```sql
'sink.buffer-flush.max-rows' = '100',    -- 缓冲最大行数，默认 100
'sink.buffer-flush.interval' = '1s',     -- 刷新间隔，默认 1s
'sink.max-retries' = '3'                 -- 最大重试次数，默认 3
```

---

### 场景 4: UPSERT 写入 (Sink)

**场景描述**: 主键冲突时更新，否则插入

**必需参数**: 同场景 3

**关键要求**:
- 表必须定义主键 (`PRIMARY KEY (id) NOT ENFORCED`)
- 底层使用 `INSERT ... ON DUPLICATE KEY UPDATE` 语法

**示例**:
```sql
CREATE TABLE student_sink (
    id INT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED   -- 必须定义主键
) WITH (
    'connector' = 'gaussdb',
    'url' = 'jdbc:gaussdb://...',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'xxx'
);

-- UPSERT 操作：主键存在则更新，不存在则插入
INSERT INTO student_sink VALUES (1, '张三');
INSERT INTO student_sink VALUES (1, '张三更新');  -- 触发更新
```

---

### 场景 5: 数据类型映射验证

**场景描述**: 验证 GaussDB 与 Flink 类型映射

**测试表结构**:
```sql
CREATE TABLE type_test (
    col_int INT,
    col_bigint BIGINT,
    col_varchar VARCHAR(100),
    col_decimal DECIMAL(10,2),
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_bool BOOLEAN,
    col_double DOUBLE
);
```

**参数配置**: 标准 Source/Sink 参数

---

### 场景 6: 聚合查询 (Source)

**场景描述**: 验证 Source 支持 Flink SQL 聚合操作

**支持操作**:
- `GROUP BY` 分组聚合
- `COUNT`, `SUM`, `AVG`, `MAX`, `MIN` 等聚合函数
- `WHERE` 条件过滤

**限制**:
- 流模式下 `ORDER BY` 仅支持时间字段
- 非时间字段 `ORDER BY` 仅在批模式支持

---

## 前置条件

## 前置条件

### 1. 准备测试表

在 GaussDB 中创建测试表：
```sql
CREATE TABLE student (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    class_name VARCHAR(50),
    score DECIMAL(5,2),
    created_date DATE
);
```

### 2. Flink 部署

#### 2.1 放置 JAR 包

将 JAR 包放置到 Flink lib 目录：
```bash
cp flink-connector-jdbc-gaussdb-1.17-4.0-SNAPSHOT.jar $FLINK_HOME/lib/
cp gaussdbjdbc-JRE7.jar $FLINK_HOME/lib/
```

#### 2.2 重启 Flink 集群（重要）

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

### 步骤 2: 创建 JDBC Source 表

```sql
CREATE TABLE student_source (
    id INT,
    name STRING,
    gender STRING,
    age INT,
    class_name STRING,
    score DECIMAL(5,2),
    created_date DATE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb',
    'url' = 'jdbc:gaussdb://1.92.120.69:8000/test?compatibleMode=mysql',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'GuassDB123'
);
```

**预期结果**: `CREATE TABLE` 成功，无报错。

### 步骤 3: 查询 Source 数据

```sql
SELECT * FROM student_source;
```

**预期结果**: 显示 `student` 表中的数据。

### 步骤 4: 创建 JDBC Sink 表

```sql
CREATE TABLE student_sink (
    id INT,
    name STRING,
    gender STRING,
    age INT,
    class_name STRING,
    score DECIMAL(5,2),
    created_date DATE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb',
    'url' = 'jdbc:gaussdb://1.92.120.69:8000/test?compatibleMode=mysql',
    'table-name' = 'student',
    'username' = 'root',
    'password' = 'GuassDB123'
);
```

### 步骤 5: 测试 INSERT 写入

```sql
INSERT INTO student_sink (id, name, gender, age, class_name, score, created_date) 
VALUES (1, '张三', 'M', 20, 'Class A', 85.50, DATE '2024-01-15');
```

**预期结果**: 
```
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: xxxxxxxx
```

### 步骤 6: 验证 INSERT 数据

```sql
SELECT * FROM student_source WHERE id = 1;
```

**预期结果**: 能看到刚插入的 "张三" 数据。

### 步骤 7: 测试 UPSERT 更新

```sql
-- 再次插入相同主键的数据，会触发 UPSERT（更新）
INSERT INTO student_sink (id, name, gender, age, class_name, score, created_date) 
VALUES (1, '张三', 'M', 21, 'Class B', 90.00, DATE '2024-01-15');
```

**预期结果**: UPSERT 执行成功，数据被更新。

### 步骤 8: 验证 UPSERT 更新结果

```sql
SELECT * FROM student_source WHERE id = 1;
```

**预期结果**: 
- age 更新为 21
- class_name 更新为 'Class B'
- score 更新为 90.00

### 步骤 9: 测试批量写入

```sql
INSERT INTO student_sink (id, name, gender, age, class_name, score, created_date) 
VALUES 
    (2, '李四', 'F', 19, 'Class A', 88.00, DATE '2024-01-16'),
    (3, '王五', 'M', 20, 'Class C', 92.50, DATE '2024-01-17'),
    (4, '赵六', 'F', 21, 'Class B', 78.50, DATE '2024-01-18');
```

**预期结果**: 批量插入成功，Job ID 生成。

### 步骤 10: 验证批量写入结果

```sql
SELECT * FROM student_source ORDER BY id;
```

**预期结果**: 能看到所有插入的数据（共 4 条）。

### 步骤 11: 测试 Source 过滤查询

```sql
SELECT * FROM student_source WHERE age > 20;
```

**预期结果**: 返回 age > 20 的数据（张三和赵六）。

### 步骤 12: 测试 Source 聚合查询

```sql
SELECT class_name, COUNT(*) as cnt, AVG(score) as avg_score 
FROM student_source 
GROUP BY class_name;
```

**预期结果**: 按班级分组统计人数和平均分数。

## 测试结果

| 测试项 | 状态 | 备注 |
|--------|------|------|
| lib 目录部署 | ✅ 通过 | 重启 Flink 集群后 lib 目录 JAR 包被正确加载 |
| JDBC Source 表创建 | ✅ 通过 | 成功创建 Source 表，参数识别正常 |
| Source 数据读取 | ✅ 通过 | 成功读取 student 表数据 |
| JDBC Sink 表创建 | ✅ 通过 | 成功创建 Sink 表 |
| INSERT 单条写入 | ✅ 通过 | 成功插入单条数据 |
| UPSERT 更新 | ✅ 通过 | 主键冲突时自动更新数据 |
| 批量写入 | ✅ 通过 | 支持多条数据批量插入 |
| Source 过滤查询 | ✅ 通过 | 支持 WHERE 条件过滤 |
| Source 聚合查询 | ✅ 通过 | 支持 GROUP BY 聚合 |

## 测试结论

✅ **GaussDB JDBC Connector 功能验证通过**

- 支持 JDBC Source 读取 GaussDB 数据
- 支持 JDBC Sink 写入 GaussDB 数据
- 支持 UPSERT（INSERT ON CONFLICT UPDATE）语义
- 支持批量写入
- 支持标准 SQL 查询（过滤、聚合等）

## 完整参数列表

### Source 参数

| 参数名 | 是否必需 | 默认值 | 说明 |
|-------|---------|-------|------|
| `connector` | 是 | - | 固定值 `gaussdb` |
| `url` | 是 | - | JDBC URL，格式 `jdbc:gaussdb://host:port/db?compatibleMode=mysql` |
| `table-name` | 是 | - | GaussDB 表名 |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `driver` | 否 | - | JDBC 驱动类名 |
| `scan.fetch-size` | 否 | 0 | 每次从数据库获取的行数 |
| `scan.auto-commit` | 否 | true | 是否自动提交 |
| `scan.partition.column` | 否 | - | 分区列名（用于并行读取） |
| `scan.partition.num` | 否 | - | 分区数量 |
| `scan.partition.lower-bound` | 否 | - | 分区下界 |
| `scan.partition.upper-bound` | 否 | - | 分区上界 |

### Sink 参数

| 参数名 | 是否必需 | 默认值 | 说明 |
|-------|---------|-------|------|
| `connector` | 是 | - | 固定值 `gaussdb` |
| `url` | 是 | - | JDBC URL |
| `table-name` | 是 | - | GaussDB 表名 |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `sink.buffer-flush.max-rows` | 否 | 100 | 缓冲最大行数 |
| `sink.buffer-flush.interval` | 否 | 1s | 缓冲刷新间隔 |
| `sink.max-retries` | 否 | 3 | 写入失败最大重试次数 |
| `sink.parallelism` | 否 | - | Sink 并行度 |

---

## 已知限制

### 1. Flink 1.17 流模式限制
- **DELETE 不支持**: Flink 1.17 流模式 SQL 不支持 DELETE 操作
- **UPDATE 不支持**: Flink 1.17 流模式 SQL 不支持 UPDATE 操作
- **ORDER BY 限制**: 流模式下 `ORDER BY` 仅支持时间字段，非时间字段排序仅在批模式支持

> 这些是 Flink SQL 引擎的限制，不是 Connector 本身的问题

### 2. 部署限制
- **必须重启 Flink 集群**: 添加新的 JAR 包到 lib 目录后，必须重启 Flink 集群才能生效
- **lib 目录部署**: 不支持 `ADD JAR` 动态加载，必须放置到 `$FLINK_HOME/lib/`

### 3. 方言限制
- **必须使用 `gaussdb` 标识符**: 不能使用通用的 `jdbc` connector 名称
- **MySQL 兼容模式**: URL 中必须包含 `compatibleMode=mysql` 参数

## 附录

### 测试环境详细信息

```
GaussDB 表结构:
- id (int4) - 主键
- name (varchar)
- gender (varchar)
- age (int4)
- class_name (varchar)
- score (decimal)
- created_date (date)

连接参数:
- url: jdbc:gaussdb://1.92.120.69:8000/test?compatibleMode=mysql
- username: root
- table-name: student
```

### 常见问题

**Q: 为什么添加了 JAR 包到 lib 目录后还是找不到类？**
A: **必须重启 Flink 集群**。Flink 只在启动时扫描 lib 目录构建类路径，运行时添加的 JAR 包不会被加载。

**Q: 为什么使用 `gaussdb` 而不是 `jdbc` 作为 connector？**
A: 这是 GaussDB 专用的 JDBC Connector，提供了针对 GaussDB 的优化（如 UPSERT 语法适配）。

**Q: UPSERT 是如何实现的？**
A: 底层使用 GaussDB 的 `INSERT ... ON DUPLICATE KEY UPDATE` 语法实现。

**Q: 支持哪些数据类型？**
A: 支持 INT、VARCHAR、DECIMAL、DATE、TIMESTAMP 等常用类型。
