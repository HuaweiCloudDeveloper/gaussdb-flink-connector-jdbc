# GaussDB JDBC Connector 1.17 手动验证文档

## 环境信息

- **Flink 版本**: 1.17.2
- **GaussDB 版本**: 505.2.1.SPC0800
- **JDBC Connector**: flink-connector-jdbc-gaussdb-1.17-4.0-SNAPSHOT.jar
- **GaussDB 驱动**: gaussdbjdbc-JRE7.jar

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

## 已知限制

1. **Flink 1.17 流模式不支持 DELETE/UPDATE**: 这是 Flink SQL 引擎的限制，不是 Connector 的问题
2. **必须重启 Flink 集群**: 添加新的 JAR 包到 lib 目录后，必须重启 Flink 集群才能生效
3. **必须使用 GaussDB 方言**: Connector 标识符为 `gaussdb`，不是 `jdbc`

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
