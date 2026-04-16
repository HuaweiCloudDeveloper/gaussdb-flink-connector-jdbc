<p align="center">
  <h1 align="center">Flink GaussDB JDBC Connector</h1>
  <p align="center">专为 Flink 1.17 设计的 GaussDB 连接器，支持双模式 UPSERT 和高级数据写入功能</p>
</p>

## 目录

- [项目介绍](#项目介绍)
- [核心特性](#核心特性)
- [版本兼容性](#版本兼容性)
- [前置条件](#前置条件)
- [快速开始](#快速开始)
- [使用说明](#使用说明)
- [配置参数](#配置参数)
- [获取帮助](#获取帮助)
- [如何贡献](#如何贡献)

## 项目介绍

[Flink GaussDB JDBC Connector](https://github.com/HuaweiCloudDeveloper/gaussdb-flink-connector-jdbc) 是一个开源的 GaussDB 连接器，专为 Flink 1.17 版本设计，支持将数据写入 GaussDB（sink）以及从 GaussDB 读取数据（source）。

本项目包含两个模块：
- **flink-connector-jdbc-gaussdb**: 基于 Flink 2.0+ JDBC Connector 的 GaussDB 实现
- **flink-connector-jdbc-gaussdb-1.17**: 专为 Flink 1.17 适配的 GaussDB 连接器（推荐 Flink 1.17 用户使用）

## 核心特性

### 1. 双模式 UPSERT 支持
- **MySQL 兼容模式**: `ON DUPLICATE KEY UPDATE` - 适用于 GaussDB B 兼容模式
- **PostgreSQL 原生模式**: `ON CONFLICT ... DO UPDATE` - 适用于 GaussDB 原生模式
- 通过 URL 参数 `compatibleMode=mysql` 自动切换

### 2. 高级写入功能
- **Insert**: 新增增量数据
- **Update**: 更新存量数据（通过 UPSERT 实现）
- **Delete**: 删除存量数据
- **Upsert**: 根据主键自动判断插入或更新
- **ignoreNullWhenUpdate**: 更新时忽略 NULL 值，保留原数据

### 3. Catalog 支持
- 支持 Flink SQL Catalog 功能
- 自动发现 GaussDB 表结构
- 支持 Schema 管理

### 4. 数据源支持
- 支持从 GaussDB 读取数据（Source）
- 支持 Filter 下推优化
- 支持 Projection 下推优化

## 版本兼容性

| 连接器版本 | Flink 版本 | GaussDB 版本 | 状态 |
|-----------|-----------|-------------|------|
| flink-connector-jdbc-gaussdb-1.17 | 1.17.x | 506.0.0+ | ✅ 推荐 |
| flink-connector-jdbc-gaussdb | 2.0+ | 506.0.0+ | ✅ 支持 |

**注意**: Flink 1.17 用户请使用 `flink-connector-jdbc-gaussdb-1.17` 模块。

## 前置条件

### 系统要求
- **CPU**: 2GHz 或更高
- **RAM**: 4GB 或更大
- **Disk**: 至少 40GB
- **JDK**: 8/11/17

### 依赖 JAR 包
将以下 JAR 包放置到 Flink 安装目录的 `lib/` 文件夹下：

1. **GaussDB JDBC 驱动** (必选)
   - JDK 8/11: `gaussdbjdbc-506.0.0.b058-jdk7.jar`
   - JDK 17+: `gaussdbjdbc-506.0.0.b058.jar`

2. **Flink JDBC Connector** (必选)
   - Flink 1.17: `flink-connector-jdbc-3.1.2-1.17.jar`

3. **GaussDB Connector** (必选)
   - `flink-connector-jdbc-gaussdb-1.17-4.0-SNAPSHOT.jar`

### Maven 依赖

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb.flink</groupId>
    <artifactId>flink-connector-jdbc-gaussdb-1.17</artifactId>
    <version>4.0-SNAPSHOT</version>
</dependency>
```

## 快速开始

### 1. SQL 方式使用 GaussDB Connector

```sql
-- 创建 GaussDB Sink 表
CREATE TABLE gaussdb_sink (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb',
    'url' = 'jdbc:gaussdb://localhost:8000/postgres?compatibleMode=mysql',
    'table-name' = 'users',
    'username' = 'gaussdb',
    'password' = 'password',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver',
    'sink.ignore-null-when-update' = 'true'
);

-- 插入数据
INSERT INTO gaussdb_sink VALUES (1, 'Alice', 20), (2, 'Bob', 25);
```

### 2. UPSERT 示例

```sql
-- 首次插入
INSERT INTO gaussdb_sink VALUES (1, 'Alice', 20);

-- 更新（主键存在则更新，不存在则插入）
INSERT INTO gaussdb_sink VALUES (1, 'Alice Updated', 21), (3, 'Charlie', 30);
```

### 3. 使用 Catalog

```sql
-- 创建 GaussDB Catalog
CREATE CATALOG gaussdb_catalog WITH (
    'type' = 'jdbc',
    'base-url' = 'jdbc:gaussdb://localhost:8000',
    'default-database' = 'postgres',
    'username' = 'gaussdb',
    'password' = 'password'
);

-- 使用 Catalog
USE CATALOG gaussdb_catalog;
SHOW TABLES;
```

## 使用说明

### 连接器类型选择

| 场景 | Connector 类型 | 说明 |
|------|---------------|------|
| Sink (写入) | `gaussdb` | 使用 GaussDB 专用 Sink，支持 ignoreNullWhenUpdate |
| Source (读取) | `jdbc` | 使用标准 JDBC Source |

### UPSERT 语法模式选择

| GaussDB 模式 | URL 参数 | UPSERT 语法 |
|-------------|---------|------------|
| B 兼容模式 (MySQL) | `compatibleMode=mysql` | `ON DUPLICATE KEY UPDATE` |
| 原生模式 | 无 | `ON CONFLICT ... DO UPDATE` |

## 配置参数

### 通用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| connector | 是 | - | 连接器类型：`gaussdb` 或 `jdbc` |
| url | 是 | - | JDBC URL |
| table-name | 是 | - | 表名 |
| username | 是 | - | 用户名 |
| password | 是 | - | 密码 |
| driver | 否 | com.huawei.gaussdb.jdbc.Driver | 驱动类名 |

### Sink 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| sink.ignore-null-when-update | 否 | false | 更新时忽略 NULL 值 |
| sink.buffer-flush.max-rows | 否 | 100 | 缓冲最大行数 |
| sink.buffer-flush.interval | 否 | 1s | 缓冲刷新间隔 |
| sink.max-retries | 否 | 3 | 最大重试次数 |

### Source 专用参数

| 参数 | 必填 | 默认值 | 说明 |
|-----|------|-------|------|
| scan.fetch-size | 否 | 0 | 每次读取行数 |
| scan.partition.column | 否 | - | 分区列名 |
| scan.partition.num | 否 | - | 分区数量 |

## 注意事项

1. **驱动版本选择**: 根据 JDK 版本选择对应的 GaussDB 驱动，否则可能导致任务提交失败
2. **类加载器配置**: 如遇到 `ClassNotFoundException`，请在 `flink-conf.yaml` 中设置 `classloader.resolve-order: parent-first`
3. **连接器版本匹配**: flink-connector-jdbc 和 flink-connector-jdbc-gaussdb 版本需要与 Flink 版本匹配
4. **字符编码**: GaussDB B 模式建议使用 UTF-8 编码，URL 中添加 `characterEncoding=UTF-8`

## 获取帮助

- **GitHub Issues**: [提交问题](https://github.com/HuaweiCloudDeveloper/gaussdb-flink-connector-jdbc/issues)
- **文档**: [Wiki](https://github.com/HuaweiCloudDeveloper/gaussdb-flink-connector-jdbc/wiki)

## 如何贡献

1. Fork 此存储库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 许可证

本项目基于 Apache License 2.0 开源许可证。
