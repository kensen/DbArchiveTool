# 修复记录：分区 SWITCH 索引不匹配错误

**日期**：2025-11-10  
**修复人员**：系统  
**严重程度**：高（阻止归档任务执行）

## 问题描述

在执行 BCP 归档任务时，使用分区优化方案（SWITCH 分区到临时表）时报错：

```
ALTER TABLE SWITCH 语句失败。表 'IMSTest.dbo.Tasks' 具有聚集索引 'PK_TASKS'，而表 'IMSTest.dbo.Tasks_Temp_20251110164532' 没有聚集索引。
```

## 根本原因

在 `PartitionSwitchHelper.CreateTempTableForSwitchAsync` 方法中，使用 `SELECT TOP 0 * INTO` 语法创建临时表时，**只会复制表的列定义，不会复制索引**。

而 SQL Server 的 `ALTER TABLE SWITCH PARTITION` 操作要求源表和目标表的**索引结构必须完全一致**，包括：
- 聚集索引
- 非聚集索引
- 主键约束
- 唯一约束
- 索引列的顺序和排序方向
- INCLUDE 列
- 筛选条件

## 修复方案

修改 `PartitionSwitchHelper.CreateTempTableForSwitchAsync` 方法，增加以下步骤：

1. **创建表结构**：使用 `SELECT TOP 0 * INTO` 复制列定义
2. **读取源表索引**：查询 `sys.indexes` 等系统视图获取所有索引定义
3. **在临时表上重建索引**：按照源表的索引定义，在临时表上创建相同的索引

### 关键实现细节

#### 1. 新增 `GetTableIndexesAsync` 方法
从系统视图中读取表的所有索引定义，包括：
- 索引 ID、名称、类型（聚集/非聚集）
- 是否唯一、是否主键、是否唯一约束
- 键列（带排序方向）
- INCLUDE 列
- 筛选条件

```csharp
private async Task<List<TempTableIndexDefinition>> GetTableIndexesAsync(
    string connectionString,
    string schemaName,
    string tableName,
    CancellationToken cancellationToken = default)
{
    // 查询 sys.indexes, sys.index_columns, sys.columns 等系统视图
    // 返回完整的索引定义列表
}
```

#### 2. 新增 `GenerateCreateIndexSqlForTempTable` 方法
根据索引定义生成创建索引的 SQL 语句：
- 主键约束：`ALTER TABLE ADD CONSTRAINT ... PRIMARY KEY`
- 唯一约束：`ALTER TABLE ADD CONSTRAINT ... UNIQUE`
- 普通索引：`CREATE [UNIQUE] [CLUSTERED|NONCLUSTERED] INDEX`
- 指定文件组：`ON [filegroup]`

```csharp
private string GenerateCreateIndexSqlForTempTable(
    string schemaName,
    string tableName,
    TempTableIndexDefinition index,
    string fileGroupName)
{
    // 根据索引类型生成相应的 CREATE 语句
    // 确保索引在相同的文件组上创建
}
```

#### 3. 新增 `TempTableIndexDefinition` 类
用于存储从系统视图读取的索引信息，包含所有必要的属性。

#### 4. 修改 `CreateTempTableForSwitchAsync` 流程
```csharp
public async Task<string> CreateTempTableForSwitchAsync(...)
{
    // 1. 复制表结构（仅列定义）
    var createTableSql = "SELECT TOP 0 * INTO ... FROM ...";
    await _sqlExecutor.ExecuteAsync(connectionString, createTableSql);

    // 2. 读取源表的所有索引定义
    var indexes = await GetTableIndexesAsync(...);

    // 3. 在临时表上重建所有索引（按 IndexId 排序，确保聚集索引先创建）
    foreach (var index in indexes.OrderBy(i => i.IndexId))
    {
        var createIndexSql = GenerateCreateIndexSqlForTempTable(...);
        await _sqlExecutor.ExecuteAsync(connectionString, createIndexSql);
    }

    return tempTableName;
}
```

### 错误处理

- 对于唯一索引/主键创建失败，记录警告并继续（可能是数据有重复）
- 对于普通索引创建失败，抛出异常并终止操作

## 修改文件

- **主要修改**：`src/DbArchiveTool.Infrastructure/Partitions/PartitionSwitchHelper.cs`
  - 修改 `CreateTempTableForSwitchAsync` 方法
  - 新增 `GetTableIndexesAsync` 方法
  - 新增 `GenerateCreateIndexSqlForTempTable` 方法
  - 新增 `TempTableIndexDefinition` 类

## 测试验证

### 验证步骤
1. 编译项目：`dotnet build` ✅ 成功
2. 针对有聚集索引的分区表执行归档任务
3. 验证临时表是否正确创建了所有索引
4. 验证 `ALTER TABLE SWITCH PARTITION` 操作是否成功

### 预期结果
- 临时表应包含与源表完全一致的索引结构
- `ALTER TABLE SWITCH` 操作应成功执行
- 不再出现"索引不匹配"错误

## 相关代码参考

项目中已有类似的索引复制逻辑可供参考：
- `PartitionSwitchAutoFixExecutor.ExecuteSyncIndexesAsync`：完整的索引同步实现
- `SqlPartitionCommandExecutor.ConvertToPartitionedTableAsync`：分区转换时的索引重建
- 逆向工程代码 `DBSqlHelper.CloneTableFullStructure`：旧工具的表结构复制实现

## 性能影响

- **创建临时表时间增加**：需要额外创建索引，耗时取决于索引数量和复杂度
- **优化**：索引按 IndexId 排序创建，确保聚集索引先创建（性能最优）
- **超时设置**：每个索引创建操作超时时间设为 300 秒

## 后续优化建议

1. **CHECK 约束支持**：当前未实现分区边界的 CHECK 约束创建（标记为 TODO）
2. **并发创建索引**：对于非聚集索引，可考虑并发创建以提升速度
3. **增量统计信息**：考虑是否需要复制统计信息（目前不复制）

## 相关文档

- `Sql Server 数据库分区最佳实践.md`：分区 SWITCH 操作的要求
- `BCP-BulkCopy技术设计.md`：BCP 归档的整体设计
- `BackgroundTask架构设计.md`：后台任务执行框架

## 总结

此修复确保了在使用分区优化方案进行归档时，临时表与源表具有完全一致的索引结构，从而满足 SQL Server 对 `ALTER TABLE SWITCH PARTITION` 操作的严格要求。这是执行分区切换操作的**必要前提条件**。
