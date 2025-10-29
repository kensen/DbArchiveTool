-- 索引重建功能测试脚本
-- 用于验证分区转换时是否正确保存和重建所有索引
-- 修复后验证：所有索引都应该对齐到分区方案（无论是否包含分区列）

USE [YourDatabaseName]; -- 替换为实际数据库名
GO

-- ============================================================
-- 测试场景：创建一个包含多种索引类型的测试表（20万条数据）
-- ============================================================
IF OBJECT_ID('dbo.IndexTestTable2', 'U') IS NOT NULL
    DROP TABLE dbo.IndexTestTable2;
GO

PRINT '正在创建测试表 IndexTestTable2...';
GO

CREATE TABLE dbo.IndexTestTable2 (
    -- 主键列（自增ID）
    OrderId INT IDENTITY(1,1) NOT NULL,
    -- 普通列
    CustomerId INT NOT NULL,
    OrderDate DATETIME NOT NULL,
    Amount DECIMAL(18,2),
    Status NVARCHAR(50),
    ProductId INT,
    Quantity INT,
    Discount DECIMAL(5,2),
    Notes NVARCHAR(MAX),
    CreatedAt DATETIME DEFAULT GETDATE(),
    
    -- 主键约束（聚集索引）
    CONSTRAINT PK_IndexTestTable2 PRIMARY KEY CLUSTERED (OrderId)
);
GO

PRINT '正在创建索引...';
GO

-- 创建非聚集索引 1：普通索引（不包含分区列 OrderDate）
CREATE NONCLUSTERED INDEX IX_IndexTestTable2_CustomerId 
ON dbo.IndexTestTable2(CustomerId);
GO

-- 创建非聚集索引 2：唯一索引（包含分区列 OrderDate）
CREATE UNIQUE NONCLUSTERED INDEX IX_IndexTestTable2_OrderDate_CustomerId
ON dbo.IndexTestTable2(OrderDate DESC, CustomerId ASC);
GO

-- 创建非聚集索引 3：包含列索引（不包含分区列，但在 INCLUDE 中有）
CREATE NONCLUSTERED INDEX IX_IndexTestTable2_Status
ON dbo.IndexTestTable2(Status)
INCLUDE (Amount, OrderDate, CustomerId);
GO

-- 创建非聚集索引 4：筛选索引（包含分区列 OrderDate）
CREATE NONCLUSTERED INDEX IX_IndexTestTable2_ActiveOrders
ON dbo.IndexTestTable2(OrderDate DESC)
WHERE Status = 'Active';
GO

-- 创建非聚集索引 5：复合索引（不包含分区列）
CREATE NONCLUSTERED INDEX IX_IndexTestTable2_Product_Quantity
ON dbo.IndexTestTable2(ProductId, Quantity DESC)
INCLUDE (Amount, Discount);
GO

PRINT '正在插入 200,000 条测试数据（预计耗时 30-60 秒）...';
GO

-- 插入 200,000 条测试数据
SET NOCOUNT ON;

WITH Numbers AS (
    SELECT TOP (200000)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.IndexTestTable2 (CustomerId, OrderDate, Amount, Status, ProductId, Quantity, Discount, Notes)
SELECT
    100 + (RowNum % 1000) AS CustomerId,
    DATEADD(DAY, (RowNum % 365), DATEADD(HOUR, (RowNum % 24), '2023-01-01')) AS OrderDate,
    CAST(ROUND(50 + (RowNum % 5000) * 0.13, 2) AS DECIMAL(18,2)) AS Amount,
    CASE RowNum % 4
        WHEN 0 THEN 'Active'
        WHEN 1 THEN 'Completed'
        WHEN 2 THEN 'Pending'
        ELSE 'Archived'
    END AS Status,
    1000 + (RowNum % 500) AS ProductId,
    1 + (RowNum % 50) AS Quantity,
    CAST(((RowNum % 10) * 0.05) AS DECIMAL(5,2)) AS Discount,
    CONCAT(N'测试订单 #', RowNum) AS Notes
FROM Numbers;

SET NOCOUNT OFF;
GO

PRINT '';
PRINT '✓ 测试表创建完成！';
PRINT '----------------------------------------';
SELECT 
    OBJECT_NAME(object_id) AS TableName,
    SUM(row_count) AS TotalRows,
    SUM(reserved_page_count) * 8 / 1024 AS ReservedMB,
    SUM(used_page_count) * 8 / 1024 AS UsedMB
FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('dbo.IndexTestTable2')
GROUP BY object_id;
GO

-- ============================================================
-- 查询：转换前的索引状态
-- ============================================================
PRINT '';
PRINT '===== 转换前的索引列表 =====';
SELECT 
    i.index_id,
    i.name AS index_name,
    i.type_desc AS index_type,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    kc.name AS constraint_name,
    -- 索引列
    STUFF((SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
           FROM sys.index_columns ic
           INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
           ORDER BY ic.key_ordinal
           FOR XML PATH('')), 1, 2, '') AS key_columns,
    -- INCLUDE 列
    STUFF((SELECT ', ' + QUOTENAME(c.name)
           FROM sys.index_columns ic
           INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
           FOR XML PATH('')), 1, 2, '') AS included_columns,
    -- 筛选条件
    i.filter_definition,
    -- 分区信息
    ps.name AS partition_scheme,
    pf.name AS partition_function,
    -- 是否包含 OrderDate 列
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id 
          AND ic.index_id = i.index_id 
          AND c.name = 'OrderDate'
          AND ic.is_included_column = 0
    ) THEN '是' ELSE '否' END AS 包含分区列OrderDate
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
LEFT JOIN sys.key_constraints kc ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
LEFT JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
LEFT JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
WHERE t.name = 'IndexTestTable2'
  AND SCHEMA_NAME(t.schema_id) = 'dbo'
  AND i.type IN (1, 2)
ORDER BY i.index_id;
GO

-- ============================================================
-- 验证点
-- ============================================================
PRINT '';
PRINT '========================================';
PRINT '验证点（转换前）';
PRINT '========================================';
PRINT '1. 应该有 6 个索引：';
PRINT '   - PK_IndexTestTable2 (主键聚集索引)';
PRINT '   - IX_IndexTestTable2_CustomerId (不包含 OrderDate)';
PRINT '   - IX_IndexTestTable2_OrderDate_CustomerId (包含 OrderDate)';
PRINT '   - IX_IndexTestTable2_Status (不包含 OrderDate，INCLUDE 中有)';
PRINT '   - IX_IndexTestTable2_ActiveOrders (包含 OrderDate)';
PRINT '   - IX_IndexTestTable2_Product_Quantity (不包含 OrderDate)';
PRINT '';
PRINT '2. 所有索引的 partition_scheme 应该为 NULL';
PRINT '';
PRINT '========================================';
PRINT '期望结果（转换后 - 修复后的行为）';
PRINT '========================================';
PRINT '✅ 所有 6 个索引都应该对齐到分区方案';
PRINT '✅ 所有索引的 partition_scheme 都应该相同';
PRINT '✅ 不包含 OrderDate 的索引会自动补齐该列';
PRINT '✅ SWITCH PARTITION 操作可以成功执行';
PRINT '';
PRINT '❌ 修复前的错误行为（已修复）：';
PRINT '   - 不包含 OrderDate 的索引被放在 PRIMARY 文件组';
PRINT '   - 导致 SWITCH PARTITION 失败';
PRINT '';
PRINT '========================================';
PRINT '测试步骤';
PRINT '========================================';
PRINT '1. 通过 "分区执行向导" 转换此表为分区表';
PRINT '   - 分区列: OrderDate';
PRINT '   - 边界值: 按月或按年划分';
PRINT '';
PRINT '2. 转换完成后，执行以下查询验证：';
PRINT '   USE [YourDatabaseName];';
PRINT '   EXEC sp_executesql N''';
PRINT '   SELECT i.name AS 索引名, ds.type_desc AS 数据空间类型, ps.name AS 分区方案';
PRINT '   FROM sys.indexes i';
PRINT '   INNER JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id';
PRINT '   LEFT JOIN sys.partition_schemes ps ON ds.data_space_id = ps.data_space_id';
PRINT '   WHERE i.object_id = OBJECT_ID(''''dbo.IndexTestTable2'''')';
PRINT '     AND i.type IN (1, 2);'';';
PRINT '';
PRINT '3. 验证结果：';
PRINT '   - 所有索引的 数据空间类型 都应该是 PARTITION_SCHEME';
PRINT '   - 所有索引的 分区方案 名称都应该相同';
PRINT '';
GO

-- ============================================================
-- 清理测试数据（可选，取消注释以清理）
-- ============================================================
-- PRINT '正在清理测试表...';
-- DROP TABLE IF EXISTS dbo.IndexTestTable2;
-- GO
-- PRINT '✓ 测试表已清理';
-- GO
