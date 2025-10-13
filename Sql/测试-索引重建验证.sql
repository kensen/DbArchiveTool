-- 索引重建功能测试脚本
-- 用于验证分区转换时是否正确保存和重建所有索引

USE [YourDatabaseName]; -- 替换为实际数据库名
GO

-- ============================================================
-- 测试场景 1: 创建一个包含多种索引类型的测试表
-- ============================================================
IF OBJECT_ID('dbo.IndexTestTable', 'U') IS NOT NULL
    DROP TABLE dbo.IndexTestTable;
GO

CREATE TABLE dbo.IndexTestTable (
    -- 主键列
    OrderId INT NOT NULL,
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
    CONSTRAINT PK_IndexTestTable PRIMARY KEY CLUSTERED (OrderId)
);
GO

-- 创建非聚集索引 1：普通索引
CREATE NONCLUSTERED INDEX IX_IndexTestTable_CustomerId 
ON dbo.IndexTestTable(CustomerId);
GO

-- 创建非聚集索引 2：唯一索引
CREATE UNIQUE NONCLUSTERED INDEX IX_IndexTestTable_OrderDate_CustomerId
ON dbo.IndexTestTable(OrderDate DESC, CustomerId ASC);
GO

-- 创建非聚集索引 3：包含列索引
CREATE NONCLUSTERED INDEX IX_IndexTestTable_Status
ON dbo.IndexTestTable(Status)
INCLUDE (Amount, OrderDate, CustomerId);
GO

-- 创建非聚集索引 4：筛选索引
CREATE NONCLUSTERED INDEX IX_IndexTestTable_ActiveOrders
ON dbo.IndexTestTable(OrderDate DESC)
WHERE Status = 'Active';
GO

-- 创建非聚集索引 5：复合索引
CREATE NONCLUSTERED INDEX IX_IndexTestTable_Product_Quantity
ON dbo.IndexTestTable(ProductId, Quantity DESC)
INCLUDE (Amount, Discount);
GO

-- 插入测试数据
INSERT INTO dbo.IndexTestTable (OrderId, CustomerId, OrderDate, Amount, Status, ProductId, Quantity, Discount)
VALUES 
    (1, 101, '2025-01-01', 100.00, 'Active', 1001, 5, 0.1),
    (2, 102, '2025-01-02', 200.00, 'Completed', 1002, 3, 0.05),
    (3, 103, '2025-01-03', 150.00, 'Active', 1003, 2, 0.15),
    (4, 104, '2025-01-04', 300.00, 'Pending', 1001, 10, 0.2),
    (5, 105, '2025-01-05', 250.00, 'Active', 1004, 7, 0.1);
GO

-- ============================================================
-- 查询：转换前的索引状态
-- ============================================================
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
    pf.name AS partition_function
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
LEFT JOIN sys.key_constraints kc ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
LEFT JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
LEFT JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
WHERE t.name = 'IndexTestTable'
  AND SCHEMA_NAME(t.schema_id) = 'dbo'
  AND i.type IN (1, 2)
ORDER BY i.index_id;
GO

-- ============================================================
-- 验证点：
-- 1. 应该有 6 个索引（1 个聚集主键 + 5 个非聚集索引）
-- 2. 所有索引的 partition_scheme 应该为 NULL（转换前）
-- ============================================================

PRINT '';
PRINT '===== 期望结果 =====';
PRINT '转换前应该有 6 个索引：';
PRINT '1. PK_IndexTestTable - 主键聚集索引';
PRINT '2. IX_IndexTestTable_CustomerId - 普通非聚集索引';
PRINT '3. IX_IndexTestTable_OrderDate_CustomerId - 唯一非聚集索引';
PRINT '4. IX_IndexTestTable_Status - 包含列非聚集索引';
PRINT '5. IX_IndexTestTable_ActiveOrders - 筛选非聚集索引';
PRINT '6. IX_IndexTestTable_Product_Quantity - 复合包含列非聚集索引';
PRINT '';
PRINT '转换后（执行分区转换后）：';
PRINT '- 所有索引应该保留';
PRINT '- 聚集索引应该在分区方案上';
PRINT '- 包含 OrderDate 列的非聚集索引应该对齐到分区方案';
PRINT '- 不包含 OrderDate 列的非聚集索引应该在 PRIMARY 文件组（非对齐）';
GO

-- ============================================================
-- 清理测试数据（可选）
-- ============================================================
-- DROP TABLE IF EXISTS dbo.IndexTestTable;
-- GO
