-- 诊断备份表缺失索引的问题
-- 比较源表和备份表的索引差异

USE [YourDatabaseName]; -- 替换为实际数据库名
GO

PRINT '========================================';
PRINT '源表索引列表 (dbo.IndexTestTable2)';
PRINT '========================================';

SELECT 
    i.index_id,
    i.name AS 索引名,
    i.type_desc AS 索引类型,
    CASE WHEN i.is_unique = 1 THEN '是' ELSE '否' END AS 是否唯一,
    CASE WHEN i.is_primary_key = 1 THEN '是' ELSE '否' END AS 是否主键,
    -- 键列
    STUFF((SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
           FROM sys.index_columns ic
           INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
           ORDER BY ic.key_ordinal
           FOR XML PATH('')), 1, 2, '') AS 键列,
    -- INCLUDE列
    STUFF((SELECT ', ' + QUOTENAME(c.name)
           FROM sys.index_columns ic
           INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
           FOR XML PATH('')), 1, 2, '') AS INCLUDE列
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID('dbo.IndexTestTable2')
  AND i.type IN (1, 2)
ORDER BY i.index_id;

PRINT '';
PRINT '========================================';
PRINT '备份表索引列表 (dbo.IndexTestTable2_bak)';
PRINT '========================================';

SELECT 
    i.index_id,
    i.name AS 索引名,
    i.type_desc AS 索引类型,
    CASE WHEN i.is_unique = 1 THEN '是' ELSE '否' END AS 是否唯一,
    CASE WHEN i.is_primary_key = 1 THEN '是' ELSE '否' END AS 是否主键,
    -- 键列
    STUFF((SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
           FROM sys.index_columns ic
           INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
           ORDER BY ic.key_ordinal
           FOR XML PATH('')), 1, 2, '') AS 键列,
    -- INCLUDE列
    STUFF((SELECT ', ' + QUOTENAME(c.name)
           FROM sys.index_columns ic
           INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
           FOR XML PATH('')), 1, 2, '') AS INCLUDE列
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID('dbo.IndexTestTable2_bak')
  AND i.type IN (1, 2)
ORDER BY i.index_id;

PRINT '';
PRINT '========================================';
PRINT '索引差异分析';
PRINT '========================================';

-- 源表有但备份表没有的索引
SELECT 
    '源表独有' AS 差异类型,
    src.name AS 索引名,
    src.type_desc AS 索引类型,
    CASE WHEN src.is_unique = 1 THEN '是' ELSE '否' END AS 是否唯一
FROM sys.indexes src
WHERE src.object_id = OBJECT_ID('dbo.IndexTestTable2')
  AND src.type IN (1, 2)
  AND NOT EXISTS (
      SELECT 1 FROM sys.indexes bak
      WHERE bak.object_id = OBJECT_ID('dbo.IndexTestTable2_bak')
        AND bak.name LIKE src.name + '%'
  )

UNION ALL

-- 备份表有但源表没有的索引
SELECT 
    '备份表独有' AS 差异类型,
    bak.name AS 索引名,
    bak.type_desc AS 索引类型,
    CASE WHEN bak.is_unique = 1 THEN '是' ELSE '否' END AS 是否唯一
FROM sys.indexes bak
WHERE bak.object_id = OBJECT_ID('dbo.IndexTestTable2_bak')
  AND bak.type IN (1, 2)
  AND NOT EXISTS (
      SELECT 1 FROM sys.indexes src
      WHERE src.object_id = OBJECT_ID('dbo.IndexTestTable2')
        AND bak.name LIKE src.name + '%'
  );

PRINT '';
PRINT '========================================';
PRINT '唯一索引数据验证';
PRINT '========================================';
PRINT '检查备份表中 (OrderDate, CustomerId) 组合是否存在重复值...';
PRINT '';

-- 检查备份表中是否有重复的 (OrderDate, CustomerId) 组合
SELECT 
    OrderDate,
    CustomerId,
    COUNT(*) AS 重复次数
FROM dbo.IndexTestTable2_bak
GROUP BY OrderDate, CustomerId
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- 如果没有重复
IF NOT EXISTS (
    SELECT 1 FROM dbo.IndexTestTable2_bak
    GROUP BY OrderDate, CustomerId
    HAVING COUNT(*) > 1
)
BEGIN
    PRINT '✓ 备份表中没有重复的 (OrderDate, CustomerId) 组合';
    PRINT '  理论上可以创建唯一索引';
END
ELSE
BEGIN
    PRINT '✗ 备份表中存在重复的 (OrderDate, CustomerId) 组合';
    PRINT '  无法创建唯一索引';
END

PRINT '';
PRINT '========================================';
PRINT '问题总结';
PRINT '========================================';
PRINT '如果缺少唯一索引 IX_IndexTestTable2_OrderDate_CustomerId,';
PRINT '可能的原因:';
PRINT '1. 补齐分区列后,数据存在唯一性冲突';
PRINT '2. 创建索引时SQL执行失败,但错误被忽略';
PRINT '3. 唯一索引的创建被跳过(代码逻辑问题)';
GO
