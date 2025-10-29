-- =============================================
-- 诊断索引分区对齐情况
-- =============================================
-- 用途: 检查指定表的所有索引是否对齐到分区方案
-- 用法: 修改 @TableName 参数后执行
-- =============================================

DECLARE @TableName NVARCHAR(128) = N'IndexTestTable'; -- 修改为要检查的表名

SELECT 
    t.name AS 表名,
    i.name AS 索引名,
    i.type_desc AS 索引类型,
    CASE 
        WHEN i.is_primary_key = 1 THEN 'PRIMARY KEY'
        WHEN i.is_unique_constraint = 1 THEN 'UNIQUE CONSTRAINT'
        WHEN i.is_unique = 1 THEN 'UNIQUE INDEX'
        ELSE '普通索引'
    END AS 约束类型,
    ds.type_desc AS 数据空间类型,
    ds.name AS 数据空间名称,
    ps.name AS 分区方案名称,
    c.name AS 分区列名称,
    CASE 
        WHEN ds.type_desc = 'PARTITION_SCHEME' THEN '✓ 已对齐'
        WHEN ds.type_desc = 'ROWS_FILEGROUP' THEN '✗ 未对齐 (使用文件组)'
        ELSE '✗ 未对齐'
    END AS 对齐状态
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
LEFT JOIN sys.partition_schemes ps ON ds.data_space_id = ps.data_space_id
LEFT JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.partition_ordinal = 1
LEFT JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE t.name = @TableName
    AND i.type IN (1, 2) -- 1=聚集索引, 2=非聚集索引
ORDER BY 
    CASE i.is_primary_key WHEN 1 THEN 1 ELSE 2 END, -- 主键排在最前
    i.type, -- 聚集索引其次
    i.name;

-- =============================================
-- 统计结果
-- =============================================
DECLARE @TotalIndexes INT, @AlignedIndexes INT, @UnalignedIndexes INT;

SELECT 
    @TotalIndexes = COUNT(*),
    @AlignedIndexes = SUM(CASE WHEN ds.type_desc = 'PARTITION_SCHEME' THEN 1 ELSE 0 END),
    @UnalignedIndexes = SUM(CASE WHEN ds.type_desc <> 'PARTITION_SCHEME' THEN 1 ELSE 0 END)
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
WHERE t.name = @TableName
    AND i.type IN (1, 2);

PRINT N'======================================';
PRINT N'索引对齐诊断结果';
PRINT N'======================================';
PRINT N'表名: ' + @TableName;
PRINT N'总索引数: ' + CAST(@TotalIndexes AS NVARCHAR(10));
PRINT N'已对齐: ' + CAST(@AlignedIndexes AS NVARCHAR(10));
PRINT N'未对齐: ' + CAST(@UnalignedIndexes AS NVARCHAR(10));
PRINT N'======================================';

IF @UnalignedIndexes > 0
BEGIN
    PRINT N'⚠ 警告: 发现未对齐的索引!';
    PRINT N'这会导致 SWITCH PARTITION 操作失败。';
    PRINT N'请使用归档任务的"自动修复"功能进行对齐。';
END
ELSE
BEGIN
    PRINT N'✓ 所有索引均已对齐到分区方案。';
END
