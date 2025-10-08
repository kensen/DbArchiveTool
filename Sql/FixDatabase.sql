-- =============================================
-- 数据库修复脚本
-- 用途: 修复现有 DbArchiveTool 数据库，使其与 EF 迁移定义完全匹配
-- 版本: 1.0
-- 日期: 2025-10-08
-- =============================================

USE DbArchiveTool;
GO

PRINT '开始执行数据库修复脚本...';
GO

-- =============================================
-- 1. 检查并修复 ArchiveDataSource 表的默认值
-- =============================================
PRINT '检查 ArchiveDataSource 表的默认值约束...';

-- 1.1 修复 UseSourceAsTarget 默认值（应为 1）
DECLARE @constraintName NVARCHAR(200);
DECLARE @sql NVARCHAR(MAX);

-- 删除旧的 UseSourceAsTarget 约束（如果存在）
SELECT @constraintName = dc.name 
FROM sys.default_constraints dc 
JOIN sys.columns c ON c.default_object_id = dc.object_id
WHERE c.object_id = OBJECT_ID('ArchiveDataSource') 
  AND c.name = 'UseSourceAsTarget';

IF @constraintName IS NOT NULL
BEGIN
    SET @sql = 'ALTER TABLE ArchiveDataSource DROP CONSTRAINT ' + QUOTENAME(@constraintName);
    EXEC sp_executesql @sql;
    PRINT '  ✓ 已删除旧的 UseSourceAsTarget 约束';
END

-- 添加正确的默认值约束
IF NOT EXISTS (
    SELECT 1 FROM sys.default_constraints dc 
    JOIN sys.columns c ON c.default_object_id = dc.object_id
    WHERE c.object_id = OBJECT_ID('ArchiveDataSource') 
      AND c.name = 'UseSourceAsTarget'
)
BEGIN
    ALTER TABLE ArchiveDataSource 
    ADD CONSTRAINT DF_ArchiveDataSource_UseSourceAsTarget DEFAULT 1 FOR UseSourceAsTarget;
    PRINT '  ✓ 已添加 UseSourceAsTarget 默认值约束 (DEFAULT 1)';
END
ELSE
    PRINT '  ✓ UseSourceAsTarget 默认值约束已存在';

-- 1.2 添加 TargetServerPort 默认值（应为 0）
IF NOT EXISTS (
    SELECT 1 FROM sys.default_constraints dc 
    JOIN sys.columns c ON c.default_object_id = dc.object_id
    WHERE c.object_id = OBJECT_ID('ArchiveDataSource') 
      AND c.name = 'TargetServerPort'
)
BEGIN
    ALTER TABLE ArchiveDataSource 
    ADD CONSTRAINT DF_ArchiveDataSource_TargetServerPort DEFAULT 0 FOR TargetServerPort;
    PRINT '  ✓ 已添加 TargetServerPort 默认值约束 (DEFAULT 0)';
END
ELSE
    PRINT '  ✓ TargetServerPort 默认值约束已存在';

-- 1.3 确保 TargetUseIntegratedSecurity 默认值为 0
IF NOT EXISTS (
    SELECT 1 FROM sys.default_constraints dc 
    JOIN sys.columns c ON c.default_object_id = dc.object_id
    WHERE c.object_id = OBJECT_ID('ArchiveDataSource') 
      AND c.name = 'TargetUseIntegratedSecurity'
)
BEGIN
    ALTER TABLE ArchiveDataSource 
    ADD CONSTRAINT DF_ArchiveDataSource_TargetUseIntegratedSecurity DEFAULT 0 FOR TargetUseIntegratedSecurity;
    PRINT '  ✓ 已添加 TargetUseIntegratedSecurity 默认值约束 (DEFAULT 0)';
END
ELSE
    PRINT '  ✓ TargetUseIntegratedSecurity 默认值约束已存在';

GO

-- =============================================
-- 2. 修复现有数据的 NULL 值
-- =============================================
PRINT '检查并修复现有数据的 NULL 值...';

-- 2.1 修复 TargetServerPort 的 NULL 值
UPDATE ArchiveDataSource 
SET TargetServerPort = 1433 
WHERE TargetServerPort IS NULL;

IF @@ROWCOUNT > 0
    PRINT '  ✓ 已更新 ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' 行的 TargetServerPort 为 1433';
ELSE
    PRINT '  ✓ TargetServerPort 无 NULL 值';

-- 2.2 确保 TargetServerPort 列为 NOT NULL
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'ArchiveDataSource' 
      AND COLUMN_NAME = 'TargetServerPort' 
      AND IS_NULLABLE = 'YES'
)
BEGIN
    ALTER TABLE ArchiveDataSource ALTER COLUMN TargetServerPort INT NOT NULL;
    PRINT '  ✓ 已将 TargetServerPort 设置为 NOT NULL';
END
ELSE
    PRINT '  ✓ TargetServerPort 已是 NOT NULL';

-- 2.3 确保 UseSourceAsTarget 为有效布尔值
UPDATE ArchiveDataSource 
SET UseSourceAsTarget = 1 
WHERE UseSourceAsTarget IS NULL OR UseSourceAsTarget NOT IN (0, 1);

IF @@ROWCOUNT > 0
    PRINT '  ✓ 已修复 ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' 行的 UseSourceAsTarget 值';
ELSE
    PRINT '  ✓ UseSourceAsTarget 值都有效';

GO

-- =============================================
-- 3. 验证表结构
-- =============================================
PRINT '验证表结构...';

-- 3.1 检查必需的列是否存在
DECLARE @missingColumns TABLE (ColumnName NVARCHAR(128));

INSERT INTO @missingColumns (ColumnName)
SELECT name FROM (
    VALUES 
        ('UseSourceAsTarget'),
        ('TargetServerAddress'),
        ('TargetServerPort'),
        ('TargetDatabaseName'),
        ('TargetUseIntegratedSecurity'),
        ('TargetUserName'),
        ('TargetPassword')
) AS RequiredColumns(name)
WHERE name NOT IN (
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'ArchiveDataSource'
);

IF EXISTS (SELECT 1 FROM @missingColumns)
BEGIN
    PRINT '  ⚠ 警告：缺少以下列：';
    SELECT '    - ' + ColumnName FROM @missingColumns;
    PRINT '  请手动运行完整迁移脚本';
END
ELSE
    PRINT '  ✓ 所有必需列都存在';

GO

-- 3.2 检查列的数据类型和长度
PRINT '检查列的数据类型和长度...';

DECLARE @typeIssues TABLE (ColumnName NVARCHAR(128), Issue NVARCHAR(500));

-- TargetServerAddress 应为 nvarchar(128)
INSERT INTO @typeIssues
SELECT 'TargetServerAddress', 
       'Expected: nvarchar(128), Actual: ' + DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(10)) + ')'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ArchiveDataSource' 
  AND COLUMN_NAME = 'TargetServerAddress'
  AND (DATA_TYPE != 'nvarchar' OR CHARACTER_MAXIMUM_LENGTH != 128);

-- TargetDatabaseName 应为 nvarchar(128)
INSERT INTO @typeIssues
SELECT 'TargetDatabaseName', 
       'Expected: nvarchar(128), Actual: ' + DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(10)) + ')'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ArchiveDataSource' 
  AND COLUMN_NAME = 'TargetDatabaseName'
  AND (DATA_TYPE != 'nvarchar' OR CHARACTER_MAXIMUM_LENGTH != 128);

-- TargetUserName 应为 nvarchar(64)
INSERT INTO @typeIssues
SELECT 'TargetUserName', 
       'Expected: nvarchar(64), Actual: ' + DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(10)) + ')'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ArchiveDataSource' 
  AND COLUMN_NAME = 'TargetUserName'
  AND (DATA_TYPE != 'nvarchar' OR CHARACTER_MAXIMUM_LENGTH != 64);

-- TargetPassword 应为 nvarchar(256)
INSERT INTO @typeIssues
SELECT 'TargetPassword', 
       'Expected: nvarchar(256), Actual: ' + DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(10)) + ')'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ArchiveDataSource' 
  AND COLUMN_NAME = 'TargetPassword'
  AND (DATA_TYPE != 'nvarchar' OR CHARACTER_MAXIMUM_LENGTH != 256);

IF EXISTS (SELECT 1 FROM @typeIssues)
BEGIN
    PRINT '  ⚠ 警告：以下列的类型或长度不正确：';
    SELECT '    - ' + ColumnName + ': ' + Issue FROM @typeIssues;
END
ELSE
    PRINT '  ✓ 所有列的类型和长度都正确';

GO

-- =============================================
-- 4. 验证迁移历史
-- =============================================
PRINT '验证迁移历史记录...';

IF NOT EXISTS (SELECT 1 FROM __EFMigrationsHistory WHERE MigrationId = '20251008023227_AddTargetServerConfiguration')
BEGIN
    PRINT '  ⚠ 警告：迁移历史表中缺少 AddTargetServerConfiguration 记录';
    PRINT '  如果列已存在，请手动插入记录：';
    PRINT '    INSERT INTO __EFMigrationsHistory (MigrationId, ProductVersion)';
    PRINT '    VALUES (''20251008023227_AddTargetServerConfiguration'', ''8.0.11'');';
END
ELSE
    PRINT '  ✓ 迁移历史记录完整';

GO

-- =============================================
-- 5. 最终验证报告
-- =============================================
PRINT '';
PRINT '========================================';
PRINT '最终验证报告';
PRINT '========================================';

SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
    c.is_nullable AS IsNullable,
    dc.definition AS DefaultValue
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
WHERE c.object_id = OBJECT_ID('ArchiveDataSource')
  AND c.name IN (
      'UseSourceAsTarget', 
      'TargetServerAddress', 
      'TargetServerPort', 
      'TargetDatabaseName',
      'TargetUseIntegratedSecurity',
      'TargetUserName',
      'TargetPassword'
  )
ORDER BY c.column_id;

PRINT '';
PRINT '数据统计：';
SELECT 
    COUNT(*) AS TotalRecords,
    SUM(CASE WHEN UseSourceAsTarget = 1 THEN 1 ELSE 0 END) AS UseSourceAsTarget_True,
    SUM(CASE WHEN TargetServerPort IS NULL THEN 1 ELSE 0 END) AS TargetServerPort_Null,
    SUM(CASE WHEN TargetServerAddress IS NOT NULL THEN 1 ELSE 0 END) AS HasCustomTarget
FROM ArchiveDataSource;

PRINT '';
PRINT '✓ 数据库修复脚本执行完成！';
PRINT '========================================';
GO
