-- 迁移 ArchiveMethod 枚举值
-- 从旧定义: Bcp=1, BulkCopy=2, PartitionSwitch=3
-- 到新定义: PartitionSwitch=0, Bcp=1, BulkCopy=2

SET QUOTED_IDENTIFIER ON;
GO

-- 查看现有数据
SELECT Id, Name, ArchiveMethod AS OldValue
FROM ArchiveConfiguration
WHERE IsDeleted = 0;
GO

-- 迁移数据
UPDATE ArchiveConfiguration
SET ArchiveMethod = CASE 
    WHEN ArchiveMethod = 3 THEN 0  -- PartitionSwitch: 3 → 0
    WHEN ArchiveMethod = 1 THEN 1  -- Bcp: 保持不变
    WHEN ArchiveMethod = 2 THEN 2  -- BulkCopy: 保持不变
    ELSE ArchiveMethod
END
WHERE IsDeleted = 0;
GO

-- 验证结果
SELECT Id, Name, ArchiveMethod AS NewValue
FROM ArchiveConfiguration
WHERE IsDeleted = 0;
GO

PRINT '迁移完成！';
