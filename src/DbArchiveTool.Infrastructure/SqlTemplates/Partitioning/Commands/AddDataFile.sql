-- 添加数据文件到文件组
ALTER DATABASE {DatabaseName}
ADD FILE
(
    NAME = '{FileName}',
    FILENAME = '{FilePath}',
    SIZE = {InitialSizeMB}MB,
    FILEGROWTH = {GrowthSizeMB}MB
)
TO FILEGROUP {FilegroupName};
