IF OBJECT_ID(N'[__EFMigrationsHistory]') IS NULL
BEGIN
    CREATE TABLE [__EFMigrationsHistory] (
        [MigrationId] nvarchar(150) NOT NULL,
        [ProductVersion] nvarchar(32) NOT NULL,
        CONSTRAINT [PK___EFMigrationsHistory] PRIMARY KEY ([MigrationId])
    );
END;
GO

BEGIN TRANSACTION;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20250930085014_AddAdminUser'
)
BEGIN
    CREATE TABLE [AdminUser] (
        [Id] uniqueidentifier NOT NULL,
        [UserName] nvarchar(64) NOT NULL,
        [PasswordHash] nvarchar(512) NOT NULL,
        [CreatedAtUtc] datetime2 NOT NULL,
        [CreatedBy] nvarchar(max) NOT NULL,
        [UpdatedAtUtc] datetime2 NOT NULL,
        [UpdatedBy] nvarchar(max) NOT NULL,
        [IsDeleted] bit NOT NULL,
        CONSTRAINT [PK_AdminUser] PRIMARY KEY ([Id])
    );
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20250930085014_AddAdminUser'
)
BEGIN
    CREATE TABLE [ArchiveTask] (
        [Id] uniqueidentifier NOT NULL,
        [DataSourceId] uniqueidentifier NOT NULL,
        [SourceTableName] nvarchar(128) NOT NULL,
        [TargetTableName] nvarchar(128) NOT NULL,
        [Status] int NOT NULL,
        [IsAutoArchive] bit NOT NULL,
        [StartedAtUtc] datetime2 NULL,
        [CompletedAtUtc] datetime2 NULL,
        [SourceRowCount] bigint NULL,
        [TargetRowCount] bigint NULL,
        [LegacyOperationRecordId] nvarchar(12) NULL,
        [CreatedAtUtc] datetime2 NOT NULL,
        [CreatedBy] nvarchar(max) NOT NULL,
        [UpdatedAtUtc] datetime2 NOT NULL,
        [UpdatedBy] nvarchar(max) NOT NULL,
        [IsDeleted] bit NOT NULL,
        CONSTRAINT [PK_ArchiveTask] PRIMARY KEY ([Id])
    );
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20250930085014_AddAdminUser'
)
BEGIN
    CREATE UNIQUE INDEX [IX_AdminUser_UserName] ON [AdminUser] ([UserName]);
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20250930085014_AddAdminUser'
)
BEGIN
    INSERT INTO [__EFMigrationsHistory] ([MigrationId], [ProductVersion])
    VALUES (N'20250930085014_AddAdminUser', N'8.0.11');
END;
GO

COMMIT;
GO

BEGIN TRANSACTION;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251001124957_AddArchiveDataSource'
)
BEGIN
    CREATE TABLE [ArchiveDataSource] (
        [Id] uniqueidentifier NOT NULL,
        [Name] nvarchar(100) NOT NULL,
        [Description] nvarchar(256) NULL,
        [ServerAddress] nvarchar(128) NOT NULL,
        [ServerPort] int NOT NULL,
        [DatabaseName] nvarchar(128) NOT NULL,
        [UserName] nvarchar(64) NULL,
        [Password] nvarchar(256) NULL,
        [UseIntegratedSecurity] bit NOT NULL,
        [IsEnabled] bit NOT NULL,
        [CreatedAtUtc] datetime2 NOT NULL,
        [CreatedBy] nvarchar(max) NOT NULL,
        [UpdatedAtUtc] datetime2 NOT NULL,
        [UpdatedBy] nvarchar(max) NOT NULL,
        [IsDeleted] bit NOT NULL,
        CONSTRAINT [PK_ArchiveDataSource] PRIMARY KEY ([Id])
    );
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251001124957_AddArchiveDataSource'
)
BEGIN
    INSERT INTO [__EFMigrationsHistory] ([MigrationId], [ProductVersion])
    VALUES (N'20251001124957_AddArchiveDataSource', N'8.0.11');
END;
GO

COMMIT;
GO

BEGIN TRANSACTION;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251007053916_AddPartitionCommandExtendedFields'
)
BEGIN
    CREATE TABLE [PartitionCommand] (
        [Id] uniqueidentifier NOT NULL,
        [DataSourceId] uniqueidentifier NOT NULL,
        [SchemaName] nvarchar(128) NOT NULL,
        [TableName] nvarchar(128) NOT NULL,
        [CommandType] int NOT NULL,
        [Status] int NOT NULL,
        [Script] nvarchar(max) NOT NULL,
        [ScriptHash] nvarchar(64) NOT NULL,
        [RiskNotes] nvarchar(2000) NOT NULL,
        [Payload] nvarchar(max) NOT NULL,
        [PreviewJson] nvarchar(max) NULL,
        [ExecutionLog] nvarchar(max) NULL,
        [RequestedBy] nvarchar(64) NOT NULL,
        [RequestedAt] datetime2 NOT NULL,
        [ExecutedAt] datetimeoffset NULL,
        [CompletedAt] datetimeoffset NULL,
        [FailureReason] nvarchar(max) NULL,
        [CreatedAtUtc] datetime2 NOT NULL,
        [CreatedBy] nvarchar(max) NOT NULL,
        [UpdatedAtUtc] datetime2 NOT NULL,
        [UpdatedBy] nvarchar(max) NOT NULL,
        [IsDeleted] bit NOT NULL,
        CONSTRAINT [PK_PartitionCommand] PRIMARY KEY ([Id])
    );
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251007053916_AddPartitionCommandExtendedFields'
)
BEGIN
    INSERT INTO [__EFMigrationsHistory] ([MigrationId], [ProductVersion])
    VALUES (N'20251007053916_AddPartitionCommandExtendedFields', N'8.0.11');
END;
GO

COMMIT;
GO

BEGIN TRANSACTION;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    ALTER TABLE [ArchiveDataSource] ADD [TargetDatabaseName] nvarchar(128) NULL;
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    ALTER TABLE [ArchiveDataSource] ADD [TargetPassword] nvarchar(256) NULL;
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    ALTER TABLE [ArchiveDataSource] ADD [TargetServerAddress] nvarchar(128) NULL;
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    ALTER TABLE [ArchiveDataSource] ADD [TargetServerPort] int NOT NULL DEFAULT 0;
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    ALTER TABLE [ArchiveDataSource] ADD [TargetUseIntegratedSecurity] bit NOT NULL DEFAULT CAST(0 AS bit);
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    ALTER TABLE [ArchiveDataSource] ADD [TargetUserName] nvarchar(64) NULL;
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    ALTER TABLE [ArchiveDataSource] ADD [UseSourceAsTarget] bit NOT NULL DEFAULT CAST(1 AS bit);
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    CREATE TABLE [PartitionCommand] (
        [Id] uniqueidentifier NOT NULL,
        [DataSourceId] uniqueidentifier NOT NULL,
        [SchemaName] nvarchar(128) NOT NULL,
        [TableName] nvarchar(128) NOT NULL,
        [CommandType] int NOT NULL,
        [Status] int NOT NULL,
        [Script] nvarchar(max) NOT NULL,
        [ScriptHash] nvarchar(max) NOT NULL,
        [RiskNotes] nvarchar(max) NOT NULL,
        [Payload] nvarchar(max) NOT NULL,
        [PreviewJson] nvarchar(max) NULL,
        [ExecutionLog] nvarchar(max) NULL,
        [RequestedBy] nvarchar(64) NOT NULL,
        [RequestedAt] datetime2 NOT NULL,
        [ExecutedAt] datetimeoffset NULL,
        [CompletedAt] datetimeoffset NULL,
        [FailureReason] nvarchar(max) NULL,
        [CreatedAtUtc] datetime2 NOT NULL,
        [CreatedBy] nvarchar(max) NOT NULL,
        [UpdatedAtUtc] datetime2 NOT NULL,
        [UpdatedBy] nvarchar(max) NOT NULL,
        [IsDeleted] bit NOT NULL,
        CONSTRAINT [PK_PartitionCommand] PRIMARY KEY ([Id])
    );
END;
GO

IF NOT EXISTS (
    SELECT * FROM [__EFMigrationsHistory]
    WHERE [MigrationId] = N'20251008023227_AddTargetServerConfiguration'
)
BEGIN
    INSERT INTO [__EFMigrationsHistory] ([MigrationId], [ProductVersion])
    VALUES (N'20251008023227_AddTargetServerConfiguration', N'8.0.11');
END;
GO

COMMIT;
GO

