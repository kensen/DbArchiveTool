SET XACT_ABORT ON;
SET NOCOUNT ON;
BEGIN TRY
    BEGIN TRAN;
    ALTER TABLE [{SourceSchema}].[{SourceTable}] SWITCH PARTITION {SourcePartitionNumber}
    TO [{TargetSchema}].[{TargetTable}] PARTITION {TargetPartitionNumber};
    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR (''分区 SWITCH 操作失败: %s'', 16, 1, @ErrorMessage);
END CATCH;
