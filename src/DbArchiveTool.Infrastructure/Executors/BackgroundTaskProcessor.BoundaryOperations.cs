using System.Diagnostics;
using System.Text.Json;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// BackgroundTaskProcessor 部分类 - 边界操作相关方法
/// 包含添加边界、拆分边界、合并边界的配置构建和执行逻辑
/// </summary>
internal sealed partial class BackgroundTaskProcessor
{
    /// <summary>
    /// 为"添加分区边界"操作构建临时配置对象
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigForAddBoundaryAsync(
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        // 解析快照JSON
        var snapshot = JsonSerializer.Deserialize<AddBoundarySnapshot>(task.ConfigurationSnapshot!);
        if (snapshot is null)
        {
            logger.LogError("无法解析 AddBoundary 快照：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }

        // 从数据库读取实际的分区元数据（这会返回完整的 PartitionConfiguration 对象）
        var config = await metadataRepository.GetConfigurationAsync(
            task.DataSourceId,
            snapshot.SchemaName,
            snapshot.TableName,
            cancellationToken);

        if (config is null)
        {
            logger.LogError("无法从数据库读取分区元数据：{Schema}.{Table}", snapshot.SchemaName, snapshot.TableName);
            return null;
        }

        // 返回实际读取的配置（已包含所有现有边界和文件组信息）
        // 注意：新边界已经在 PartitionManagementAppService 中通过DDL脚本添加
        // 这里只需要返回配置供后续权限校验等使用
        return config;
    }

    /// <summary>
    /// 为"拆分分区边界"操作构建临时配置对象
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigForSplitBoundaryAsync(
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        // 解析快照JSON
        var snapshot = JsonSerializer.Deserialize<SplitBoundarySnapshot>(task.ConfigurationSnapshot!);
        if (snapshot is null)
        {
            logger.LogError("无法解析 SplitBoundary 快照：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }

        // 从数据库读取实际的分区元数据（这会返回完整的 PartitionConfiguration 对象）
        var config = await metadataRepository.GetConfigurationAsync(
            task.DataSourceId,
            snapshot.SchemaName,
            snapshot.TableName,
            cancellationToken);

        if (config is null)
        {
            logger.LogError("无法从数据库读取分区元数据：{Schema}.{Table}", snapshot.SchemaName, snapshot.TableName);
            return null;
        }

        // 返回实际读取的配置（已包含所有现有边界和文件组信息）
        // 注意：拆分操作与添加边界类似，都是直接操作模式，不需要草稿配置
        return config;
    }

    /// <summary>
    /// 为"合并分区边界"操作构建临时配置对象
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigForMergeBoundaryAsync(
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        // 解析快照JSON
        var snapshot = JsonSerializer.Deserialize<MergeBoundarySnapshot>(task.ConfigurationSnapshot!);
        if (snapshot is null)
        {
            logger.LogError("无法解析 MergeBoundary 快照：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }

        // 从数据库读取实际的分区元数据
        var config = await metadataRepository.GetConfigurationAsync(
            task.DataSourceId,
            snapshot.SchemaName,
            snapshot.TableName,
            cancellationToken);

        if (config is null)
        {
            logger.LogError("无法从数据库读取分区元数据：{Schema}.{Table}", snapshot.SchemaName, snapshot.TableName);
            return null;
        }

        return config;
    }

    /// <summary>
    /// 执行"添加分区边界值"操作的简化流程
    /// </summary>
    private async Task ExecuteAddBoundaryAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:添加分区边界值。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<AddBoundarySnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析任务快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"目标表:{snapshot.SchemaName}.{snapshot.TableName},边界值:{snapshot.BoundaryValue},文件组:{snapshot.FilegroupName ?? "NEXT USED"}", 
                cancellationToken);

            // ============== 阶段 2: 加载数据源 ==============
            var dataSource = await dataSourceRepository.GetAsync(task.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                await HandleValidationFailureAsync(task, "未找到归档数据源配置。", cancellationToken);
                return;
            }

            task.UpdateProgress(0.2, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 3: 验证分区对象存在 ==============
            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "验证分区对象", 
                $"正在检查分区函数 {snapshot.PartitionFunctionName} 与分区方案 {snapshot.PartitionSchemeName} 是否存在...", 
                cancellationToken);

            var functionExists = await commandExecutor.CheckPartitionFunctionExistsAsync(
                task.DataSourceId,
                snapshot.PartitionFunctionName,
                cancellationToken);

            if (!functionExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区函数不存在", 
                    $"分区函数 {snapshot.PartitionFunctionName} 不存在,无法添加边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区函数 {snapshot.PartitionFunctionName} 不存在。", cancellationToken);
                return;
            }

            var schemeExists = await commandExecutor.CheckPartitionSchemeExistsAsync(
                task.DataSourceId,
                snapshot.PartitionSchemeName,
                cancellationToken);

            if (!schemeExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区方案不存在", 
                    $"分区方案 {snapshot.PartitionSchemeName} 不存在,无法添加边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区方案 {snapshot.PartitionSchemeName} 不存在。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(task.Id, "Info", "分区对象验证通过", 
                $"分区函数和分区方案均已存在。", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.3, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 进入执行队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "校验完成,任务进入执行队列。", cancellationToken);

            // ============== 阶段 5: 开始执行DDL ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.4, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "执行DDL", 
                $"正在执行分区边界添加DDL脚本...\n```sql\n{snapshot.DdlScript}\n```", 
                cancellationToken);

            // 创建数据库连接并执行DDL脚本
            try
            {
                await using var connection = await connectionFactory.CreateSqlConnectionAsync(task.DataSourceId, cancellationToken);

                await sqlExecutor.ExecuteAsync(
                    connection,
                    snapshot.DdlScript,
                    null,
                    null,
                    timeoutSeconds: LongRunningCommandTimeoutSeconds);

                stepWatch.Stop();

                await AppendLogAsync(task.Id, "Info", "DDL执行成功", 
                    $"成功添加分区边界值 '{snapshot.BoundaryValue}'。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(0.9, "SYSTEM");
                await taskRepository.UpdateAsync(task, cancellationToken);
            }
            catch (Exception ddlEx)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "DDL执行失败", 
                    $"执行DDL脚本时发生错误:\n{ddlEx.Message}", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                // 注意: 必须先更新进度再标记失败
                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", ddlEx.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            // 注意: 必须先更新进度再标记成功,因为 MarkSucceeded 会改变状态
            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkSucceeded("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var durationText = overallStopwatch.ElapsedMilliseconds < 1000
                ? $"{overallStopwatch.ElapsedMilliseconds} ms"
                : $"{overallStopwatch.Elapsed.TotalSeconds:F2} s";

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"添加分区边界值操作成功完成,总耗时:{durationText}。", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行添加分区边界值任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            // 注意: 必须先更新进度再标记失败
            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 执行"拆分分区边界"操作的简化流程(参考添加边界的流程)
    /// </summary>
    private async Task ExecuteSplitBoundaryAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:拆分分区边界。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<SplitBoundarySnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析任务快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"目标表:{snapshot.SchemaName}.{snapshot.TableName},边界值数量:{snapshot.Boundaries.Length},文件组:{snapshot.FilegroupName ?? "默认"}", 
                cancellationToken);

            // ============== 阶段 2: 加载数据源 ==============
            var dataSource = await dataSourceRepository.GetAsync(task.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                await HandleValidationFailureAsync(task, "未找到归档数据源配置。", cancellationToken);
                return;
            }

            task.UpdateProgress(0.2, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 3: 验证分区对象存在 ==============
            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "验证分区对象", 
                $"正在检查分区函数 {snapshot.PartitionFunctionName} 与分区方案 {snapshot.PartitionSchemeName} 是否存在...", 
                cancellationToken);

            var functionExists = await commandExecutor.CheckPartitionFunctionExistsAsync(
                task.DataSourceId,
                snapshot.PartitionFunctionName,
                cancellationToken);

            if (!functionExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区函数不存在", 
                    $"分区函数 {snapshot.PartitionFunctionName} 不存在,无法拆分边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区函数 {snapshot.PartitionFunctionName} 不存在。", cancellationToken);
                return;
            }

            var schemeExists = await commandExecutor.CheckPartitionSchemeExistsAsync(
                task.DataSourceId,
                snapshot.PartitionSchemeName,
                cancellationToken);

            if (!schemeExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区方案不存在", 
                    $"分区方案 {snapshot.PartitionSchemeName} 不存在,无法拆分边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区方案 {snapshot.PartitionSchemeName} 不存在。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(task.Id, "Info", "分区对象验证通过", 
                $"分区函数和分区方案均已存在。", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.3, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 进入执行队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "校验完成,任务进入执行队列。", cancellationToken);

            // ============== 阶段 5: 开始执行DDL ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.4, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "执行DDL", 
                $"正在执行分区拆分DDL脚本,将拆分 {snapshot.Boundaries.Length} 个边界值...\n```sql\n{snapshot.DdlScript}\n```", 
                cancellationToken);

            // 创建数据库连接并执行DDL脚本
            try
            {
                await using var connection = await connectionFactory.CreateSqlConnectionAsync(task.DataSourceId, cancellationToken);

                await sqlExecutor.ExecuteAsync(
                    connection,
                    snapshot.DdlScript,
                    null,
                    null,
                    timeoutSeconds: LongRunningCommandTimeoutSeconds);

                stepWatch.Stop();

                var boundariesDisplay = snapshot.Boundaries.Length == 1 
                    ? $"'{snapshot.Boundaries[0]}'" 
                    : $"{snapshot.Boundaries.Length} 个边界值";

                await AppendLogAsync(task.Id, "Info", "DDL执行成功", 
                    $"成功拆分分区边界值: {boundariesDisplay}。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(0.9, "SYSTEM");
                await taskRepository.UpdateAsync(task, cancellationToken);
            }
            catch (Exception ddlEx)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "DDL执行失败", 
                    $"执行DDL脚本时发生错误:\n{ddlEx.Message}", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", ddlEx.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkSucceeded("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var durationText = overallStopwatch.ElapsedMilliseconds < 1000
                ? $"{overallStopwatch.ElapsedMilliseconds} ms"
                : $"{overallStopwatch.Elapsed.TotalSeconds:F2} s";

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"拆分分区边界操作成功完成,处理了 {snapshot.Boundaries.Length} 个边界值,总耗时:{durationText}。", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行拆分分区边界任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 执行"合并分区边界"操作的简化流程
    /// </summary>
    private async Task ExecuteMergeBoundaryAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:合并分区边界。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<MergeBoundarySnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析任务快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"目标表:{snapshot.SchemaName}.{snapshot.TableName},删除边界:{snapshot.BoundaryKey}", 
                cancellationToken);

            // ============== 阶段 2: 加载数据源 ==============
            var dataSource = await dataSourceRepository.GetAsync(task.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                await HandleValidationFailureAsync(task, "未找到归档数据源配置。", cancellationToken);
                return;
            }

            task.UpdateProgress(0.2, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 3: 验证分区对象存在 ==============
            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "验证分区对象", 
                $"正在检查分区函数 {snapshot.PartitionFunctionName} 与分区方案 {snapshot.PartitionSchemeName} 是否存在...", 
                cancellationToken);

            var functionExists = await commandExecutor.CheckPartitionFunctionExistsAsync(
                task.DataSourceId,
                snapshot.PartitionFunctionName,
                cancellationToken);

            if (!functionExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区函数不存在", 
                    $"分区函数 {snapshot.PartitionFunctionName} 不存在,无法合并边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区函数 {snapshot.PartitionFunctionName} 不存在。", cancellationToken);
                return;
            }

            var schemeExists = await commandExecutor.CheckPartitionSchemeExistsAsync(
                task.DataSourceId,
                snapshot.PartitionSchemeName,
                cancellationToken);

            if (!schemeExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区方案不存在", 
                    $"分区方案 {snapshot.PartitionSchemeName} 不存在,无法合并边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区方案 {snapshot.PartitionSchemeName} 不存在。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(task.Id, "Info", "分区对象验证通过", 
                $"分区函数和分区方案均已存在。", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.3, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 进入执行队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "校验完成,任务进入执行队列。", cancellationToken);

            // ============== 阶段 5: 开始执行DDL ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.4, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "执行DDL", 
                $"正在执行分区合并DDL脚本,将删除边界值: '{snapshot.BoundaryKey}'...\n```sql\n{snapshot.DdlScript}\n```", 
                cancellationToken);

            // 创建数据库连接并执行DDL脚本
            try
            {
                await using var connection = await connectionFactory.CreateSqlConnectionAsync(task.DataSourceId, cancellationToken);

                await sqlExecutor.ExecuteAsync(
                    connection,
                    snapshot.DdlScript,
                    null,
                    null,
                    timeoutSeconds: LongRunningCommandTimeoutSeconds);

                stepWatch.Stop();

                await AppendLogAsync(task.Id, "Info", "DDL执行成功", 
                    $"成功合并分区边界值: '{snapshot.BoundaryKey}'。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(0.9, "SYSTEM");
                await taskRepository.UpdateAsync(task, cancellationToken);
            }
            catch (Exception ddlEx)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "DDL执行失败", 
                    $"执行DDL脚本时发生错误:\n{ddlEx.Message}", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", ddlEx.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkSucceeded("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var durationText = overallStopwatch.ElapsedMilliseconds < 1000
                ? $"{overallStopwatch.ElapsedMilliseconds} ms"
                : $"{overallStopwatch.Elapsed.TotalSeconds:F2} s";

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"合并分区边界操作成功完成,已删除边界值: '{snapshot.BoundaryKey}',总耗时:{durationText}。", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行合并分区边界任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }
}
