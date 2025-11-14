using AntDesign;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Web.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Web.Pages.BackgroundTasks;

/// <summary>
/// 任务调度监控页面 - 代码逻辑部分
/// </summary>
public partial class MonitorBase : ComponentBase, IDisposable
{
    #region 依赖注入

    [Inject] private BackgroundTaskApiClient ExecutionApi { get; set; } = default!;
    [Inject] private MessageService Message { get; set; } = default!;
    [Inject] private ILogger<MonitorBase> Logger { get; set; } = default!;

    #endregion

    #region 页面参数

    /// <summary>
    /// 数据源ID路由参数 (可选,用于过滤特定数据源的任务)
    /// </summary>
    [Parameter]
    public Guid? DataSourceId { get; set; }

    #endregion

    #region 状态字段

    /// <summary>
    /// 是否正在加载
    /// </summary>
    protected bool Loading { get; set; }

    /// <summary>
    /// 是否启用自动刷新
    /// </summary>
    protected bool AutoRefresh { get; set; } = true;

    /// <summary>
    /// 选中的状态筛选
    /// </summary>
    protected string? SelectedStatus { get; set; }

    /// <summary>
    /// 所有任务列表
    /// </summary>
    protected List<BackgroundTaskSummaryModel> AllTasks { get; set; } = new();

    /// <summary>
    /// 过滤后的任务列表
    /// </summary>
    protected List<BackgroundTaskSummaryModel> FilteredTasks { get; set; } = new();

    /// <summary>
    /// 任务详情抽屉是否可见
    /// </summary>
    protected bool DetailDrawerVisible { get; set; }

    /// <summary>
    /// 当前查看的任务详情
    /// </summary>
    protected BackgroundTaskDetailModel? TaskDetail { get; set; }

    /// <summary>
    /// 日志查看抽屉是否可见
    /// </summary>
    protected bool LogDrawerVisible { get; set; }

    /// <summary>
    /// 当前查看的任务日志
    /// </summary>
    protected GetLogsPagedResponse? Logs { get; set; }

    /// <summary>
    /// 自动刷新定时器
    /// </summary>
    private System.Threading.Timer? _refreshTimer;

    /// <summary>
    /// 刷新间隔(毫秒)
    /// </summary>
    private const int RefreshIntervalMs = 5000;

    #endregion

    #region 生命周期

    /// <summary>
    /// 组件初始化时加载任务列表并启动定时器
    /// </summary>
    protected override async Task OnInitializedAsync()
    {
        await LoadTasksAsync();
        StartAutoRefresh();
    }

    /// <summary>
    /// 组件释放时停止定时器
    /// </summary>
    public void Dispose()
    {
        _refreshTimer?.Dispose();
    }

    #endregion

    #region 数据加载

    /// <summary>
    /// 加载任务列表
    /// </summary>
    private async Task LoadTasksAsync()
    {
        try
        {
            Loading = true;
            StateHasChanged();

            // 调用 API 获取任务列表 (默认获取最近 100 条)
            var tasks = await ExecutionApi.ListAsync(DataSourceId, maxCount: 100);
            AllTasks = tasks?.ToList() ?? new List<BackgroundTaskSummaryModel>();

            // 应用筛选
            ApplyFilter();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "加载任务列表失败");
            Message.Error("加载任务列表失败,请稍后重试");
        }
        finally
        {
            Loading = false;
            StateHasChanged();
        }
    }

    /// <summary>
    /// 刷新按钮点击事件
    /// </summary>
    protected async Task RefreshAsync()
    {
        await LoadTasksAsync();
        Message.Success("刷新成功");
    }

    /// <summary>
    /// 应用筛选条件按钮点击事件
    /// </summary>
    protected Task ApplyFilterAsync()
    {
        ApplyFilter();
        StateHasChanged();
        return Task.CompletedTask;
    }

    /// <summary>
    /// 应用筛选逻辑
    /// </summary>
    private void ApplyFilter()
    {
        if (string.IsNullOrWhiteSpace(SelectedStatus))
        {
            FilteredTasks = AllTasks;
        }
        else
        {
            FilteredTasks = AllTasks
                .Where(t => t.Status.Equals(SelectedStatus, StringComparison.OrdinalIgnoreCase))
                .ToList();
        }
    }

    #endregion

    #region 任务详情

    /// <summary>
    /// 显示任务详情
    /// </summary>
    protected async Task ShowDetailAsync(Guid taskId)
    {
        try
        {
            Loading = true;
            StateHasChanged();

            // 调用 API 获取任务详情
            TaskDetail = await ExecutionApi.GetAsync(taskId);
            DetailDrawerVisible = true;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "加载任务详情失败: {TaskId}", taskId);
            Message.Error("加载任务详情失败,请稍后重试");
        }
        finally
        {
            Loading = false;
            StateHasChanged();
        }
    }

    #endregion

    #region 任务日志

    /// <summary>
    /// 显示任务日志
    /// </summary>
    protected async Task ShowLogsAsync(Guid taskId)
    {
        try
        {
            Loading = true;
            StateHasChanged();

            // 调用 API 获取任务日志 (默认第一页,每页50条)
            Logs = await ExecutionApi.GetLogsAsync(taskId, pageIndex: 1, pageSize: 50);
            LogDrawerVisible = true;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "加载任务日志失败: {TaskId}", taskId);
            Message.Error("加载任务日志失败,请稍后重试");
        }
        finally
        {
            Loading = false;
            StateHasChanged();
        }
    }

    #endregion

    #region 任务取消

    /// <summary>
    /// 取消任务
    /// </summary>
    protected async Task CancelTaskAsync(Guid taskId)
    {
        try
        {
            Loading = true;
            StateHasChanged();

            // 调用 API 取消任务 (需要提供取消人信息,这里暂时硬编码,后续可从用户上下文获取)
            var success = await ExecutionApi.CancelTaskAsync(taskId, cancelledBy: "当前用户", reason: "用户手动取消");

            if (success)
            {
                Message.Success("任务已取消");
                // 重新加载任务列表
                await LoadTasksAsync();
            }
            else
            {
                Message.Error("取消任务失败");
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "取消任务失败: {TaskId}", taskId);
            Message.Error($"取消任务失败: {ex.Message}");
        }
        finally
        {
            Loading = false;
            StateHasChanged();
        }
    }

    /// <summary>
    /// 判断任务是否可取消
    /// </summary>
    protected bool IsTaskCancellable(string status)
    {
        return status is "PendingValidation" or "Validating" or "Queued" or "Running";
    }

    #endregion

    #region 自动刷新

    /// <summary>
    /// 启动自动刷新定时器
    /// </summary>
    private void StartAutoRefresh()
    {
        _refreshTimer = new System.Threading.Timer(async _ =>
        {
            if (!AutoRefresh) return;

            try
            {
                await InvokeAsync(async () =>
                {
                    await LoadTasksAsync();
                });
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "自动刷新失败");
            }
        }, null, RefreshIntervalMs, RefreshIntervalMs);
    }

    #endregion

    #region UI 辅助方法

    /// <summary>
    /// 获取状态显示文本
    /// </summary>
    protected string GetStatusDisplayText(string status)
    {
        return status switch
        {
            "PendingValidation" => "待校验",
            "Validating" => "校验中",
            "Queued" => "已排队",
            "Running" => "执行中",
            "Succeeded" => "已成功",
            "Failed" => "已失败",
            "Cancelled" => "已取消",
            _ => status
        };
    }

    /// <summary>
    /// 获取日志时间线颜色
    /// </summary>
    protected string GetLogColor(string category)
    {
        return category switch
        {
            "Error" => "red",
            "Warning" => "orange",
            "Information" => "blue",
            _ => "gray"
        };
    }

    /// <summary>
    /// 根据操作类型返回显示文本。
    /// </summary>
    protected string GetOperationDisplay(BackgroundTaskSummaryModel task)
    {
        return task.OperationType switch
        {
            BackgroundTaskOperationType.AddBoundary => "添加分区值",
            BackgroundTaskOperationType.SplitBoundary => "拆分分区",
            BackgroundTaskOperationType.MergeBoundary => "合并分区",
            BackgroundTaskOperationType.ArchiveSwitch => "归档（分区切换）",
            BackgroundTaskOperationType.ArchiveBcp => "归档（BCP）",
            BackgroundTaskOperationType.ArchiveBulkCopy => "归档（BulkCopy）",
            BackgroundTaskOperationType.Custom => "自定义任务",
            _ => string.IsNullOrWhiteSpace(task.TaskType) ? "未知任务" : task.TaskType
        };
    }

    /// <summary>
    /// 获取归档目标显示信息。
    /// </summary>
    protected string GetArchiveTargetDisplay(BackgroundTaskSummaryModel task)
    {
        if (string.IsNullOrWhiteSpace(task.ArchiveTargetDatabase) &&
            string.IsNullOrWhiteSpace(task.ArchiveTargetTable))
        {
            return "-";
        }

        var targetDb = string.IsNullOrWhiteSpace(task.ArchiveTargetDatabase)
            ? "(未指定库)"
            : task.ArchiveTargetDatabase;
        var targetTable = string.IsNullOrWhiteSpace(task.ArchiveTargetTable)
            ? "(未指定表)"
            : task.ArchiveTargetTable;

        return $"{targetDb} / {targetTable}";
    }

    #endregion
}
