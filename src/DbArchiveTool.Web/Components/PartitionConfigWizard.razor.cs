using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using AntDesign;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Web.Core;
using DbArchiveTool.Web.Pages;
using DbArchiveTool.Web.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace DbArchiveTool.Web.Components;

public sealed partial class PartitionConfigWizard : ComponentBase
{
    [Parameter] public Guid DataSourceId { get; set; }
    [Parameter] public string DatabaseName { get; set; } = string.Empty;
    [Parameter] public bool Visible { get; set; }
    [Parameter] public EventCallback<bool> VisibleChanged { get; set; }
    [Parameter] public string Title { get; set; } = "添加分区配置";
    [Parameter] public PartitionTableInfo? SelectedTable { get; set; }
    [Parameter] public EventCallback OnCompleted { get; set; }
    [Parameter] public Guid? EditingConfigurationId { get; set; }
    [Parameter] public PartitionConfigurationDetailModel? EditingConfiguration { get; set; }
    [Parameter] public bool IsEditMode { get; set; }

    [Inject] private PartitionInfoApiClient PartitionInfoApi { get; set; } = default!;
    [Inject] private PartitionConfigurationApiClient PartitionConfigApi { get; set; } = default!;
    [Inject] private MessageService Message { get; set; } = default!;
    [Inject] private AdminSessionState AdminSession { get; set; } = default!;

    private bool _loadingWizard;
    private bool _submitting;
    private PartitionConfigWizardState _configWizard = new();
    private bool _wasVisible;
    private bool IsEditing => IsEditMode && EditingConfigurationId.HasValue && EditingConfiguration is not null;

    // 🚀 性能优化：缓存基础数据，避免每次打开都重新查询
    private List<DatabaseTableDto>? _cachedTables;
    private List<TargetDatabaseDto>? _cachedDatabases;
    private string? _cachedDefaultPath;
    private Guid? _cachedDataSourceId;

    /// <summary>
    /// 清除缓存数据，强制下次打开时重新加载
    /// </summary>
    public void ClearCache()
    {
        _cachedTables = null;
        _cachedDatabases = null;
        _cachedDefaultPath = null;
        _cachedDataSourceId = null;
    }

    private Task HandleDrawerClose()
    {
        return VisibleChanged.InvokeAsync(false);
    }

    protected override async Task OnParametersSetAsync()
    {
        if (Visible && !_wasVisible)
        {
            // 🚀 性能优化：检测是否需要重新加载（数据源变化或首次加载）
            bool needReload = _cachedDataSourceId != DataSourceId || _cachedTables == null;
            
            if (IsEditing)
            {
                await InitializeWizardForEditAsync(forceReload: needReload);
            }
            else
            {
                await InitializeWizardAsync(forceReload: needReload);
            }
            
            _cachedDataSourceId = DataSourceId;
        }

        _wasVisible = Visible;
    }

    private async Task InitializeWizardAsync(bool forceReload = false)
    {
        _loadingWizard = true;
        _configWizard = new PartitionConfigWizardState();

        try
        {
            // 🚀 性能优化：使用缓存或重新加载基础数据
            if (forceReload || _cachedTables == null)
            {
                var tablesTask = PartitionInfoApi.GetDatabaseTablesAsync(DataSourceId);
                var databasesTask = PartitionInfoApi.GetTargetDatabasesAsync(DataSourceId);
                var defaultPathTask = PartitionInfoApi.GetDefaultFilePathAsync(DataSourceId);
                
                await Task.WhenAll(tablesTask, databasesTask, defaultPathTask);
                
                _cachedTables = await tablesTask;
                _cachedDatabases = await databasesTask;
                _cachedDefaultPath = await defaultPathTask;
            }

            // ✅ 使用缓存数据
            var allTables = _cachedTables!;
            var databases = _cachedDatabases!;
            var defaultPath = _cachedDefaultPath;

            if (allTables.Count == 0)
            {
                Message.Warning("当前数据源暂无可用于配置的用户表。");
                await CloseAsync();
                return;
            }

            _configWizard.TableOptions = allTables
                .Select(t => new PartitionTableOption(
                    $"{t.SchemaName}.{t.TableName}",
                    $"{t.SchemaName}.{t.TableName}",
                    t.SchemaName,
                    t.TableName,
                    SelectedTable is not null &&
                    string.Equals(t.SchemaName, SelectedTable.SchemaName, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(t.TableName, SelectedTable.TableName, StringComparison.OrdinalIgnoreCase)
                        ? SelectedTable.PartitionColumn
                        : null))
                .ToList();

            _configWizard.Form.StorageMode = PartitionStorageMode.PrimaryFilegroup;
            _configWizard.Form.FilegroupName = "PRIMARY";
            _configWizard.Form.TargetDatabaseName = DatabaseName;
            _configWizard.Form.RequirePartitionColumnNotNull = false;
            _configWizard.Generator.StepDays = 1;
            _configWizard.CurrentStep = 0;

            var defaultOptionKey = SelectedTable is not null
                ? $"{SelectedTable.SchemaName}.{SelectedTable.TableName}"
                : _configWizard.TableOptions.First().Key;

            _configWizard.TargetDatabases = databases;
            var defaultDb = databases.FirstOrDefault(db => db.IsCurrent) ?? databases.FirstOrDefault();
            if (defaultDb is not null)
            {
                _configWizard.Form.TargetDatabaseName = defaultDb.Name;
            }

            if (!string.IsNullOrWhiteSpace(defaultPath))
            {
                _configWizard.CachedDefaultFileDirectory = defaultPath;
                _configWizard.Form.DataFileDirectory = defaultPath;
            }

            await ApplyTableSelectionAsync(defaultOptionKey, resetNaming: true);
            ApplyStorageModeDefaults();
        }
        catch (Exception ex)
        {
            Message.Error($"初始化分区配置向导失败: {ex.Message}");
            await CloseAsync();
        }
        finally
        {
            _loadingWizard = false;
        }
    }

    private async Task InitializeWizardForEditAsync(bool forceReload = false)
    {
        _loadingWizard = true;
        _configWizard = new PartitionConfigWizardState();

        if (!IsEditing)
        {
            await InitializeWizardAsync(forceReload);
            return;
        }

        try
        {
            var detail = EditingConfiguration!;
            var tableKey = $"{detail.SchemaName}.{detail.TableName}";

            // 🚀 性能优化：使用缓存或重新加载基础数据，列信息始终重新加载
            Task<List<DatabaseTableDto>> tablesTask;
            Task<List<TargetDatabaseDto>> databasesTask;
            Task<string?> defaultPathTask;
            
            if (forceReload || _cachedTables == null)
            {
                tablesTask = PartitionInfoApi.GetDatabaseTablesAsync(DataSourceId);
                databasesTask = PartitionInfoApi.GetTargetDatabasesAsync(DataSourceId);
                defaultPathTask = PartitionInfoApi.GetDefaultFilePathAsync(DataSourceId);
            }
            else
            {
                // ✅ 使用缓存，创建已完成的 Task
                tablesTask = Task.FromResult(_cachedTables);
                databasesTask = Task.FromResult(_cachedDatabases!);
                defaultPathTask = Task.FromResult(_cachedDefaultPath);
            }
            
            // 列信息必须重新查询（因为依赖具体表）
            var columnsTask = PartitionInfoApi.GetTableColumnsAsync(DataSourceId, detail.SchemaName, detail.TableName);
            
            await Task.WhenAll(tablesTask, databasesTask, defaultPathTask, columnsTask);
            
            // 更新缓存
            if (forceReload || _cachedTables == null)
            {
                _cachedTables = await tablesTask;
                _cachedDatabases = await databasesTask;
                _cachedDefaultPath = await defaultPathTask;
            }
            
            var allTables = _cachedTables!;
            var databases = _cachedDatabases!;
            var defaultPath = _cachedDefaultPath;
            var columns = await columnsTask;

            // 处理表选项
            _configWizard.TableOptions = allTables
                .Select(t => new PartitionTableOption(
                    $"{t.SchemaName}.{t.TableName}",
                    $"{t.SchemaName}.{t.TableName}",
                    t.SchemaName,
                    t.TableName,
                    null))
                .ToList();

            if (_configWizard.TableOptions.All(o => !o.Key.Equals(tableKey, StringComparison.OrdinalIgnoreCase)))
            {
                _configWizard.TableOptions.Add(new PartitionTableOption(
                    tableKey,
                    tableKey,
                    detail.SchemaName,
                    detail.TableName,
                    detail.PartitionColumnName));
            }

            _configWizard.Form.SourceTableKey = tableKey;
            _configWizard.Form.SchemaName = detail.SchemaName;
            _configWizard.Form.TableName = detail.TableName;

            // 处理目标数据库列表
            _configWizard.TargetDatabases = databases;
            if (_configWizard.TargetDatabases.All(db => !string.Equals(db.Name, detail.TargetDatabaseName, StringComparison.OrdinalIgnoreCase)))
            {
                _configWizard.TargetDatabases.Add(new TargetDatabaseDto
                {
                    Name = detail.TargetDatabaseName,
                    DatabaseId = -1,
                    IsCurrent = false
                });
            }
            _configWizard.Form.TargetDatabaseName = detail.TargetDatabaseName;
            _configWizard.Form.TargetSchemaName = detail.TargetSchemaName;
            _configWizard.Form.TargetTableName = detail.TargetTableName;

            // 处理默认文件路径
            if (!string.IsNullOrWhiteSpace(defaultPath))
            {
                _configWizard.CachedDefaultFileDirectory = defaultPath;
            }

            // 处理列信息（同步处理，避免再次API调用）
            if (columns.Count == 0)
            {
                throw new InvalidOperationException("未能读取分区列信息。");
            }

            _configWizard.Columns = columns;
            var defaultColumn = columns.FirstOrDefault(c => string.Equals(c.ColumnName, detail.PartitionColumnName, StringComparison.OrdinalIgnoreCase))
                                ?? columns.FirstOrDefault();
            if (defaultColumn is null)
            {
                throw new InvalidOperationException("未能读取分区列信息。");
            }

            // 🚀 性能优化：异步后台加载列统计信息（不阻塞主流程，加载完成后自动更新UI）
            _ = Task.Run(async () =>
            {
                await LoadColumnStatisticsAsync(detail.PartitionColumnName);
            });

            _configWizard.Form.PartitionColumn = detail.PartitionColumnName;
            _configWizard.Form.PartitionFunctionName = detail.PartitionFunctionName;
            _configWizard.Form.PartitionSchemeName = detail.PartitionSchemeName;
            _configWizard.SelectedColumnIsNullable = detail.PartitionColumnIsNullable;
            _configWizard.Form.RequirePartitionColumnNotNull = detail.RequirePartitionColumnNotNull;
            _configWizard.ColumnKind = detail.PartitionColumnKind;

            _configWizard.Form.StorageMode = detail.StorageMode;
            _configWizard.Form.FilegroupName = detail.FilegroupName;
            _configWizard.Form.DataFileDirectory = !string.IsNullOrWhiteSpace(detail.DataFileDirectory)
                ? detail.DataFileDirectory
                : _configWizard.CachedDefaultFileDirectory;
            _configWizard.Form.DataFileName = detail.DataFileName;
            _configWizard.Form.InitialFileSizeMb = detail.InitialFileSizeMb;
            _configWizard.Form.AutoGrowthMb = detail.AutoGrowthMb;
            _configWizard.Form.Remarks = detail.Remarks;

            _configWizard.Boundaries = detail.BoundaryValues
                .Select(value => PartitionValue.FromInvariantString(detail.PartitionColumnKind, value))
                .Select(boundaryValue => new PartitionBoundaryItem(boundaryValue))
                .ToList();

            _configWizard.ConfigurationId = detail.Id;
            _configWizard.CurrentStep = 0;
        }
        catch (Exception ex)
        {
            Message.Error($"加载配置详情失败: {ex.Message}");
            await CloseAsync();
        }
        finally
        {
            _loadingWizard = false;
        }
    }

    private async Task ApplyTableSelectionAsync(string tableKey, bool resetNaming)
    {
        var option = _configWizard.TableOptions.FirstOrDefault(o => o.Key.Equals(tableKey, StringComparison.OrdinalIgnoreCase));
        if (option is null)
        {
            return;
        }

        _configWizard.Form.SourceTableKey = option.Key;
        _configWizard.Form.SchemaName = option.SchemaName;
        _configWizard.Form.TableName = option.TableName;

        if (resetNaming || string.IsNullOrWhiteSpace(_configWizard.Form.TargetSchemaName))
        {
            _configWizard.Form.TargetSchemaName = option.SchemaName;
        }

        if (resetNaming || string.IsNullOrWhiteSpace(_configWizard.Form.TargetTableName))
        {
            _configWizard.Form.TargetTableName = $"{option.TableName}_bak";
        }

        if (resetNaming || string.IsNullOrWhiteSpace(_configWizard.Form.FilegroupName))
        {
            _configWizard.Form.FilegroupName = "PRIMARY";
        }

        _configWizard.Form.PartitionColumn = string.Empty;
        _configWizard.Columns.Clear();
        _configWizard.Boundaries.Clear();

        await LoadColumnsAsync(option.SchemaName, option.TableName, option.PartitionColumn);
    }

    private async Task LoadColumnsAsync(string schemaName, string tableName, string? preferredColumn)
    {
        // 🚀 优化：立即显示加载状态，然后异步加载列
        _configWizard.Columns.Clear();
        _configWizard.ColumnMinValue = null;
        _configWizard.ColumnMaxValue = null;
        await InvokeAsync(StateHasChanged); // 立即刷新 UI，显示空状态
        
        var columns = await PartitionInfoApi.GetTableColumnsAsync(DataSourceId, schemaName, tableName);
        if (columns.Count == 0)
        {
            throw new InvalidOperationException("未能读取分区列信息。");
        }

        _configWizard.Columns = columns;
        await InvokeAsync(StateHasChanged); // 列列表加载完成，立即显示
        
        var defaultColumn = columns.FirstOrDefault(c => string.Equals(c.ColumnName, preferredColumn, StringComparison.OrdinalIgnoreCase))
                            ?? columns.FirstOrDefault();
        if (defaultColumn is null)
        {
            throw new InvalidOperationException("未能读取分区列信息。");
        }

        _configWizard.Form.PartitionColumn = defaultColumn.ColumnName;
        _configWizard.SelectedColumnIsNullable = defaultColumn.IsNullable;
        _configWizard.Form.RequirePartitionColumnNotNull = defaultColumn.IsNullable;
        _configWizard.ColumnKind = ResolveValueKind(defaultColumn);
        
        // 🚀 优化：列统计异步后台加载（不阻塞列下拉框显示）
        _ = Task.Run(async () =>
        {
            await LoadColumnStatisticsAsync(defaultColumn.ColumnName);
        });
    }

    private async Task HandleSourceTableChanged(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        try
        {
            await ApplyTableSelectionAsync(value, resetNaming: true);
            ApplyStorageModeDefaults();
            await InvokeAsync(StateHasChanged);
        }
        catch (Exception ex)
        {
            Message.Error($"加载分区元数据失败: {ex.Message}");
        }
    }

    private async Task HandlePartitionColumnChanged(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        var column = _configWizard.Columns.FirstOrDefault(c => string.Equals(c.ColumnName, value, StringComparison.OrdinalIgnoreCase));
        if (column is null)
        {
            return;
        }

        _configWizard.SelectedColumnIsNullable = column.IsNullable;
        _configWizard.Form.RequirePartitionColumnNotNull = column.IsNullable;
        _configWizard.ColumnKind = ResolveValueKind(column);
        _configWizard.Boundaries.Clear();
        _configWizard.ColumnMinValue = null;
        _configWizard.ColumnMaxValue = null;

        await LoadColumnStatisticsAsync(column.ColumnName);
        await InvokeAsync(StateHasChanged);
    }

    private async Task LoadColumnStatisticsAsync(string columnName)
    {
        try
        {
            // 🔧 优化：添加加载状态指示
            _configWizard.ColumnMinValue = "加载中...";
            _configWizard.ColumnMaxValue = "加载中...";
            await InvokeAsync(StateHasChanged);
            
            var stats = await PartitionInfoApi.GetColumnStatisticsAsync(DataSourceId, _configWizard.Form.SchemaName, _configWizard.Form.TableName, columnName);
            _configWizard.ColumnMinValue = stats?.MinValue ?? "-";
            _configWizard.ColumnMaxValue = stats?.MaxValue ?? "-";
            
            // 🔔 通知 UI 更新：统计数据已加载完成
            await InvokeAsync(StateHasChanged);
        }
        catch (Exception ex)
        {
            _configWizard.ColumnMinValue = $"加载失败: {ex.Message}";
            _configWizard.ColumnMaxValue = "-";
            await InvokeAsync(StateHasChanged);
        }
    }

    private async Task OnStorageModeChanged(PartitionStorageMode mode)
    {
        _configWizard.Form.StorageMode = mode;
        ApplyStorageModeDefaults();
        await InvokeAsync(StateHasChanged);
    }

    private void RegenerateDataFileName()
    {
        var db = string.IsNullOrWhiteSpace(_configWizard.Form.TargetDatabaseName) ? DatabaseName : _configWizard.Form.TargetDatabaseName;
        var fg = _configWizard.Form.FilegroupName ?? "FG";
        _configWizard.Form.DataFileName = $"{db}_{fg}_p{DateTime.UtcNow:yyyyMMddHHmm}.ndf";
    }

    private void ApplyStorageModeDefaults()
    {
        if (_configWizard.Form.StorageMode == PartitionStorageMode.DedicatedFilegroupSingleFile)
        {
            var filegroupChanged = false;

            if (string.IsNullOrWhiteSpace(_configWizard.Form.FilegroupName) || string.Equals(_configWizard.Form.FilegroupName, "PRIMARY", StringComparison.OrdinalIgnoreCase))
            {
                _configWizard.Form.FilegroupName = $"{_configWizard.Form.TableName}_FG_{DateTime.UtcNow:yyyyMMdd}";
                filegroupChanged = true;
            }

            if (string.IsNullOrWhiteSpace(_configWizard.Form.DataFileDirectory) && !string.IsNullOrWhiteSpace(_configWizard.CachedDefaultFileDirectory))
            {
                _configWizard.Form.DataFileDirectory = _configWizard.CachedDefaultFileDirectory;
            }

            if (filegroupChanged || string.IsNullOrWhiteSpace(_configWizard.Form.DataFileName))
            {
                RegenerateDataFileName();
            }

            _configWizard.Form.InitialFileSizeMb ??= 512;
            _configWizard.Form.AutoGrowthMb ??= 128;
        }
        else
        {
            _configWizard.Form.FilegroupName = string.IsNullOrWhiteSpace(_configWizard.Form.FilegroupName) ? "PRIMARY" : _configWizard.Form.FilegroupName;
            _configWizard.Form.DataFileDirectory = null;
            _configWizard.Form.DataFileName = null;
            _configWizard.Form.InitialFileSizeMb = null;
            _configWizard.Form.AutoGrowthMb = null;
        }
    }

    private bool ValidateStepOne()
    {
        if (string.IsNullOrWhiteSpace(_configWizard.Form.SourceTableKey))
        {
            Message.Warning("请选择源表。");
            return false;
        }

        if (string.IsNullOrWhiteSpace(_configWizard.Form.PartitionColumn))
        {
            Message.Warning("请选择分区列。");
            return false;
        }

        if (string.IsNullOrWhiteSpace(_configWizard.Form.FilegroupName))
        {
            Message.Warning("请输入文件组名称。");
            return false;
        }

        if (!Regex.IsMatch(_configWizard.Form.FilegroupName, @"^[A-Za-z_][A-Za-z0-9_]*$"))
        {
            Message.Warning("文件组名称仅支持字母、数字与下划线，并以字母或下划线开头。");
            return false;
        }

        if (_configWizard.Form.StorageMode == PartitionStorageMode.DedicatedFilegroupSingleFile)
        {
            if (string.IsNullOrWhiteSpace(_configWizard.Form.DataFileDirectory))
            {
                Message.Warning("请输入数据文件目录。");
                return false;
            }

            if (string.IsNullOrWhiteSpace(_configWizard.Form.DataFileName))
            {
                Message.Warning("请输入数据文件名称。");
                return false;
            }

            if (!Regex.IsMatch(_configWizard.Form.DataFileName, @"^[A-Za-z0-9_\-\.]+$"))
            {
                Message.Warning("数据文件名称仅支持字母、数字、下划线、短横线与点号。");
                return false;
            }

            if (!_configWizard.Form.InitialFileSizeMb.HasValue || _configWizard.Form.InitialFileSizeMb <= 0)
            {
                Message.Warning("请填写有效的初始大小。");
                return false;
            }

            if (!_configWizard.Form.AutoGrowthMb.HasValue || _configWizard.Form.AutoGrowthMb <= 0)
            {
                Message.Warning("请填写有效的自动增长大小。");
                return false;
            }
        }

        if (string.IsNullOrWhiteSpace(_configWizard.Form.TargetDatabaseName))
        {
            Message.Warning("请选择目标数据库。");
            return false;
        }

        if (string.IsNullOrWhiteSpace(_configWizard.Form.TargetSchemaName))
        {
            Message.Warning("请输入目标架构。");
            return false;
        }

        if (string.IsNullOrWhiteSpace(_configWizard.Form.TargetTableName))
        {
            Message.Warning("请输入目标表名称。");
            return false;
        }

        return true;
    }

    private bool ValidateStepTwo()
    {
        if (_configWizard.Boundaries.Count == 0)
        {
            Message.Warning("请至少添加一个分区边界值。");
            return false;
        }

        return true;
    }

    private void NextStep()
    {
        if (_configWizard.CurrentStep == 0 && ValidateStepOne())
        {
            _configWizard.CurrentStep = 1;
        }
    }

    private void PrevStep()
    {
        if (_configWizard.CurrentStep > 0)
        {
            _configWizard.CurrentStep--;
        }
    }

    private async Task CloseAsync()
    {
        if (VisibleChanged.HasDelegate)
        {
            await VisibleChanged.InvokeAsync(false);
        }
    }

    private void AddManualBoundary()
    {
        if (!TryParseBoundaryValue(_configWizard.NewBoundaryValue, out var partitionValue, out var error))
        {
            if (!string.IsNullOrEmpty(error))
            {
                Message.Warning(error);
            }

            return;
        }

        var invariant = partitionValue.ToInvariantString();
        if (_configWizard.Boundaries.Any(x => x.DisplayValue.Equals(invariant, StringComparison.Ordinal)))
        {
            Message.Warning("该边界值已存在。");
            _configWizard.NewBoundaryValue = string.Empty;
            return;
        }

        _configWizard.Boundaries.Add(new PartitionBoundaryItem(partitionValue));
        _configWizard.NewBoundaryValue = string.Empty;
    }

    private void ClearBoundaryValues()
    {
        _configWizard.Boundaries.Clear();
    }

    private void RemoveBoundary(Guid id)
    {
        var item = _configWizard.Boundaries.FirstOrDefault(x => x.Id == id);
        if (item is not null)
        {
            _configWizard.Boundaries.Remove(item);
        }
    }

    private void GenerateNumericBoundaries()
    {
        if (!TryParseBoundaryValue(_configWizard.Generator.StartValue, out var startValue, out var error) ||
            !TryParseBoundaryValue(_configWizard.Generator.EndValue, out var endValue, out error))
        {
            if (!string.IsNullOrEmpty(error))
            {
                Message.Warning(error);
            }

            return;
        }

        if (!int.TryParse(_configWizard.Generator.StepValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var step) || step <= 0)
        {
            Message.Warning("请输入有效的步长。");
            return;
        }

        var start = startValue.ToInvariantString();
        var end = endValue.ToInvariantString();

        if (!int.TryParse(start, NumberStyles.Integer, CultureInfo.InvariantCulture, out var startInt) ||
            !int.TryParse(end, NumberStyles.Integer, CultureInfo.InvariantCulture, out var endInt))
        {
            Message.Warning("起始值或结束值无效。");
            return;
        }

        if (endInt <= startInt)
        {
            Message.Warning("结束值必须大于起始值。");
            return;
        }

        for (var current = startInt + step; current <= endInt; current += step)
        {
            var value = PartitionValue.FromInt(current);
            var invariant = value.ToInvariantString();
            if (_configWizard.Boundaries.Any(x => x.DisplayValue.Equals(invariant, StringComparison.Ordinal)))
            {
                continue;
            }

            _configWizard.Boundaries.Add(new PartitionBoundaryItem(value));
        }
    }

    private void GenerateDateBoundaries()
    {
        if (!_configWizard.Generator.StartDate.HasValue || !_configWizard.Generator.EndDate.HasValue)
        {
            Message.Warning("请选择起始与结束日期。");
            return;
        }

        var start = _configWizard.Generator.StartDate.Value;
        var end = _configWizard.Generator.EndDate.Value;

        if (end <= start)
        {
            Message.Warning("结束日期必须大于起始日期。");
            return;
        }

        // 根据粒度生成边界值
        var current = start;
        var isYearly = string.Equals(_configWizard.Generator.DateGranularity, "year", StringComparison.OrdinalIgnoreCase);

        while (current <= end)
        {
            // 按月或按年的第一天生成边界值
            var boundaryDate = new DateTime(current.Year, current.Month, 1);
            
            PartitionValue value = _configWizard.ColumnKind switch
            {
                PartitionValueKind.Date => PartitionValue.FromDate(DateOnly.FromDateTime(boundaryDate)),
                PartitionValueKind.DateTime => PartitionValue.FromDateTime(boundaryDate),
                PartitionValueKind.DateTime2 => PartitionValue.FromDateTime2(boundaryDate),
                _ => PartitionValue.FromString(boundaryDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))
            };

            var invariant = value.ToInvariantString();
            if (_configWizard.Boundaries.All(x => !x.DisplayValue.Equals(invariant, StringComparison.Ordinal)))
            {
                _configWizard.Boundaries.Add(new PartitionBoundaryItem(value));
            }

            // 按年或按月递增
            current = isYearly ? current.AddYears(1) : current.AddMonths(1);
        }
    }

    private async Task SubmitAsync()
    {
        if (!ValidateStepTwo())
        {
            return;
        }

        _submitting = true;

        try
        {
            var boundaryValues = _configWizard.Boundaries
                .Select(b => b.Value.ToInvariantString())
                .OrderBy(v => v, StringComparer.Ordinal)
                .ToList();

            var userName = string.IsNullOrWhiteSpace(AdminSession.UserName) ? "SYSTEM" : AdminSession.UserName!;
            var successMessage = IsEditing ? "分区配置更新成功。" : "分区配置创建成功。";
            Guid configurationId;

            if (IsEditing && _configWizard.ConfigurationId.HasValue)
            {
                var updateRequest = new UpdatePartitionConfigurationRequestModel
                {
                    StorageMode = _configWizard.Form.StorageMode,
                    FilegroupName = _configWizard.Form.FilegroupName,
                    DataFileDirectory = _configWizard.Form.DataFileDirectory,
                    DataFileName = _configWizard.Form.DataFileName,
                    InitialFileSizeMb = _configWizard.Form.InitialFileSizeMb,
                    AutoGrowthMb = _configWizard.Form.AutoGrowthMb,
                    TargetDatabaseName = _configWizard.Form.TargetDatabaseName,
                    TargetSchemaName = _configWizard.Form.TargetSchemaName ?? _configWizard.Form.SchemaName,
                    TargetTableName = _configWizard.Form.TargetTableName,
                    RequirePartitionColumnNotNull = _configWizard.Form.RequirePartitionColumnNotNull,
                    Remarks = _configWizard.Form.Remarks,
                    UpdatedBy = userName
                };

                var updateResult = await PartitionConfigApi.UpdateAsync(_configWizard.ConfigurationId.Value, updateRequest);
                if (!updateResult.IsSuccess)
                {
                    Message.Error(updateResult.Error ?? "更新分区配置失败。");
                    return;
                }

                configurationId = _configWizard.ConfigurationId.Value;
            }
            else
            {
                var createRequest = new CreatePartitionConfigurationRequestModel
                {
                    DataSourceId = DataSourceId,
                    SchemaName = _configWizard.Form.SchemaName,
                    TableName = _configWizard.Form.TableName,
                    PartitionColumnName = _configWizard.Form.PartitionColumn,
                    PartitionColumnKind = _configWizard.ColumnKind,
                    PartitionColumnIsNullable = _configWizard.SelectedColumnIsNullable,
                    StorageMode = _configWizard.Form.StorageMode,
                    FilegroupName = _configWizard.Form.FilegroupName,
                    DataFileDirectory = _configWizard.Form.DataFileDirectory,
                    DataFileName = _configWizard.Form.DataFileName,
                    InitialFileSizeMb = _configWizard.Form.InitialFileSizeMb,
                    AutoGrowthMb = _configWizard.Form.AutoGrowthMb,
                    TargetDatabaseName = _configWizard.Form.TargetDatabaseName,
                    TargetSchemaName = _configWizard.Form.TargetSchemaName ?? _configWizard.Form.SchemaName,
                    TargetTableName = _configWizard.Form.TargetTableName,
                    RequirePartitionColumnNotNull = _configWizard.Form.RequirePartitionColumnNotNull,
                    Remarks = _configWizard.Form.Remarks,
                    CreatedBy = userName
                };

                var createResult = await PartitionConfigApi.CreateAsync(createRequest);
                if (!createResult.IsSuccess)
                {
                    Message.Error(createResult.Error ?? "创建分区配置失败。");
                    return;
                }

                configurationId = createResult.Value;
                _configWizard.ConfigurationId = configurationId;
            }

            var replaceRequest = new ReplacePartitionValuesRequestModel
            {
                BoundaryValues = boundaryValues,
                UpdatedBy = userName
            };

            var replaceResult = await PartitionConfigApi.ReplaceValuesAsync(configurationId, replaceRequest);
            if (!replaceResult.IsSuccess)
            {
                Message.Error(replaceResult.Error ?? "保存分区边界值失败。");
                return;
            }

            _configWizard.ConfigurationId = configurationId;
            _configWizard.CurrentStep = 2;

            if (OnCompleted.HasDelegate)
            {
                await OnCompleted.InvokeAsync();
            }

            Message.Success(successMessage);
        }
        catch (Exception ex)
        {
            Message.Error($"提交分区配置失败: {ex.Message}");
        }
        finally
        {
            _submitting = false;
        }
    }

    private void OnBoundaryInputKeyPress(KeyboardEventArgs args)
    {
        if (args.Key == "Enter")
        {
            AddManualBoundary();
        }
    }

    private bool TryParseBoundaryValue(string? input, out PartitionValue value, out string error)
    {
        value = null!;
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(input))
        {
            error = "边界值不能为空。";
            return false;
        }

        var raw = input.Trim();

        try
        {
            switch (_configWizard.ColumnKind)
            {
                case PartitionValueKind.Int:
                    if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue))
                    {
                        error = "请输入有效的整数边界值。";
                        return false;
                    }
                    value = PartitionValue.FromInt(intValue);
                    return true;
                case PartitionValueKind.BigInt:
                    if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
                    {
                        error = "请输入有效的长整型边界值。";
                        return false;
                    }
                    value = PartitionValue.FromBigInt(longValue);
                    return true;
                case PartitionValueKind.Date:
                    if (!DateOnly.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateOnly))
                    {
                        error = "请输入有效的日期边界值 (yyyy-MM-dd)。";
                        return false;
                    }
                    value = PartitionValue.FromDate(dateOnly);
                    return true;
                case PartitionValueKind.DateTime:
                case PartitionValueKind.DateTime2:
                    if (!DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var dateTime))
                    {
                        error = "请输入有效的日期时间边界值。";
                        return false;
                    }
                    value = _configWizard.ColumnKind == PartitionValueKind.DateTime
                        ? PartitionValue.FromDateTime(dateTime)
                        : PartitionValue.FromDateTime2(dateTime);
                    return true;
                case PartitionValueKind.Guid:
                    if (!Guid.TryParse(raw, out var guid))
                    {
                        error = "请输入有效的 GUID 边界值。";
                        return false;
                    }
                    value = PartitionValue.FromGuid(guid);
                    return true;
                default:
                    value = PartitionValue.FromString(raw);
                    return true;
            }
        }
        catch (Exception ex)
        {
            error = $"解析边界值失败: {ex.Message}";
            return false;
        }
    }

    private PartitionValueKind ResolveValueKind(PartitionTableColumnDto column)
    {
        return column.DataType.ToLowerInvariant() switch
        {
            "int" => PartitionValueKind.Int,
            "bigint" => PartitionValueKind.BigInt,
            "date" => PartitionValueKind.Date,
            "datetime" => PartitionValueKind.DateTime,
            "datetime2" => PartitionValueKind.DateTime2,
            "uniqueidentifier" => PartitionValueKind.Guid,
            _ => PartitionValueKind.String
        };
    }

    private string FormatColumnStatValue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        if (_configWizard.ColumnKind is PartitionValueKind.Date or PartitionValueKind.DateTime or PartitionValueKind.DateTime2)
        {
            if (DateTime.TryParse(value, out var dateTime))
            {
                if (_configWizard.ColumnKind == PartitionValueKind.Date)
                {
                    return dateTime.ToString("yyyy-MM-dd");
                }

                return dateTime.ToString("yyyy-MM-dd HH:mm:ss");
            }
        }

        return value;
    }

    private bool SupportsAutoGeneration =>
        _configWizard.ColumnKind is PartitionValueKind.Int or PartitionValueKind.BigInt or PartitionValueKind.Date or PartitionValueKind.DateTime or PartitionValueKind.DateTime2;

    private bool IsDateKind =>
        _configWizard.ColumnKind is PartitionValueKind.Date or PartitionValueKind.DateTime or PartitionValueKind.DateTime2;

    private sealed class PartitionConfigWizardState
    {
        public int CurrentStep { get; set; }
        public PartitionConfigForm Form { get; } = new();
        public List<PartitionTableColumnDto> Columns { get; set; } = new();
        public List<TargetDatabaseDto> TargetDatabases { get; set; } = new();
        public List<PartitionTableOption> TableOptions { get; set; } = new();
        public List<PartitionBoundaryItem> Boundaries { get; set; } = new();
        public string? ColumnMinValue { get; set; }
        public string? ColumnMaxValue { get; set; }
        public PartitionValueKind ColumnKind { get; set; } = PartitionValueKind.String;
        public bool SelectedColumnIsNullable { get; set; }
        public string NewBoundaryValue { get; set; } = string.Empty;
        public BoundaryGeneratorModel Generator { get; } = new();
        public Guid? ConfigurationId { get; set; }
        public string? CachedDefaultFileDirectory { get; set; }
    }

    private sealed class PartitionConfigForm
    {
        public string SourceTableKey { get; set; } = string.Empty;
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string PartitionColumn { get; set; } = string.Empty;
        public string PartitionFunctionName { get; set; } = string.Empty;
        public string PartitionSchemeName { get; set; } = string.Empty;
        public bool RequirePartitionColumnNotNull { get; set; }
        public PartitionStorageMode StorageMode { get; set; } = PartitionStorageMode.PrimaryFilegroup;
        public string? FilegroupName { get; set; }
        public string? DataFileDirectory { get; set; }
        public string? DataFileName { get; set; }
        public int? InitialFileSizeMb { get; set; }
        public int? AutoGrowthMb { get; set; }
        public string TargetDatabaseName { get; set; } = string.Empty;
        public string? TargetSchemaName { get; set; }
        public string TargetTableName { get; set; } = string.Empty;
        public string? Remarks { get; set; }
    }

    private sealed class BoundaryGeneratorModel
    {
        public string? StartValue { get; set; }
        public string? EndValue { get; set; }
        public string? StepValue { get; set; }
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public int StepDays { get; set; } = 1;
        public string DateGranularity { get; set; } = "month";
    }

    private sealed class PartitionBoundaryItem
    {
        public PartitionBoundaryItem(PartitionValue value)
        {
            Value = value;
        }

        public Guid Id { get; } = Guid.NewGuid();
        public PartitionValue Value { get; }
        public string DisplayValue => Value.ToInvariantString();
    }

    private sealed class PartitionTableOption
    {
        public PartitionTableOption(string key, string label, string schemaName, string tableName, string? partitionColumn)
        {
            Key = key;
            Label = label;
            SchemaName = schemaName;
            TableName = tableName;
            PartitionColumn = partitionColumn;
        }

        public string Key { get; }
        public string Label { get; }
        public string SchemaName { get; }
        public string TableName { get; }
        public string? PartitionColumn { get; }
    }
}
