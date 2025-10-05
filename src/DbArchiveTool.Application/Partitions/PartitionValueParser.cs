using System.Globalization;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 提供将请求字符串解析为 PartitionValue 的工具，确保与数据类型匹配。
/// </summary>
public sealed class PartitionValueParser
{
    /// <summary>
    /// 尝试解析请求中的边界值集合，若格式错误返回失败原因。
    /// </summary>
    public Result<IReadOnlyList<PartitionValue>> ParseValues(PartitionColumn column, IReadOnlyList<string> boundaries)
    {
        if (boundaries.Count == 0)
        {
            return Result<IReadOnlyList<PartitionValue>>.Failure("至少需要一个分区边界。");
        }

        var results = new List<PartitionValue>();
        foreach (var raw in boundaries)
        {
            var parsed = ParseSingle(column.ValueKind, raw);
            if (!parsed.IsSuccess)
            {
                return Result<IReadOnlyList<PartitionValue>>.Failure(parsed.Error!);
            }

            results.Add(parsed.Value!);
        }

        return Result<IReadOnlyList<PartitionValue>>.Success(results);
    }

    private static Result<PartitionValue> ParseSingle(PartitionValueKind kind, string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return Result<PartitionValue>.Failure("边界值不能为空。");
        }

        try
        {
            return kind switch
            {
                PartitionValueKind.Int => Result<PartitionValue>.Success(PartitionValue.FromInt(int.Parse(raw, CultureInfo.InvariantCulture))),
                PartitionValueKind.BigInt => Result<PartitionValue>.Success(PartitionValue.FromBigInt(long.Parse(raw, CultureInfo.InvariantCulture))),
                PartitionValueKind.Date => Result<PartitionValue>.Success(PartitionValue.FromDate(DateOnly.Parse(raw, CultureInfo.InvariantCulture))),
                PartitionValueKind.DateTime => Result<PartitionValue>.Success(PartitionValue.FromDateTime(DateTime.Parse(raw, CultureInfo.InvariantCulture))),
                PartitionValueKind.DateTime2 => Result<PartitionValue>.Success(PartitionValue.FromDateTime2(DateTime.Parse(raw, CultureInfo.InvariantCulture))),
                PartitionValueKind.Guid => Result<PartitionValue>.Success(PartitionValue.FromGuid(Guid.Parse(raw))),
                PartitionValueKind.String => Result<PartitionValue>.Success(PartitionValue.FromString(raw)),
                _ => Result<PartitionValue>.Failure("不支持的分区列类型。")
            };
        }
        catch (Exception ex)
        {
            return Result<PartitionValue>.Failure($"无法解析边界值: {ex.Message}");
        }
    }
}
