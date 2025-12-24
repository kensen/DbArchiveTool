using System.Text.Json;
using System.Text.Json.Serialization;

namespace DbArchiveTool.Web.Models;

/// <summary>
/// JobExecutionStatus 枚举的容错 JSON 转换器
/// 当遇到未知值时，自动转换为 NotStarted
/// </summary>
public class JobExecutionStatusConverter : JsonConverter<JobExecutionStatus>
{
    public override JobExecutionStatus Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Number)
        {
            var value = reader.GetInt32();
            
            // 检查值是否在有效范围内 (0-4)
            if (Enum.IsDefined(typeof(JobExecutionStatus), value))
            {
                return (JobExecutionStatus)value;
            }
            
            // 未知值默认返回 NotStarted
            return JobExecutionStatus.NotStarted;
        }
        
        if (reader.TokenType == JsonTokenType.String)
        {
            var stringValue = reader.GetString();
            if (Enum.TryParse<JobExecutionStatus>(stringValue, true, out var result))
            {
                return result;
            }
            
            // 未知值默认返回 NotStarted
            return JobExecutionStatus.NotStarted;
        }
        
        throw new JsonException($"无法将 JSON 值转换为 JobExecutionStatus");
    }

    public override void Write(Utf8JsonWriter writer, JobExecutionStatus value, JsonSerializerOptions options)
    {
        writer.WriteNumberValue((int)value);
    }
}
