// 测试解析 KeyColumns 的逻辑
using System;
using System.Linq;

var keyColumns = "[OrderDate] DESC, [CustomerId] ASC, [OrderId] ASC";
var partitionColumnName = "OrderId";

var columnList = keyColumns.Split(',')
    .Select(c => c.Trim().TrimStart('[').TrimEnd(']').Split(' ')[0])
    .ToList();

Console.WriteLine("解析结果:");
foreach (var col in columnList)
{
    Console.WriteLine($"  - '{col}'");
}

var contains = columnList.Contains(partitionColumnName, StringComparer.OrdinalIgnoreCase);
Console.WriteLine($"\n包含 '{partitionColumnName}': {contains}");

// 测试边界情况
Console.WriteLine("\n=== 测试各种格式 ===");

var testCases = new[]
{
    "[OrderDate] DESC, [CustomerId] ASC",
    "[OrderDate] DESC, [CustomerId] ASC, [OrderId] ASC",
    "[OrderDate] DESC,[CustomerId] ASC,[OrderId] ASC",  // 没有空格
    "[OrderDate]DESC,[CustomerId]ASC",  // 紧贴
};

foreach (var test in testCases)
{
    var parsed = test.Split(',')
        .Select(c => c.Trim().TrimStart('[').TrimEnd(']').Split(' ')[0])
        .ToList();
    var hasOrderId = parsed.Contains("OrderId", StringComparer.OrdinalIgnoreCase);
    Console.WriteLine($"\n输入: {test}");
    Console.WriteLine($"解析: {string.Join(", ", parsed)}");
    Console.WriteLine($"包含OrderId: {hasOrderId}");
}
