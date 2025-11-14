# DbArchiveTool API Dockerfile
# 多阶段构建，优化镜像大小

# ============ 构建阶段 ============
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# 复制项目文件并还原依赖
COPY ["src/DbArchiveTool.Api/DbArchiveTool.Api.csproj", "src/DbArchiveTool.Api/"]
COPY ["src/DbArchiveTool.Application/DbArchiveTool.Application.csproj", "src/DbArchiveTool.Application/"]
COPY ["src/DbArchiveTool.Domain/DbArchiveTool.Domain.csproj", "src/DbArchiveTool.Domain/"]
COPY ["src/DbArchiveTool.Infrastructure/DbArchiveTool.Infrastructure.csproj", "src/DbArchiveTool.Infrastructure/"]
COPY ["src/DbArchiveTool.Shared/DbArchiveTool.Shared.csproj", "src/DbArchiveTool.Shared/"]

RUN dotnet restore "src/DbArchiveTool.Api/DbArchiveTool.Api.csproj"

# 复制所有源代码
COPY . .

# 构建和发布应用
WORKDIR "/src/src/DbArchiveTool.Api"
RUN dotnet build "DbArchiveTool.Api.csproj" -c Release -o /app/build
RUN dotnet publish "DbArchiveTool.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

# ============ 运行时阶段 ============
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# 安装 SQL Server 命令行工具（BCP）
RUN apt-get update \
    && apt-get install -y curl gnupg2 apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev \
    && echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 添加 BCP 工具到 PATH
ENV PATH="${PATH}:/opt/mssql-tools18/bin"

# 创建应用目录
RUN mkdir -p /app/logs /app/temp /app/data

# 从构建阶段复制发布文件
COPY --from=build /app/publish .

# 配置环境变量
ENV ASPNETCORE_ENVIRONMENT=Production \
    ASPNETCORE_URLS=http://+:5001 \
    TZ=Asia/Shanghai

# 暴露端口
EXPOSE 5001

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:5001/api/v1/health || exit 1

# 启动应用
ENTRYPOINT ["dotnet", "DbArchiveTool.Api.dll"]
