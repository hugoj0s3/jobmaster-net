FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Benchmarks/JobMaster.Benchmarks.JobMasterHost/JobMaster.Benchmarks.JobMasterHost.csproj", "Benchmarks/JobMaster.Benchmarks.JobMasterHost/"]
COPY ["Benchmarks/JobMaster.Benchmarks.Common/JobMaster.Benchmarks.Common.csproj", "Benchmarks/JobMaster.Benchmarks.Common/"]
COPY ["JobMaster/JobMaster.csproj", "JobMaster/"]
COPY ["Sql/JobMaster.SqlBase/JobMaster.SqlBase.csproj", "Sql/JobMaster.SqlBase/"]
COPY ["Sql/JobMaster.Postgres/JobMaster.Postgres.csproj", "Sql/JobMaster.Postgres/"]
COPY ["Sql/JobMaster.MySql/JobMaster.MySql.csproj", "Sql/JobMaster.MySql/"]
COPY ["Sql/JobMaster.SqlServer/JobMaster.SqlServer.csproj", "Sql/JobMaster.SqlServer/"]
COPY ["JobMaster.NatsJetStream/JobMaster.NatsJetStream.csproj", "JobMaster.NatsJetStream/"]
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
RUN dotnet restore "Benchmarks/JobMaster.Benchmarks.JobMasterHost/JobMaster.Benchmarks.JobMasterHost.csproj"
COPY . .
WORKDIR "/src/Benchmarks/JobMaster.Benchmarks.JobMasterHost"
RUN dotnet build "JobMaster.Benchmarks.JobMasterHost.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "JobMaster.Benchmarks.JobMasterHost.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "JobMaster.Benchmarks.JobMasterHost.dll"]
