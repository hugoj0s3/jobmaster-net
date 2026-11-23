FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
# false = restore JobMaster/*.SqlBase/etc from nuget.org (the published package) instead of the
# copied local source -- lets a benchmark run exercise the real published package end to end.
ARG USE_PROJECT_REFS=false
WORKDIR /src
COPY ["misc/Benchmarks/JobMaster.Benchmarks.JobMasterHost/JobMaster.Benchmarks.JobMasterHost.csproj", "misc/Benchmarks/JobMaster.Benchmarks.JobMasterHost/"]
COPY ["misc/Benchmarks/JobMaster.Benchmarks.Common/JobMaster.Benchmarks.Common.csproj", "misc/Benchmarks/JobMaster.Benchmarks.Common/"]
COPY ["src/JobMaster/JobMaster.csproj", "src/JobMaster/"]
COPY ["src/StorageProviders/JobMaster.SqlBase/JobMaster.SqlBase.csproj", "src/StorageProviders/JobMaster.SqlBase/"]
COPY ["src/StorageProviders/JobMaster.Postgres/JobMaster.Postgres.csproj", "src/StorageProviders/JobMaster.Postgres/"]
COPY ["src/StorageProviders/JobMaster.MySql/JobMaster.MySql.csproj", "src/StorageProviders/JobMaster.MySql/"]
COPY ["src/StorageProviders/JobMaster.SqlServer/JobMaster.SqlServer.csproj", "src/StorageProviders/JobMaster.SqlServer/"]
COPY ["src/StorageProviders/JobMaster.NatsJetStream/JobMaster.NatsJetStream.csproj", "src/StorageProviders/JobMaster.NatsJetStream/"]
COPY ["src/StorageProviders/JobMaster.RavenDB/JobMaster.RavenDB.csproj", "src/StorageProviders/JobMaster.RavenDB/"]
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
RUN dotnet restore "misc/Benchmarks/JobMaster.Benchmarks.JobMasterHost/JobMaster.Benchmarks.JobMasterHost.csproj" -p:UseProjectRefs=$USE_PROJECT_REFS
COPY . .
WORKDIR "/src/misc/Benchmarks/JobMaster.Benchmarks.JobMasterHost"
RUN dotnet build "JobMaster.Benchmarks.JobMasterHost.csproj" -c $BUILD_CONFIGURATION -o /app/build -p:UseProjectRefs=$USE_PROJECT_REFS

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
ARG USE_PROJECT_REFS=false
RUN dotnet publish "JobMaster.Benchmarks.JobMasterHost.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false -p:UseProjectRefs=$USE_PROJECT_REFS

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "JobMaster.Benchmarks.JobMasterHost.dll"]
