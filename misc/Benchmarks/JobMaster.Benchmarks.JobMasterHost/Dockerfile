FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
# false = restore JobMaster/*.SqlBase/etc from nuget.org (the published package) instead of the
# copied local source -- lets a benchmark run exercise the real published package end to end.
ARG USE_PROJECT_REFS=false
WORKDIR /src
COPY ["misc/Benchmarks/JobMaster.Benchmarks.JobMasterHost/JobMaster.Benchmarks.JobMasterHost.csproj", "misc/Benchmarks/JobMaster.Benchmarks.JobMasterHost/"]
COPY ["misc/Benchmarks/JobMaster.Benchmarks.Common/JobMaster.Benchmarks.Common.csproj", "misc/Benchmarks/JobMaster.Benchmarks.Common/"]
COPY ["JobMaster/JobMaster.csproj", "JobMaster/"]
COPY ["Providers/JobMaster.SqlBase/JobMaster.SqlBase.csproj", "Providers/JobMaster.SqlBase/"]
COPY ["Providers/JobMaster.Postgres/JobMaster.Postgres.csproj", "Providers/JobMaster.Postgres/"]
COPY ["Providers/JobMaster.MySql/JobMaster.MySql.csproj", "Providers/JobMaster.MySql/"]
COPY ["Providers/JobMaster.SqlServer/JobMaster.SqlServer.csproj", "Providers/JobMaster.SqlServer/"]
COPY ["Providers/JobMaster.NatsJetStream/JobMaster.NatsJetStream.csproj", "Providers/JobMaster.NatsJetStream/"]
COPY ["Providers/JobMaster.RavenDb/JobMaster.RavenDb.csproj", "Providers/JobMaster.RavenDb/"]
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
