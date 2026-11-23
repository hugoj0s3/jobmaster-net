FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["tests/TargetTestRecurringApp/TargetTestRecurringApp.csproj", "tests/TargetTestRecurringApp/"]
COPY ["src/JobMaster/JobMaster.csproj", "src/JobMaster/"]
COPY ["src/StorageProviders/JobMaster.SqlBase/JobMaster.SqlBase.csproj", "src/StorageProviders/JobMaster.SqlBase/"]
COPY ["src/StorageProviders/JobMaster.Postgres/JobMaster.Postgres.csproj", "src/StorageProviders/JobMaster.Postgres/"]
COPY ["src/StorageProviders/JobMaster.MySql/JobMaster.MySql.csproj", "src/StorageProviders/JobMaster.MySql/"]
COPY ["src/StorageProviders/JobMaster.SqlServer/JobMaster.SqlServer.csproj", "src/StorageProviders/JobMaster.SqlServer/"]
COPY ["src/StorageProviders/JobMaster.NatsJetStream/JobMaster.NatsJetStream.csproj", "src/StorageProviders/JobMaster.NatsJetStream/"]
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
RUN dotnet restore "tests/TargetTestRecurringApp/TargetTestRecurringApp.csproj"
COPY . .
WORKDIR "/src/tests/TargetTestRecurringApp"
RUN dotnet build "TargetTestRecurringApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "TargetTestRecurringApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "TargetTestRecurringApp.dll"]
