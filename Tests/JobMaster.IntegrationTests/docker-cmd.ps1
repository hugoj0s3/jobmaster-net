#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

Write-Host "Resetting JobMaster test containers..."

$containers = @("jobmaster-sqlserver", "jobmaster-pg", "jobmaster-mysql", "nats-js")
$namedVolumes = @("nats-js")

# Stop and remove containers (with anonymous volumes)
foreach ($c in $containers) {
    Write-Host "Removing (if exists): $c"
    $id = docker ps -aq -f "name=^${c}$"
    if ($id) {
        docker rm -f -v $id 2>&1 | Out-Null
    }
}

# Remove named volumes
foreach ($v in $namedVolumes) {
    $volumeExists = docker volume ls --format '{{.Name}}' | Select-String -Pattern "^${v}$" -Quiet
    if ($volumeExists) {
        Write-Host "Removing volume: $v"
        docker volume rm -f $v 2>&1 | Out-Null
    } else {
        Write-Host "Volume not present: $v"
    }
}

Write-Host "Recreating containers..."

# SQL Server
docker run -d `
  --name jobmaster-sqlserver `
  -e 'ACCEPT_EULA=Y' `
  -e 'MSSQL_SA_PASSWORD=Passw0rd!' `
  -p 1433:1433 `
  mcr.microsoft.com/mssql/server:2022-latest

# PostgreSQL
docker run -d `
  --name jobmaster-pg `
  -e POSTGRES_PASSWORD=postgres `
  -e POSTGRES_DB=jobmaster `
  -p 5432:5432 `
  --shm-size=1g `
  --health-cmd='pg_isready -U postgres' `
  --health-interval=10s `
  --health-timeout=5s `
  --health-retries=5 `
  postgres:latest `
  -c max_connections=700

# MySQL
docker run -d `
  --name jobmaster-mysql `
  -p 3306:3306 `
  -e MYSQL_ROOT_PASSWORD=root `
  --health-cmd='mysqladmin ping -h 127.0.0.1 -proot || exit 1' `
  --health-interval=10s `
  --health-timeout=5s `
  --health-retries=5 `
  mysql:8.0 `
  --default-authentication-plugin=mysql_native_password `
  --max-connections=700

# NATS JetStream (with named volume)
docker run -d `
  --name nats-js `
  --cpus="2.0" `
  --memory="2g" `
  -p 4222:4222 -p 6222:6222 -p 8222:8222 `
  -v nats-js:/data `
  nats:2.10 `
  -js -sd /data -m 8222 --user jmuser --pass jmpass -DV

Write-Host "All containers recreated successfully."
