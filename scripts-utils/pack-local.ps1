$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -Path "$ScriptDir\.."
 
$OutputDir = "./nupkgs"
 
if (Test-Path $OutputDir) {
    Remove-Item -Recurse -Force $OutputDir
}
New-Item -ItemType Directory -Path $OutputDir | Out-Null
 
# Clear stale JobMaster packages from global NuGet cache so downstream projects
# pick up the freshly built packages rather than a cached older build.
$nugetCache = "$env:USERPROFILE\.nuget\packages"
@("jobmaster", "jobmaster.api") | ForEach-Object {
    $p = Join-Path $nugetCache $_
    if (Test-Path $p) { Remove-Item -Recurse -Force $p }
}

dotnet pack JobMaster/JobMaster.csproj                                   -c Release -p:UseProjectRefs=false -o $OutputDir
dotnet pack JobMaster.Api/JobMaster.Api.csproj                           -c Release -p:UseProjectRefs=false -o $OutputDir
dotnet pack JobMaster.NatsJetStream/JobMaster.NatsJetStream.csproj       -c Release -p:UseProjectRefs=false -o $OutputDir
dotnet pack Sql/JobMaster.SqlBase/JobMaster.SqlBase.csproj               -c Release -p:UseProjectRefs=false -o $OutputDir
dotnet pack Sql/JobMaster.Postgres/JobMaster.Postgres.csproj             -c Release -p:UseProjectRefs=false -o $OutputDir
dotnet pack Sql/JobMaster.MySql/JobMaster.MySql.csproj                   -c Release -p:UseProjectRefs=false -o $OutputDir
dotnet pack Sql/JobMaster.SqlServer/JobMaster.SqlServer.csproj           -c Release -p:UseProjectRefs=false -o $OutputDir
 
Write-Host ""
Write-Host "Packages generated in $OutputDir`:"
Get-ChildItem "$OutputDir\*.nupkg" | Select-Object -ExpandProperty Name