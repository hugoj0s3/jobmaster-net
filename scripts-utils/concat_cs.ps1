[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$RootDir = ".",

    [Parameter(Position = 1)]
    [string]$OutFile = "all_cs_concat.txt"
)

$ErrorActionPreference = "Stop"

$excludedDirNames = @(
    ".git",
    "bin",
    "obj",
    "packages",
    "node_modules",
    ".vs",
    ".idea"
)

function Test-IsExcludedPath {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    foreach ($dirName in $excludedDirNames) {
        if ($Path -match [regex]::Escape([IO.Path]::DirectorySeparatorChar + $dirName + [IO.Path]::DirectorySeparatorChar) -or
            $Path -match [regex]::Escape([IO.Path]::AltDirectorySeparatorChar + $dirName + [IO.Path]::AltDirectorySeparatorChar)) {
            return $true
        }
    }

    return $false
}

$rootFullPath = (Resolve-Path -LiteralPath $RootDir).Path

if (-not [IO.Path]::IsPathRooted($OutFile)) {
    $OutFile = Join-Path -Path $PSScriptRoot -ChildPath $OutFile
}

$outDir = Split-Path -Parent $OutFile
if (-not [string]::IsNullOrWhiteSpace($outDir)) {
    if (-not (Test-Path -LiteralPath $outDir)) {
        New-Item -ItemType Directory -Path $outDir | Out-Null
    }
}

# Gather files (.cs and .csproj), exclude common build/artifact directories, sort deterministically
$files = Get-ChildItem -LiteralPath $rootFullPath -Recurse -File |
    Where-Object {
        ($_.Extension -eq ".cs" -or $_.Extension -eq ".csproj") -and
        -not (Test-IsExcludedPath -Path $_.FullName)
    } |
    Sort-Object -Property FullName

# Write output
$writer = New-Object System.IO.StreamWriter($OutFile, $false, [System.Text.Encoding]::UTF8)
try {
    foreach ($f in $files) {
        $writer.WriteLine("===== FILE: {0} =====" -f $f.FullName)

        # Read raw to preserve file contents as-is
        $content = Get-Content -LiteralPath $f.FullName -Raw
        $writer.Write($content)

        # Ensure a trailing newline between files
        if (-not $content.EndsWith("`n")) {
            $writer.WriteLine()
        }
        $writer.WriteLine()
    }
}
finally {
    $writer.Dispose()
}
