param(
    [string]$Root = ".",
    [string]$OutFile = "flat-export.txt"
)

$ErrorActionPreference = "Stop"

# Files to always include (relative to $Root)
$fixedFiles = @(
    "src\app.html",
    "src\app.css",
    "src\lib\theme-helper.ts",
    "tailwind.config.js",
    "static\jobmaster-config.json"
)

# Collect all .svelte excluding node_modules
$svelteFiles = Get-ChildItem -Path $Root -Recurse -File -Filter "*.svelte" |
        Where-Object { $_.FullName -notmatch "\\node_modules\\" } |
        Sort-Object FullName

# Collect fixed files (only if they exist)
$resolvedFixed = foreach ($rel in $fixedFiles) {
    $p = Join-Path $Root $rel
    if (Test-Path $p) { Get-Item $p }
}

# Merge (unique by full path) and sort
$allFiles = @($svelteFiles + $resolvedFixed) |
        Group-Object FullName |
        ForEach-Object { $_.Group[0] } |
        Sort-Object FullName

# Write output
"" | Out-File -FilePath $OutFile -Encoding UTF8

foreach ($f in $allFiles) {
    $relative = Resolve-Path -LiteralPath $f.FullName | ForEach-Object {
        $_.Path.Substring((Resolve-Path $Root).Path.Length).TrimStart("\","/")
    }

    "===== FILE: $relative =====" | Out-File -FilePath $OutFile -Append -Encoding UTF8
    Get-Content -LiteralPath $f.FullName -Raw | Out-File -FilePath $OutFile -Append -Encoding UTF8
    "`r`n" | Out-File -FilePath $OutFile -Append -Encoding UTF8
}

Write-Host "Wrote $($allFiles.Count) files to $OutFile"