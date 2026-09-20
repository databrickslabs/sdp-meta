#requires -version 5.1
<#
.SYNOPSIS
    Windows-native PowerShell port of scripts/deploy_app.sh -- deploys the
    sdp-meta databricks_app to Databricks Apps.

.DESCRIPTION
    Mirrors the bash script step-for-step so the deploy behaves identically
    on Windows. See scripts/deploy_app.sh for the long-form rationale of
    why staging works the way it does (Mode A entry-point files, runtime
    allow-list, NBSOURCE rename, full-sync wipe). This script intentionally
    keeps the same flag names (lower-case --profile/--path/--app/--mode are
    aliased to the canonical PowerShell -Profile / -Path / -App / -Mode),
    the same env-var fallbacks (canonical $DATABRICKS_PROFILE,
    $WORKSPACE_PATH, $APP_NAME, $DEPLOY_MODE; legacy $PROFILE accepted as
    a fallback for parity with the bash script), the same default app
    name (demo-sdp-meta), and the same SNAPSHOT default deploy mode so a
    user switching between platforms gets the same result from the same
    arguments.

    Why $DatabricksProfile and not $Profile?
    ----------------------------------------
    PowerShell defines $PROFILE as an automatic variable holding the path
    to the user's profile script (e.g. ~/Documents/PowerShell/Microsoft.
    PowerShell_profile.ps1). Reusing the literal name `$Profile` for a
    script parameter shadows the automatic variable inside the script
    body, but the bigger trap is that callers read the docs and write
    `$PROFILE = 'DEFAULT'` (assigning the local PowerShell variable, NOT
    an env var) and then can't figure out why the script aborts with
    "missing --profile". Using a namespaced -DatabricksProfile parameter
    plus $env:DATABRICKS_PROFILE removes the ambiguity. `-Profile` is
    still accepted as an alias so the old invocation keeps working.

    Tooling differences vs the bash script:
      - rsync         -> robocopy (Windows built-in; supports recursive
                        copies with directory/file excludes)
      - cp -p         -> Copy-Item
      - mktemp -d     -> [IO.Path]::GetTempPath() + GetRandomFileName()
      - python3 -c    -> ConvertFrom-Json (PowerShell built-in)
      - trap EXIT     -> try { } finally { }

    Bash returns nonzero on the first failure thanks to `set -euo pipefail`;
    PowerShell achieves the same via $ErrorActionPreference = 'Stop' plus
    explicit $LASTEXITCODE checks after every native command (databricks /
    robocopy) because native exit codes do NOT trip $ErrorActionPreference.

.PARAMETER DatabricksProfile
    Databricks CLI profile. Required. Falls back to $env:DATABRICKS_PROFILE
    then $env:PROFILE (the bash script's env-var name). Aliased to -Profile.

.PARAMETER Path
    Workspace path (e.g. /Workspace/Users/<you>/<app-folder>). Required.
    Falls back to $env:WORKSPACE_PATH.

.PARAMETER App
    App name. Default: 'demo-sdp-meta'. Falls back to $env:APP_NAME.

.PARAMETER Mode
    Databricks Apps deploy mode (SNAPSHOT or AUTO_SYNC). Default: SNAPSHOT.
    Falls back to $env:DEPLOY_MODE.

.EXAMPLE
    PS> .\scripts\deploy_app.ps1 -DatabricksProfile DEFAULT `
                                 -Path /Workspace/Users/me@databricks.com/sdp-meta-app

.EXAMPLE
    PS> # Env-var-driven invocation. Note $env:DATABRICKS_PROFILE, NOT
    PS> # $PROFILE -- the latter is PowerShell's automatic variable for
    PS> # the path to the user's profile script and is not what this
    PS> # script reads. The script accepts $env:PROFILE as a fallback for
    PS> # bash-script parity, but $env:DATABRICKS_PROFILE is the canonical
    PS> # name.
    PS> $env:DATABRICKS_PROFILE = 'DEFAULT'
    PS> $env:WORKSPACE_PATH     = '/Workspace/Users/me@databricks.com/sdp-meta-app'
    PS> .\scripts\deploy_app.ps1

.NOTES
    Requires:
      - Windows 10/11 or Windows Server (robocopy ships in-box)
      - PowerShell 5.1+ (default on Windows 10/11) or PowerShell 7+
      - databricks CLI on PATH
    The repo MUST NOT contain an `app.yaml` at its root (anti-pattern,
    same as the bash script enforces) -- staging auto-generates one.
#>
[CmdletBinding()]
param(
    # -Profile alias keeps the previous public name working without forcing
    # callers to update their muscle memory; the canonical name is now
    # -DatabricksProfile to avoid colliding with PowerShell's built-in
    # automatic $PROFILE variable (see .DESCRIPTION).
    [Alias('Profile')]
    [string]$DatabricksProfile = $(
        if ($env:DATABRICKS_PROFILE) { $env:DATABRICKS_PROFILE }
        elseif ($env:PROFILE)        { $env:PROFILE }
        else                         { '' }
    ),
    [string]$App     = $(if ($env:APP_NAME)     { $env:APP_NAME }     else { 'demo-sdp-meta' }),
    [string]$Path    = $env:WORKSPACE_PATH,
    [string]$Mode    = $(if ($env:DEPLOY_MODE)  { $env:DEPLOY_MODE }  else { 'SNAPSHOT' })
)

# -- PowerShell strictness equivalent of `set -euo pipefail` -----------------
# $ErrorActionPreference = Stop turns ANY non-terminating error from a
# PowerShell cmdlet into a terminating exception that the try/finally
# (and the outer script) propagates upward. Native-binary exit codes still
# need explicit $LASTEXITCODE checks.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# -- Required-argument guard -------------------------------------------------
$missing = @()
if ([string]::IsNullOrWhiteSpace($DatabricksProfile)) {
    $missing += '-DatabricksProfile / $env:DATABRICKS_PROFILE (or legacy $env:PROFILE)'
}
if ([string]::IsNullOrWhiteSpace($Path))    { $missing += '-Path / $env:WORKSPACE_PATH' }
if ($missing.Count -gt 0) {
    Write-Host "Error: missing required argument(s): $($missing -join ', ')" -ForegroundColor Red
    Write-Host "Run 'Get-Help .\scripts\deploy_app.ps1 -Full' for usage." -ForegroundColor Yellow
    exit 2
}

# -- Repo root + cd ----------------------------------------------------------
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -Path $RepoRoot

Write-Host '----------------------------------------------------------------------'
Write-Host " Repo root      : $RepoRoot"
Write-Host " Profile        : $DatabricksProfile"
Write-Host " App name       : $App"
Write-Host " Workspace path : $Path"
Write-Host " Deploy mode    : $Mode"
Write-Host '----------------------------------------------------------------------'

# -- Sanity checks -----------------------------------------------------------
function Assert-Command {
    param([string]$Name, [string]$Hint)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Write-Host "Error: '$Name' not found in PATH. $Hint" -ForegroundColor Red
        exit 1
    }
}

Assert-Command 'databricks' 'Install the Databricks CLI: https://docs.databricks.com/dev-tools/cli/install.html'
# robocopy is the Windows-built-in rsync equivalent. Missing here means the
# script is being run on a non-Windows host (e.g. PowerShell 7 on macOS/Linux);
# in that case the bash script is the right entry point.
Assert-Command 'robocopy' 'This script targets Windows. On macOS/Linux use scripts/deploy_app.sh.'

function Assert-File {
    param([string]$RelPath)
    $abs = Join-Path $RepoRoot $RelPath
    if (-not (Test-Path -LiteralPath $abs -PathType Leaf)) {
        Write-Host "Error: $RelPath not found at $RepoRoot" -ForegroundColor Red
        exit 1
    }
}
function Assert-Dir {
    param([string]$RelPath)
    $abs = Join-Path $RepoRoot $RelPath
    if (-not (Test-Path -LiteralPath $abs -PathType Container)) {
        Write-Host "Error: $RelPath/ must exist at repo root" -ForegroundColor Red
        exit 1
    }
}

Assert-File 'databricks_app/start.sh'
Assert-File 'databricks_app/requirements.txt'
Assert-File 'databricks_app/app.py'
Assert-Dir  'src'
Assert-Dir  'demo'

# A root-level app.yaml means someone manually copied the auto-generated
# staging file back into the working tree. That's an anti-pattern -- same
# rationale as the bash script (deploy_app.sh:115-124).
if (Test-Path -LiteralPath (Join-Path $RepoRoot 'app.yaml') -PathType Leaf) {
    Write-Host 'Error: app.yaml exists at the repo root.' -ForegroundColor Red
    Write-Host '       scripts/deploy_app.ps1 auto-generates app.yaml into a staging' -ForegroundColor Red
    Write-Host '       tempdir; never commit one at the root. Delete it and rerun.' -ForegroundColor Red
    exit 1
}

# -- Stage repo into a temp directory ----------------------------------------
$stagingName = 'dltmeta-deploy.' + [System.IO.Path]::GetRandomFileName()
$Staging     = Join-Path ([System.IO.Path]::GetTempPath()) $stagingName
New-Item -ItemType Directory -Path $Staging -Force | Out-Null

try {
    # See deploy_app.sh:130-145 for the rationale behind the allow-list.
    # Same runtime dirs, same intent: keep the upload to the runtime subset.
    $RuntimeDirs = @(
        'src',                # wheel build source (python setup.py bdist_wheel)
        'examples',           # packaged MCP JSON/YAML examples required by setup.py
        'demo',               # launch_*_demo.py + conf templates + sample data
        'integration_tests',  # imported by every demo launcher
        'databricks_app'      # the Flask app itself
    )

    # Same optional-file list as the bash script (deploy_app.sh:147-159).
    # Optional files that don't exist are silently skipped so a fresh clone
    # with a slightly different layout still deploys.
    $RuntimeFiles = @(
        'setup.py',
        'MANIFEST.in',
        'README.md',
        'CHANGELOG.md',
        'LICENSE.txt',
        'labs.yml',
        '.databricksignore'   # honoured by `databricks sync` inside staging
    )

    # robocopy /XD and /XF accept wildcard patterns. We deliberately match
    # the bash SUBDIR_EXCLUDES set so the staged tree is byte-identical to
    # what rsync would have produced.
    $ExcludeDirs  = @('__pycache__', '*.egg-info', '.databricks', '.venv', 'venv', 'dist', 'build')
    $ExcludeFiles = @('*.pyc', '*.pyo', '.DS_Store')

    Write-Host ">> Staging repo subset to $Staging ..."
    foreach ($d in $RuntimeDirs) {
        $src = Join-Path $RepoRoot $d
        if (-not (Test-Path -LiteralPath $src -PathType Container)) {
            Write-Host "Error: required runtime dir '$d/' missing at repo root" -ForegroundColor Red
            exit 1
        }
        $dst = Join-Path $Staging $d
        New-Item -ItemType Directory -Path $dst -Force | Out-Null

        # robocopy flags:
        #   /E       -- copy subdirs including empty ones (equivalent to rsync -a)
        #   /XD      -- exclude these directory name patterns (recursive)
        #   /XF      -- exclude these file name patterns (recursive)
        #   /NFL /NDL /NJH /NJS /NC /NS /NP  -- quiet output (no file/dir lists,
        #                                       no job header/summary, no class/
        #                                       size/progress columns)
        # Exit codes 0-7 are success; 8+ are real errors. robocopy's "1" means
        # "one or more files copied" -- NOT an error -- so we check >= 8 only.
        $rcArgs = @($src, $dst, '/E')
        $rcArgs += '/XD'; $rcArgs += $ExcludeDirs
        $rcArgs += '/XF'; $rcArgs += $ExcludeFiles
        $rcArgs += @('/NFL', '/NDL', '/NJH', '/NJS', '/NC', '/NS', '/NP')

        & robocopy @rcArgs | Out-Null
        if ($LASTEXITCODE -ge 8) {
            Write-Host "Error: robocopy failed copying $src -> $dst (exit $LASTEXITCODE)" -ForegroundColor Red
            exit 1
        }
    }

    foreach ($f in $RuntimeFiles) {
        $src = Join-Path $RepoRoot $f
        if (Test-Path -LiteralPath $src -PathType Leaf) {
            Copy-Item -LiteralPath $src -Destination (Join-Path $Staging $f) -Force
        }
    }

    # Hard requirements (same as bash deploy_app.sh:193-198). If these went
    # missing from the staged tree, the wheel build at App start will fail.
    foreach ($required in @('setup.py', 'MANIFEST.in')) {
        if (-not (Test-Path -LiteralPath (Join-Path $Staging $required) -PathType Leaf)) {
            Write-Host "Error: $required missing from staged tree -- wheel build will fail" -ForegroundColor Red
            exit 1
        }
    }

    # -- Inject Mode A entry-point files -------------------------------------
    Write-Host '>> Writing Mode A app.yaml + requirements.txt at staging root ...'
    # Write app.yaml explicitly as UTF-8 *without* BOM and with LF line
    # endings. Two reasons not to use Set-Content:
    #   - Set-Content on Windows defaults to the platform line ending (CRLF),
    #     and the Databricks Apps platform parser does not tolerate CRLF in
    #     app.yaml on every release.
    #   - Set-Content -Encoding utf8 on PowerShell 5.1 writes a BOM, which
    #     also breaks some YAML loaders.
    # Building the line array + joining with "`n" + UTF8Encoding($false) is
    # the only combination that produces byte-for-byte identical output to
    # the bash heredoc in deploy_app.sh.
    # `bash -c` form is deliberate: normalizes CRLF -> LF on start.sh in
    # the container BEFORE invoking it. Belt-and-suspenders defense against
    # Windows-origin deploys where the CRLF -> LF pass below (lines ~310-335)
    # didn't run -- e.g. user running an older deploy_app.ps1 missing the
    # normalization block, or a checkout pre-dating .gitattributes with
    # core.autocrlf=true. Without this, line 26 of start.sh (`set -euo
    # pipefail`) crashes with "set: pipefail : invalid option name".
    #
    # YAML SINGLE-quoted scalars are used here. A previous iteration used
    # double-quoted YAML which required '\\r' for the sed regex; some
    # YAML parsers (Apps platform has gone through more than one)
    # interpret '\\r' as backslash+CR (0x0D) instead of backslash+r,
    # silently breaking the regex. Single quotes only need '' -> '
    # escaping, so the bytes that reach sed are exactly what we wrote.
    # `s/\r//g` strips EVERY CR byte (not just trailing) so a file with
    # mid-line CRs is also healed. `printf` is an in-band diagnostic --
    # if the Apps log shows the normalizer message but start.sh STILL
    # errors, the file is unreachable or the source_code path is wrong.
    # `exec` keeps PID 1 stable for the Apps process supervisor.
    $appYamlLines = @(
        '# Auto-generated by scripts/deploy_app.ps1 -- DO NOT COMMIT this file to the',
        '# local repo. The Databricks Apps platform requires app.yaml at the source-',
        '# code-path root; databricks_app/start.sh detects Mode A (full repo) and runs',
        '# the Flask app from the repo root so demo/ and src/ resolve correctly.',
        '#',
        '# The `bash -c` form below normalizes CRLF -> LF on start.sh inside the',
        '# container before invoking it -- belt-and-suspenders defense against',
        '# Windows-origin deploys where the staging normalization was missed.',
        'command:',
        '  - ''bash''',
        '  - ''-c''',
        '  - ''printf ">>> CRLF normalizer: stripping CRs from databricks_app/start.sh\n"; sed -i ''''s/\r//g'''' databricks_app/start.sh; printf ">>> exec start.sh\n"; exec bash databricks_app/start.sh''',
        ''  # trailing newline (matches bash heredoc)
    )
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllBytes(
        (Join-Path $Staging 'app.yaml'),
        $utf8NoBom.GetBytes(($appYamlLines -join "`n"))
    )

    # Use [IO.Path]::Combine for multi-segment joins -- PowerShell 5.1's
    # Join-Path only accepts a single -ChildPath, so embedding a backslash
    # in the child arg ("databricks_app\requirements.txt") technically works
    # on Windows but is inconsistent with Join-Path's intended use. Combine
    # is PS 5.1 / 7+ compatible and platform-correct on both Win and Linux.
    Copy-Item -LiteralPath ([System.IO.Path]::Combine($RepoRoot, 'databricks_app', 'requirements.txt')) `
              -Destination (Join-Path $Staging 'requirements.txt') -Force

    # -- Normalize CRLF -> LF on staged text files ---------------------------
    # On Windows, git's default `core.autocrlf=true` converts LF to CRLF on
    # checkout. Copy-Item preserved those bytes, `databricks sync` would
    # upload them verbatim, and the Linux App container would fail to run
    # `bash start.sh` because every `command\r` is treated as a different
    # command (and the shebang `#!/bin/bash\r` becomes "bad interpreter").
    #
    # Strip all 0x0D bytes from text files we ship. Binary files (e.g. CSVs
    # under demo/resources/data/) are deliberately excluded because their
    # content may legitimately contain CR bytes inside quoted string fields.
    Write-Host '>> Normalizing line endings (CRLF -> LF) on staged text files ...'
    $TextExtensions = @(
        '.sh', '.py', '.yml', '.yaml', '.json',
        '.txt', '.md', '.toml', '.cfg', '.ini', '.in'
    )
    $TextNamesNoExt = @('Dockerfile', 'Makefile', '.databricksignore', '.gitignore')

    $normalizedCount = 0
    Get-ChildItem -LiteralPath $Staging -Recurse -File | Where-Object {
        ($TextExtensions -contains $_.Extension.ToLower()) -or
        ($TextNamesNoExt -contains $_.Name)
    } | ForEach-Object {
        $bytes = [System.IO.File]::ReadAllBytes($_.FullName)
        # Fast scan first; only rewrite when we know there is a CR to strip.
        $hasCR = $false
        for ($i = 0; $i -lt $bytes.Length; $i++) {
            if ($bytes[$i] -eq 13) { $hasCR = $true; break }
        }
        if ($hasCR) {
            $out = New-Object System.Collections.Generic.List[byte] $bytes.Length
            foreach ($b in $bytes) { if ($b -ne 13) { $out.Add($b) } }
            [System.IO.File]::WriteAllBytes($_.FullName, $out.ToArray())
            $normalizedCount++
        }
    }
    Write-Host "   normalized $normalizedCount file(s)"

    # -- Disguise the interactive demo notebook as a regular file ------------
    # Same rationale as deploy_app.sh:215-237. `databricks sync` auto-detects
    # any .py file starting with `# Databricks notebook source` and stores it
    # in the workspace as a NOTEBOOK (extension stripped). The Apps platform
    # then refuses to project NOTEBOOK-typed entries into the container's
    # source_code/ dir, so demo/launch_interactive_demo.py can't find the
    # notebook source. Renaming to .nbsource bypasses the detector; start.sh
    # restores the .py name at boot.
    # See note above about [IO.Path]::Combine vs Join-Path for multi-segment
    # paths under PowerShell 5.1.
    $nbSrc = [System.IO.Path]::Combine($Staging, 'demo', 'SDP_META_INTERACTIVE_DEMO.py')
    if (Test-Path -LiteralPath $nbSrc -PathType Leaf) {
        Write-Host '>> Renaming demo/SDP_META_INTERACTIVE_DEMO.py -> .nbsource (sync-as-FILE workaround) ...'
        Move-Item -LiteralPath $nbSrc -Destination "$nbSrc.nbsource" -Force
    }

    # -- Wipe the workspace path before re-syncing ---------------------------
    # Best-effort: a missing path returns nonzero from `workspace delete`,
    # which is fine. We deliberately swallow errors here (matches the bash
    # script's `|| true`) because the goal is just to ensure a clean dest.
    Write-Host ">> Wiping $Path (clean redeploy) ..."
    & databricks workspace delete $Path --recursive --profile $DatabricksProfile 2>&1 | Out-Null

    # -- Full sync staging -> workspace --------------------------------------
    Write-Host ">> Syncing staging -> $Path (full, --profile $DatabricksProfile) ..."
    & databricks sync $Staging $Path --full --profile $DatabricksProfile
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: databricks sync failed (exit $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }

    # -- Verify app.yaml landed in the workspace -----------------------------
    # Same defensive check as deploy_app.sh:259-272: a stray .gitignore /
    # .databricksignore rule can silently strip app.yaml, leaving the App
    # to boot with no command. Catch it here, not in the Apps log pane.
    Write-Host ">> Verifying app.yaml landed at $Path ..."
    & databricks workspace export "$Path/app.yaml" --profile $DatabricksProfile *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: app.yaml was not synced to $Path/app.yaml." -ForegroundColor Red
        Write-Host "       Most likely cause: a .gitignore rule (or .databricksignore)" -ForegroundColor Red
        Write-Host "       matches 'app.yaml'. Inspect those files and remove the rule." -ForegroundColor Red
        exit 1
    }

    # -- Trigger app deployment ----------------------------------------------
    Write-Host ">> Deploying app '$App' (mode=$Mode) ..."
    & databricks apps deploy $App `
        --source-code-path $Path `
        --profile $DatabricksProfile `
        --mode $Mode
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: databricks apps deploy failed (exit $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }

    # -- Print app URL -------------------------------------------------------
    Write-Host '>> Done. App URL:'
    $appJson = & databricks apps get $App --profile $DatabricksProfile --output json
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Warning: 'databricks apps get' failed (exit $LASTEXITCODE)" -ForegroundColor Yellow
    } else {
        try {
            $parsed = $appJson | ConvertFrom-Json
            # The Apps schema may omit `url` while a deploy is still in flight;
            # fall back to empty string rather than blowing up, matching the
            # bash `dict.get('url', '')` semantics.
            if ($parsed.PSObject.Properties.Match('url').Count -gt 0) {
                Write-Host $parsed.url
            } else {
                Write-Host '(url not yet populated -- re-run `databricks apps get` in a few seconds)'
            }
        } catch {
            Write-Host "Warning: could not parse 'databricks apps get' output: $_" -ForegroundColor Yellow
        }
    }
}
finally {
    # Equivalent of `trap 'rm -rf "$STAGING"' EXIT` in the bash script.
    # Runs on success AND on any thrown exception, so the temp dir never
    # leaks (matters under repeated dev iterations).
    if ($Staging -and (Test-Path -LiteralPath $Staging)) {
        Remove-Item -LiteralPath $Staging -Recurse -Force -ErrorAction SilentlyContinue
    }
}
