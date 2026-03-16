$ErrorActionPreference = "Stop"

$deckDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pptPath = (
  Get-ChildItem $deckDir -Filter *.pptx |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1 -ExpandProperty FullName
)
if (-not $pptPath) {
  throw "No pptx found in deck directory."
}
$outDir = Join-Path $deckDir "rendered"
$tempRoot = Join-Path $env:TEMP "pa-pattern-quant-23a-flag-render"
$tempPptPath = Join-Path $tempRoot "deck.pptx"
$tempOutDir = Join-Path $tempRoot "rendered"

if (Test-Path $outDir) {
  Remove-Item $outDir -Recurse -Force
}
New-Item -ItemType Directory -Path $outDir | Out-Null

if (Test-Path $tempRoot) {
  Remove-Item $tempRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $tempOutDir -Force | Out-Null
Copy-Item $pptPath $tempPptPath -Force
if (-not (Test-Path $tempPptPath)) {
  throw "Temp deck copy failed: $tempPptPath"
}
$tempPptPath = (Resolve-Path $tempPptPath).Path

$powerpoint = New-Object -ComObject PowerPoint.Application
$powerpoint.Visible = -1
$presentation = $null

try {
  $presentation = $powerpoint.Presentations.Open($tempPptPath, 0, 0, 0)
  foreach ($slide in $presentation.Slides) {
    $filename = Join-Path $tempOutDir ("slide-" + $slide.SlideIndex + ".png")
    $slide.Export($filename, "PNG", 1600, 900)
  }
} finally {
  if ($presentation -ne $null) {
    $presentation.Close()
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($presentation) | Out-Null
  }
  if ($powerpoint -ne $null) {
    $powerpoint.Quit()
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($powerpoint) | Out-Null
  }
  [GC]::Collect()
  [GC]::WaitForPendingFinalizers()
}

Copy-Item (Join-Path $tempOutDir "*.png") $outDir -Force
