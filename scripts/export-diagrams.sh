#!/usr/bin/env bash
# Exports every docs/diagrams/*.drawio to an editable *.drawio.svg next to
# it. The SVG carries the diagram source, so it opens in draw.io as is, and
# it renders on GitHub. Pass file names to export only those.
#
# Needs the draw.io desktop app. Set DRAWIO if it is not in the default
# macOS location.
set -euo pipefail

drawio=${DRAWIO:-/Applications/draw.io.app/Contents/MacOS/draw.io}
cd "$(dirname "$0")/../docs/diagrams"

if [ $# -eq 0 ]; then
  set -- *.drawio
fi

for src in "$@"; do
  out="${src%.drawio}.drawio.svg"
  xmllint --noout "$src"
  "$drawio" --export --format svg --embed-diagram --svg-theme light --output "$out" "$src"
  # draw.io adds a raster copy of every label as a fallback for viewers
  # without foreignObject support. Browsers do not need it, and it makes
  # up most of the file.
  python3 - "$out" <<'EOF'
import re, sys
path = sys.argv[1]
svg = open(path, encoding="utf-8").read()
svg = re.sub(r'(</foreignObject>)<image [^>]*?/>', r'\1', svg)
open(path, "w", encoding="utf-8").write(svg)
EOF
  echo "$out"
done
