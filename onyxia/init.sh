#!/usr/bin/env bash
# Script d'initialisation Onyxia : telecharge run.sh et le lance en arriere-plan (ne bloque pas le demarrage du service).
BRANCH="${TP_BRANCH:-claude/epic-wright-po7bx7}"
D="${HOME:-/home/onyxia}/work/tp-adaltas"
mkdir -p "$D"
curl -fsSL "https://raw.githubusercontent.com/ruben-ctm/ece-bigdata-2026-fall/$BRANCH/onyxia/run.sh" -o "$D/run.sh" || exit 0
TP_BRANCH="$BRANCH" nohup setsid bash "$D/run.sh" >"$D/run.out" 2>&1 &
exit 0
