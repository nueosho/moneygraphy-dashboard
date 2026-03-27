#!/bin/bash
# ─────────────────────────────────────────────
# 유튜브 채널 분석 대시보드 자동 실행 스크립트
# Mac launchd (com.youtube.dashboard.plist)에서 호출
# ─────────────────────────────────────────────

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

# 가상환경 활성화 (venv가 있으면 사용)
if [ -f "$PROJECT_DIR/venv/bin/activate" ]; then
  source "$PROJECT_DIR/venv/bin/activate"
fi

# .env 파일 로드
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  source "$PROJECT_DIR/.env"
  set +a
fi

LOG_FILE="$PROJECT_DIR/data/run.log"
mkdir -p "$PROJECT_DIR/data"

echo "──────────────────────────────────" >> "$LOG_FILE"
echo "실행 시각: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

python3 main.py >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  echo "오류 발생 (exit code: $EXIT_CODE)" >> "$LOG_FILE"
else
  echo "완료" >> "$LOG_FILE"

  # GitHub Pages 자동 배포
  cd "$PROJECT_DIR"
  git add docs/data/dashboard.json
  if ! git diff --cached --quiet; then
    git commit -m "auto: 데이터 업데이트 $(date '+%Y-%m-%d')"
    git push github main >> "$LOG_FILE" 2>&1
    echo "GitHub 배포 완료" >> "$LOG_FILE"
  else
    echo "변경사항 없음, 배포 스킵" >> "$LOG_FILE"
  fi
fi
