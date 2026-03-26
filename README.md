# 유튜브 채널 분석 대시보드

머니그라피 및 경쟁 채널 8개의 YouTube 데이터를 매일 수집하여 GitHub Pages 대시보드로 시각화합니다.

## 기능

| 탭 | 내용 |
|---|---|
| 콘텐츠 분석 | 영상별 조회수/좋아요/댓글 7일 추이, 업로드 요일 히트맵, 아웃라이어 자동 표시 |
| 프로그램 분석 | 제목 패턴으로 자동 분류, 방영중/종영 구분, 에피소드 간 증감 비교 |
| 채널 성장 | 구독자 일별 증감, 급증일과 영상 연결 |
| 경쟁 채널 비교 | 주간 성장률 랭킹, 영상당 평균 조회수, 업로드 빈도 |

## 설치

```bash
# 1. 가상환경 생성 및 의존성 설치
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. 환경변수 설정
cp .env.example .env
# .env 파일에 API 키 입력
```

## 환경변수 설정 (`.env`)

```
YOUTUBE_API_KEY=...          # Google Cloud Console에서 발급
GOOGLE_SHEETS_ID=...         # 스프레드시트 URL의 /d/와 /edit 사이 값
GOOGLE_SERVICE_ACCOUNT_JSON=service_account.json
```

### YouTube API 키 발급

1. [Google Cloud Console](https://console.cloud.google.com/) 접속
2. 새 프로젝트 생성 → **YouTube Data API v3** 활성화
3. 사용자 인증 정보 → **API 키** 생성

### Google Sheets 서비스 계정 설정

1. Google Cloud Console → IAM → 서비스 계정 생성
2. JSON 키 다운로드 → `service_account.json`으로 저장 (프로젝트 루트)
3. 해당 스프레드시트를 서비스 계정 이메일에 편집자 권한으로 공유

## 실행

```bash
# 수동 실행
python main.py

# 결과 확인: docs/data/dashboard.json 생성됨
```

## Mac 자동 실행 (launchd)

매일 오전 9시에 자동으로 데이터를 수집하고 대시보드를 업데이트합니다.

```bash
# plist 파일을 LaunchAgents에 복사
cp com.youtube.dashboard.plist ~/Library/LaunchAgents/

# 등록 및 시작
launchctl load ~/Library/LaunchAgents/com.youtube.dashboard.plist

# 즉시 테스트 실행
launchctl start com.youtube.dashboard

# 제거
launchctl unload ~/Library/LaunchAgents/com.youtube.dashboard.plist
```

## GitHub Pages 배포

```bash
# docs/ 폴더를 GitHub Pages 소스로 설정
# Settings → Pages → Source: /docs

# 데이터 업데이트 후 커밋/푸시
git add docs/data/dashboard.json
git commit -m "데이터 업데이트 $(date +%Y-%m-%d)"
git push
```

> **팁**: GitHub Actions를 사용하면 서버 없이 자동 배포도 가능합니다.

## 프로그램 자동 분류 패턴

영상 제목에서 `| 프로그램명` 또는 `| 프로그램명 숫자` 패턴을 자동 감지합니다.

예시:
- `오늘의 경제 뉴스 | B주류경제학 3` → 프로그램: **B주류경제학**, 에피소드: **3**
- `특별 게스트 | B주류초대석` → 프로그램: **B주류초대석**

## 프로젝트 구조

```
.
├── src/
│   ├── db.py           # SQLite 데이터베이스 조작
│   ├── collect.py      # YouTube Data API v3 수집
│   ├── analyze.py      # 데이터 분석 (아웃라이어, 프로그램 통계 등)
│   ├── sheets.py       # Google Sheets 동기화
│   └── generate.py     # 대시보드용 JSON 생성
├── docs/
│   ├── index.html      # GitHub Pages 대시보드 (Chart.js)
│   └── data/
│       └── dashboard.json   # 생성된 데이터 (자동 업데이트)
├── data/
│   ├── youtube.db      # SQLite DB (누적 저장)
│   └── run.log         # 실행 로그
├── scripts/
│   └── run.sh          # launchd 실행 스크립트
├── main.py             # 메인 진입점
├── com.youtube.dashboard.plist  # Mac launchd 설정
├── requirements.txt
└── .env.example
```
