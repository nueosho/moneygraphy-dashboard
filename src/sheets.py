"""
Google Sheets 연동 모듈
- 수집된 원본 데이터를 스프레드시트에 자동 업데이트
- 시트 구성: 채널통계, 영상통계, 프로그램분석
"""
import os
import logging
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials

from src import db

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_CHANNEL_STATS = "채널통계"
SHEET_VIDEO_STATS = "영상통계"
SHEET_PROGRAM_STATS = "프로그램분석"


def get_gc() -> gspread.Client:
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_sheet(spreadsheet, title: str, headers: list[str]) -> gspread.Worksheet:
    """시트가 없으면 생성하고 헤더 설정"""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=5000, cols=len(headers))
        ws.append_row(headers, value_input_option="RAW")
    return ws


def sync_channel_stats(spreadsheet):
    headers = ["날짜", "채널ID", "채널명", "구독자수", "총조회수", "영상수"]
    ws = ensure_sheet(spreadsheet, SHEET_CHANNEL_STATS, headers)

    channels = db.get_all_channels()
    rows = []
    for ch in channels:
        history = db.get_channel_stats_history(ch["channel_id"], days=90)
        for stat in history:
            rows.append([
                stat["collected_date"],
                ch["channel_id"],
                ch["name"],
                stat["subscriber_count"] or 0,
                stat["view_count"] or 0,
                stat["video_count"] or 0,
            ])

    if rows:
        ws.clear()
        ws.append_row(headers, value_input_option="RAW")
        ws.append_rows(rows, value_input_option="RAW")
        logger.info("채널통계 시트 업데이트: %d행", len(rows))


def sync_video_stats(spreadsheet):
    headers = [
        "날짜", "영상ID", "채널명", "제목", "게시일",
        "프로그램명", "에피소드", "조회수", "좋아요", "댓글수"
    ]
    ws = ensure_sheet(spreadsheet, SHEET_VIDEO_STATS, headers)

    channels = db.get_all_channels()
    channel_map = {ch["channel_id"]: ch["name"] for ch in channels}

    target_videos = db.get_all_target_videos()
    rows = []
    for v in target_videos:
        trend = db.get_video_stats_trend(v["video_id"], days=30)
        for stat in trend:
            rows.append([
                stat["collected_date"],
                v["video_id"],
                channel_map.get(v["channel_id"], ""),
                v["title"],
                v["published_at"],
                v.get("program_name") or "",
                v.get("episode_number") or "",
                stat["view_count"] or 0,
                stat["like_count"] or 0,
                stat["comment_count"] or 0,
            ])

    if rows:
        ws.clear()
        ws.append_row(headers, value_input_option="RAW")
        ws.append_rows(rows, value_input_option="RAW")
        logger.info("영상통계 시트 업데이트: %d행", len(rows))


def sync_all():
    sheets_id = os.environ.get("GOOGLE_SHEETS_ID")
    if not sheets_id:
        logger.warning("GOOGLE_SHEETS_ID 미설정, Google Sheets 동기화 건너뜀")
        return

    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    if not os.path.exists(sa_path):
        logger.warning("서비스 계정 파일 없음 (%s), 동기화 건너뜀", sa_path)
        return

    try:
        gc = get_gc()
        spreadsheet = gc.open_by_key(sheets_id)
        logger.info("Google Sheets 동기화 시작: %s", sheets_id)
        sync_channel_stats(spreadsheet)
        sync_video_stats(spreadsheet)
        logger.info("Google Sheets 동기화 완료")
    except Exception as e:
        logger.error("Google Sheets 동기화 실패: %s", e)
