#!/usr/bin/env python3
"""
유튜브 채널 분석 대시보드 - 메인 실행 스크립트
매일 오전 9시 Mac launchd로 자동 실행
"""
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "data" / "run.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def save_weekly_snapshot_if_thursday(db):
    """오늘이 목요일이면 이번 주 스냅샷 저장 (목~다음목 기준)"""
    from datetime import datetime, timezone, timedelta
    KST = timezone(timedelta(hours=9))
    today = datetime.now(KST)
    if today.weekday() != 3:  # 3 = 목요일
        return
    week_start = today.strftime("%Y-%m-%d")
    week_end = (today + timedelta(days=7)).strftime("%Y-%m-%d")
    month = today.strftime("%Y-%m")

    TARGET = "UCwXOKS-z1t9u6Axmm3blXug"
    latest = db.get_latest_channel_stats(TARGET)
    sub_count = latest.get("subscriber_count", 0) or 0

    # 이번 주(목~목) 업로드된 영상 통계
    import sqlite3
    from pathlib import Path
    DB_PATH = Path(__file__).parent / "data" / "youtube.db"
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    prev_thu = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    cur.execute(
        """
        SELECT SUM(vs.view_count) as total_views, SUM(vs.comment_count) as total_comments,
               SUM(CASE WHEN v.is_short=0 THEN 1 ELSE 0 END) as lf,
               SUM(CASE WHEN v.is_short=1 THEN 1 ELSE 0 END) as sf
        FROM videos v
        LEFT JOIN video_stats vs ON v.video_id=vs.video_id
            AND vs.collected_date=(SELECT MAX(collected_date) FROM video_stats WHERE video_id=v.video_id)
        WHERE v.channel_id=? AND v.published_at >= ? AND v.published_at < ?
        """,
        (TARGET, prev_thu, week_start)
    )
    row = cur.fetchone()
    conn.close()

    total_views = row["total_views"] or 0 if row else 0
    total_comments = row["total_comments"] or 0 if row else 0
    lf = row["lf"] or 0 if row else 0
    sf = row["sf"] or 0 if row else 0

    db.save_weekly_snapshot(TARGET, week_start, week_end, sub_count,
                            total_views, total_comments, lf, sf)
    logger.info("주간 스냅샷 저장: %s ~ %s", week_start, week_end)

    # 한달 기준: 이번 달 첫 목요일이면 지난달 월간 스냅샷도 저장
    # (매월 첫 목요일에 전월 합산)
    if today.day <= 7:  # 이달의 첫 번째 주 목요일
        last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        cur2 = sqlite3.connect(str(DB_PATH))
        cur2.row_factory = sqlite3.Row
        c2 = cur2.cursor()
        c2.execute(
            """
            SELECT SUM(vs.view_count) as tv, SUM(vs.comment_count) as tc,
                   SUM(CASE WHEN v.is_short=0 THEN 1 ELSE 0 END) as lf,
                   SUM(CASE WHEN v.is_short=1 THEN 1 ELSE 0 END) as sf
            FROM videos v
            LEFT JOIN video_stats vs ON v.video_id=vs.video_id
                AND vs.collected_date=(SELECT MAX(collected_date) FROM video_stats WHERE video_id=v.video_id)
            WHERE v.channel_id=? AND strftime('%Y-%m', v.published_at)=?
            """,
            (TARGET, last_month)
        )
        mr = c2.fetchone()
        cur2.close()
        if mr:
            # 지난달 말 구독자 수는 weekly_snapshots에서 마지막 기록 사용
            snapshots = db.get_all_weekly_snapshots(TARGET)
            last_sub = snapshots[-1]["subscriber_count"] if snapshots else sub_count
            db.save_monthly_snapshot(TARGET, last_month, last_sub,
                                     mr["tv"] or 0, mr["tc"] or 0,
                                     mr["lf"] or 0, mr["sf"] or 0)
            logger.info("월간 스냅샷 저장: %s", last_month)


def save_daily_record_today(db):
    from datetime import datetime, timezone, timedelta
    import sqlite3
    from pathlib import Path

    KST = timezone(timedelta(hours=9))
    kst_today = datetime.now(KST).strftime("%Y-%m-%d")
    TARGET = "UCwXOKS-z1t9u6Axmm3blXug"

    latest = db.get_latest_channel_stats(TARGET)
    sub_count = latest.get("subscriber_count", 0) or 0
    # 실제 수집된 날짜 (DB 기준 최신 collected_date 사용)
    collect_date = latest.get("collected_date", kst_today)

    DB_PATH = Path(__file__).parent / "data" / "youtube.db"
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 가장 최근 수집된 video_stats 날짜로 집계
    cur.execute("""
        SELECT
            SUM(vs.view_count) as tv, SUM(vs.comment_count) as tc, SUM(vs.like_count) as tl,
            SUM(CASE WHEN v.is_short=0 THEN vs.view_count ELSE 0 END) as lv,
            SUM(CASE WHEN v.is_short=0 THEN vs.comment_count ELSE 0 END) as lc,
            SUM(CASE WHEN v.is_short=0 THEN vs.like_count ELSE 0 END) as ll,
            SUM(CASE WHEN v.is_short=1 THEN vs.view_count ELSE 0 END) as sv,
            SUM(CASE WHEN v.is_short=1 THEN vs.comment_count ELSE 0 END) as sc,
            SUM(CASE WHEN v.is_short=1 THEN vs.like_count ELSE 0 END) as sl
        FROM video_stats vs
        JOIN videos v ON v.video_id = vs.video_id
        WHERE v.channel_id=? AND vs.collected_date=?
    """, (TARGET, collect_date))
    r = cur.fetchone()

    # 오늘 날짜에 업로드된 영상 수
    cur.execute("""
        SELECT
            SUM(CASE WHEN is_short=0 THEN 1 ELSE 0 END) as lf_cnt,
            SUM(CASE WHEN is_short=1 THEN 1 ELSE 0 END) as sf_cnt
        FROM videos WHERE channel_id=? AND published_at=?
    """, (TARGET, kst_today))
    u = cur.fetchone()
    conn.close()

    if r:
        db.save_daily_record(
            TARGET, kst_today, sub_count,
            r['tv'] or 0, r['tc'] or 0, r['tl'] or 0,
            r['lv'] or 0, r['lc'] or 0, r['ll'] or 0,
            r['sv'] or 0, r['sc'] or 0, r['sl'] or 0,
            u['lf_cnt'] or 0 if u else 0,
            u['sf_cnt'] or 0 if u else 0
        )
        logger.info("일별 기록 저장: %s", kst_today)


def main():
    from src import db
    from src.collect import collect_all
    from src.sheets import sync_all
    from src.generate import generate

    logger.info("=== 유튜브 채널 분석 시작 ===")

    # 1. DB 초기화
    logger.info("[1/4] 데이터베이스 초기화...")
    db.init_db()

    # 2. YouTube API로 데이터 수집
    logger.info("[2/4] YouTube 데이터 수집...")
    try:
        collect_all()
    except Exception as e:
        logger.error("데이터 수집 실패: %s", e)
        sys.exit(1)

    # 2-1. 목요일이면 주간 스냅샷 저장
    try:
        save_weekly_snapshot_if_thursday(db)
    except Exception as e:
        logger.warning("주간 스냅샷 저장 실패 (무시): %s", e)

    # 2-2. 일별 기록 저장
    try:
        save_daily_record_today(db)
    except Exception as e:
        logger.warning("일별 기록 저장 실패 (무시): %s", e)

    # 3. Google Sheets 동기화
    logger.info("[3/4] Google Sheets 동기화...")
    try:
        sync_all()
    except Exception as e:
        logger.warning("Google Sheets 동기화 실패 (무시): %s", e)

    # 4. 대시보드 JSON 생성
    logger.info("[4/4] 대시보드 데이터 생성...")
    try:
        generate()
    except Exception as e:
        logger.error("대시보드 생성 실패: %s", e)
        sys.exit(1)

    logger.info("=== 완료! ===")


if __name__ == "__main__":
    main()
