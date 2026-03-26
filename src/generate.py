"""
대시보드용 JSON 데이터 생성 모듈
docs/data/dashboard.json 파일로 출력
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from src import db, analyze

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).parent.parent / "docs" / "data" / "dashboard.json"
TARGET_CHANNEL_ID = "UCwXOKS-z1t9u6Axmm3blXug"


def build_dashboard_json() -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── 머니그라피 기본 정보 ───────────────────────────
    target_info = db.get_latest_channel_stats(TARGET_CHANNEL_ID)
    channels = {ch["channel_id"]: ch for ch in db.get_all_channels()}
    target_ch = channels[TARGET_CHANNEL_ID]

    # ── 최근 영상 (60일) ──────────────────────────────
    recent_videos_raw = db.get_recent_videos(TARGET_CHANNEL_ID, days=60)
    outlier_ids = analyze.detect_outliers(recent_videos_raw)

    recent_videos = []
    for v in recent_videos_raw:
        try:
            from datetime import datetime as dt
            pub = dt.fromisoformat(v["published_at"].replace("Z", ""))
            weekday = analyze.WEEKDAY_KR[pub.weekday()]
        except Exception:
            weekday = ""

        view_count = v.get("view_count") or 0
        like_count = v.get("like_count") or 0
        comment_count = v.get("comment_count") or 0
        is_short = bool(v.get("is_short", 0))

        recent_videos.append({
            "video_id": v["video_id"],
            "title": v["title"],
            "published_at": v["published_at"],
            "published_weekday": weekday,
            "program_name": v.get("program_name"),
            "episode_number": v.get("episode_number"),
            "duration_seconds": v.get("duration_seconds") or 0,
            "is_short": is_short,
            "view_count": view_count,
            "like_count": like_count,
            "comment_count": comment_count,
            "like_rate": analyze.safe_rate(like_count, view_count),
            "comment_rate": analyze.safe_rate(comment_count, view_count),
            "is_outlier": v["video_id"] in outlier_ids,
            "trend": analyze.build_video_trend(v["video_id"]),
        })

    # ── 업로드 요일 히트맵 ────────────────────────────
    all_target_videos = db.get_all_target_videos()
    upload_heatmap = analyze.build_upload_heatmap(all_target_videos)

    # ── 프로그램 분석 ─────────────────────────────────
    programs = analyze.analyze_programs(all_target_videos)

    # ── 채널 성장 추이 ────────────────────────────────
    growth_data = analyze.build_growth_data(TARGET_CHANNEL_ID, days=30)

    # 구독자 급증일과 연결된 영상 (급증 전후 3일 이내 업로드)
    spike_videos = []
    for i, g in enumerate(growth_data):
        if g["delta"] > 0 and i > 0:
            avg_delta = sum(x["delta"] for x in growth_data if x["delta"] > 0) / max(
                sum(1 for x in growth_data if x["delta"] > 0), 1
            )
            if g["delta"] >= avg_delta * 2:  # 평균의 2배 이상 급증
                for v in recent_videos_raw:
                    pub = v.get("published_at", "")
                    if abs((
                        datetime.fromisoformat(g["date"])
                        - datetime.fromisoformat(pub[:10])
                    ).days) <= 3:
                        spike_videos.append({
                            "date": g["date"],
                            "delta": g["delta"],
                            "video_id": v["video_id"],
                            "title": v["title"],
                        })
                        break

    # ── 주간 비교 지표 (전주 대비) ────────────────────
    weekly_comparison = db.get_weekly_comparison_stats(TARGET_CHANNEL_ID)

    # ── 주간/월간 스냅샷 히스토리 ─────────────────────
    weekly_snapshots = db.get_all_weekly_snapshots(TARGET_CHANNEL_ID)
    monthly_snapshots = db.get_all_monthly_snapshots(TARGET_CHANNEL_ID)
    daily_records = db.get_daily_records(TARGET_CHANNEL_ID, days=35)

    # ── 롱폼 30일 조회수 추이 ─────────────────────────
    longform_trend = analyze.build_longform_view_trend(TARGET_CHANNEL_ID, days=30)

    # ── 경쟁 채널 비교 ────────────────────────────────
    competitor_data = analyze.build_competitor_data()

    weekly_growth_ranking = sorted(
        competitor_data, key=lambda x: x["weekly_growth"], reverse=True
    )
    avg_views_ranking = sorted(
        competitor_data, key=lambda x: x["avg_views_per_video"], reverse=True
    )
    upload_freq_ranking = sorted(
        competitor_data, key=lambda x: x["weekly_upload_count"], reverse=True
    )

    return {
        "updated_at": today,
        "weekly_comparison": weekly_comparison,
        "target_channel": {
            "channel_id": TARGET_CHANNEL_ID,
            "name": target_ch["name"],
            "subscriber_count": target_info.get("subscriber_count", 0),
            "view_count": target_info.get("view_count", 0),
            "video_count": target_info.get("video_count", 0),
            "recent_videos": recent_videos,
            "longform_trend": longform_trend,
            "upload_heatmap": upload_heatmap,
            "programs": programs,
            "growth": growth_data,
            "spike_videos": spike_videos,
        },
        "weekly_snapshots": weekly_snapshots,
        "monthly_snapshots": monthly_snapshots,
        "daily_records": daily_records,
        "competitors": competitor_data,
        "comparison": {
            "weekly_growth_ranking": [
                {"name": c["name"], "value": c["weekly_growth"], "rate": c["weekly_growth_rate"]}
                for c in weekly_growth_ranking
            ],
            "avg_views_ranking": [
                {"name": c["name"], "value": c["avg_views_per_video"]}
                for c in avg_views_ranking
            ],
            "upload_freq_ranking": [
                {"name": c["name"], "value": c["weekly_upload_count"]}
                for c in upload_freq_ranking
            ],
        },
    }


def generate():
    logger.info("대시보드 JSON 생성 중...")
    data = build_dashboard_json()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("저장 완료: %s", OUTPUT_PATH)
