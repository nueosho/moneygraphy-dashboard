"""
데이터 분석 모듈
- 아웃라이어 영상 감지
- 프로그램 통계 집계
- 채널 성장 분석
- 경쟁 채널 비교
"""
import statistics
from datetime import datetime, timedelta
from typing import Optional

from src import db

WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]
TARGET_CHANNEL_ID = "UCwXOKS-z1t9u6Axmm3blXug"
OUTLIER_SIGMA = 1.5  # 평균 + N*표준편차 이상이면 아웃라이어


def safe_rate(numerator, denominator) -> float:
    if not denominator:
        return 0.0
    return round(numerator / denominator * 100, 2)


def detect_outliers(videos: list[dict]) -> set[str]:
    """조회수 기준 아웃라이어 영상 ID 집합 반환"""
    counts = [v["view_count"] for v in videos if v.get("view_count")]
    if len(counts) < 3:
        return set()
    mean = statistics.mean(counts)
    stdev = statistics.stdev(counts)
    threshold = mean + OUTLIER_SIGMA * stdev
    return {v["video_id"] for v in videos if (v.get("view_count") or 0) >= threshold}


def build_upload_heatmap(videos: list[dict]) -> dict:
    """요일별 업로드 횟수 히트맵 데이터"""
    heatmap = {day: 0 for day in WEEKDAY_KR}
    for v in videos:
        try:
            dt = datetime.fromisoformat(v["published_at"].replace("Z", ""))
            day = WEEKDAY_KR[dt.weekday()]
            heatmap[day] += 1
        except (ValueError, AttributeError):
            pass
    return heatmap


def analyze_programs(videos: list[dict]) -> list[dict]:
    """
    프로그램별 통계 집계.
    - 에피소드가 있는 프로그램: 최신 에피소드와 직전 에피소드 비교
    - 최근 60일 이내 에피소드가 있으면 방영중, 없으면 종영
    """
    from collections import defaultdict

    programs: dict[str, list[dict]] = defaultdict(list)
    for v in videos:
        if v.get("program_name"):
            programs[v["program_name"]].append(v)

    result = []
    cutoff_active = (datetime.utcnow() - timedelta(days=60)).strftime("%Y-%m-%d")

    for name, eps in programs.items():
        eps_sorted = sorted(
            eps,
            key=lambda x: (x.get("episode_number") or 0, x.get("published_at") or ""),
        )

        views = [e["view_count"] for e in eps_sorted if e.get("view_count")]
        likes = [e["like_count"] for e in eps_sorted if e.get("like_count")]
        comments = [e["comment_count"] for e in eps_sorted if e.get("comment_count")]

        avg_views = round(statistics.mean(views)) if views else 0
        avg_likes = round(statistics.mean(likes)) if likes else 0
        avg_comments = round(statistics.mean(comments)) if comments else 0

        latest = eps_sorted[-1] if eps_sorted else None
        prev = eps_sorted[-2] if len(eps_sorted) >= 2 else None

        # delta: 최신 vs 이전 좋아요수/댓글수 비교 (뷰 제외 - 업로드 시간 달라 의미없음)
        delta = {}
        if latest and prev:
            delta = {
                "likes": (latest.get("like_count") or 0) - (prev.get("like_count") or 0),
                "comments": (latest.get("comment_count") or 0) - (prev.get("comment_count") or 0),
            }

        # 방영중 여부: 최근 60일 내 에피소드 존재
        is_active = any(
            (e.get("published_at") or "") >= cutoff_active for e in eps_sorted
        )

        result.append({
            "name": name,
            "status": "방영중" if is_active else "종영",
            "episode_count": len(eps_sorted),
            "avg_view_count": avg_views,
            "avg_like_count": avg_likes,
            "avg_comment_count": avg_comments,
            "latest_episode": _episode_summary(latest),
            "prev_episode": _episode_summary(prev),
            "delta": delta,
        })

    result.sort(key=lambda x: (x["status"] == "종영", -x["avg_view_count"]))
    return result


def _episode_summary(ep) -> Optional[dict]:
    if not ep:
        return None
    return {
        "video_id": ep.get("video_id"),
        "title": ep.get("title"),
        "published_at": ep.get("published_at"),
        "episode_number": ep.get("episode_number"),
        "view_count": ep.get("view_count") or 0,
        "like_count": ep.get("like_count") or 0,
        "comment_count": ep.get("comment_count") or 0,
        "like_rate": safe_rate(ep.get("like_count", 0), ep.get("view_count", 0)),
        "comment_rate": safe_rate(ep.get("comment_count", 0), ep.get("view_count", 0)),
    }


def build_growth_data(channel_id: str, days: int = 30) -> list[dict]:
    """구독자 일별 증감 데이터"""
    history = db.get_channel_stats_history(channel_id, days)
    result = []
    for i, row in enumerate(history):
        delta = 0
        if i > 0:
            prev = history[i - 1]["subscriber_count"] or 0
            curr = row["subscriber_count"] or 0
            delta = curr - prev
        result.append({
            "date": row["collected_date"],
            "subscriber_count": row["subscriber_count"] or 0,
            "delta": delta,
        })
    return result


def build_competitor_data() -> list[dict]:
    """경쟁 채널 비교 데이터"""
    channels = db.get_all_channels()
    result = []
    for ch in channels:
        cid = ch["channel_id"]
        latest = db.get_latest_channel_stats(cid)
        history = db.get_channel_stats_history(cid, 14)
        weekly_upload = db.get_weekly_video_count(cid)

        # 주간 구독자 성장
        weekly_growth = 0
        weekly_growth_rate = 0.0
        if len(history) >= 8:
            curr_subs = history[-1]["subscriber_count"] or 0
            week_ago_subs = history[-8]["subscriber_count"] or 0
            weekly_growth = curr_subs - week_ago_subs
            if week_ago_subs:
                weekly_growth_rate = round(weekly_growth / week_ago_subs * 100, 2)

        # 최근 영상 평균 조회수
        recent_videos = db.get_recent_videos(cid, days=30)
        avg_views = 0
        if recent_videos:
            views = [v["view_count"] for v in recent_videos if v.get("view_count")]
            avg_views = round(statistics.mean(views)) if views else 0

        comp_recent_vids = db.get_recent_videos(cid, days=14)
        result.append({
            "channel_id": cid,
            "name": ch["name"],
            "is_target": ch["is_target"],
            "subscriber_count": latest.get("subscriber_count", 0) or 0,
            "weekly_growth": weekly_growth,
            "weekly_growth_rate": weekly_growth_rate,
            "avg_views_per_video": avg_views,
            "weekly_upload_count": weekly_upload["total"],
            "weekly_longform_count": weekly_upload["longform"],
            "weekly_shortform_count": weekly_upload["shortform"],
            "growth_history": [
                {"date": r["collected_date"], "subscribers": r["subscriber_count"] or 0}
                for r in history
            ],
            "recent_videos": [
                {
                    "video_id": v["video_id"],
                    "title": v["title"],
                    "published_at": v["published_at"],
                    "view_count": v.get("view_count") or 0,
                    "is_short": bool(v.get("is_short", 0)),
                }
                for v in comp_recent_vids[:10]
            ],
        })

    result.sort(key=lambda x: x["subscriber_count"], reverse=True)
    return result


def build_video_trend(video_id: str) -> list[dict]:
    """영상 7일간 조회수/좋아요/댓글 추이"""
    rows = db.get_video_stats_trend(video_id, days=7)
    return [
        {
            "date": r["collected_date"],
            "view_count": r["view_count"] or 0,
            "like_count": r["like_count"] or 0,
            "comment_count": r["comment_count"] or 0,
        }
        for r in rows
    ]


def build_longform_view_trend(channel_id: str, days: int = 30) -> list[dict]:
    """롱폼 영상만 최근 N일 날짜별 총 조회수 추이"""
    videos = db.get_recent_videos(channel_id, days=days)
    longform_videos = [v for v in videos if not v.get("is_short", 0)]

    from collections import defaultdict
    date_data: dict = defaultdict(lambda: {"view_count": 0, "videos": []})
    for v in longform_videos:
        pub = (v.get("published_at") or "")[:10]
        view_count = v.get("view_count") or 0
        if pub:
            date_data[pub]["view_count"] += view_count
            date_data[pub]["videos"].append({
                "video_id": v["video_id"],
                "title": v["title"],
                "view_count": view_count,
            })

    result = [
        {"date": d, "view_count": date_data[d]["view_count"], "videos": date_data[d]["videos"]}
        for d in sorted(date_data.keys())
    ]
    return result
