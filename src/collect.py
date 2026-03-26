import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src import db

logger = logging.getLogger(__name__)

MAX_RESULTS = 50  # 채널당 최근 영상 수집 수


def get_youtube_client():
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY 환경변수가 설정되지 않았습니다.")
    return build("youtube", "v3", developerKey=api_key)


def fetch_channel_info(youtube, channel_ids) -> dict:
    """채널 기본 정보 및 구독자/조회수/영상수 수집"""
    result = {}
    # API는 한 번에 최대 50개 채널 조회 가능
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        try:
            resp = (
                youtube.channels()
                .list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(batch),
                    maxResults=50,
                )
                .execute()
            )
            for item in resp.get("items", []):
                cid = item["id"]
                stats = item.get("statistics", {})
                uploads_pid = (
                    item.get("contentDetails", {})
                    .get("relatedPlaylists", {})
                    .get("uploads", "")
                )
                result[cid] = {
                    "subscriber_count": int(stats.get("subscriberCount", 0)),
                    "view_count": int(stats.get("viewCount", 0)),
                    "video_count": int(stats.get("videoCount", 0)),
                    "uploads_playlist_id": uploads_pid,
                }
        except HttpError as e:
            logger.error("채널 정보 수집 실패 %s: %s", batch, e)
    return result


def fetch_playlist_videos(youtube, playlist_id: str, max_results: int = MAX_RESULTS) -> list:
    """업로드 플레이리스트에서 최근 영상 목록 수집"""
    videos = []
    next_page = None
    while len(videos) < max_results:
        try:
            req = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=min(50, max_results - len(videos)),
                pageToken=next_page,
            )
            resp = req.execute()
            for item in resp.get("items", []):
                snippet = item["snippet"]
                vid = snippet.get("resourceId", {}).get("videoId")
                if vid:
                    videos.append(
                        {
                            "video_id": vid,
                            "title": snippet.get("title", ""),
                            "published_at": snippet.get("publishedAt", "")[:10],
                        }
                    )
            next_page = resp.get("nextPageToken")
            if not next_page:
                break
        except HttpError as e:
            logger.error("플레이리스트 %s 수집 실패: %s", playlist_id, e)
            break
    return videos


def parse_duration_seconds(iso_duration: str) -> int:
    """ISO 8601 duration (PT1M30S) → 초 단위 정수 변환"""
    import re
    if not iso_duration:
        return 0
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    m = re.match(pattern, iso_duration)
    if not m:
        return 0
    h = int(m.group(1) or 0)
    mn = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mn * 60 + s


def fetch_video_stats(youtube, video_ids) -> dict:
    """영상별 조회수/좋아요/댓글수 + duration 수집"""
    result = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        try:
            resp = (
                youtube.videos()
                .list(part="statistics,contentDetails", id=",".join(batch))
                .execute()
            )
            for item in resp.get("items", []):
                vid = item["id"]
                stats = item.get("statistics", {})
                duration_str = item.get("contentDetails", {}).get("duration", "")
                duration_sec = parse_duration_seconds(duration_str)
                result[vid] = {
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                    "duration_seconds": duration_sec,
                    "is_short": 1 if duration_sec > 0 and duration_sec <= 180 else 0,
                }
        except HttpError as e:
            logger.error("영상 통계 수집 실패: %s", e)
    return result


def collect_all():
    """전체 채널 데이터 수집 메인 함수"""
    youtube = get_youtube_client()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    channels = db.get_all_channels()
    channel_ids = [ch["channel_id"] for ch in channels]

    logger.info("채널 정보 수집 중... (%d개)", len(channel_ids))
    channel_info = fetch_channel_info(youtube, channel_ids)

    for ch in channels:
        cid = ch["channel_id"]
        info = channel_info.get(cid)
        if not info:
            logger.warning("채널 정보 없음: %s (%s)", ch["name"], cid)
            continue

        # 채널 통계 저장
        db.save_channel_stats(
            cid, today,
            info["subscriber_count"],
            info["view_count"],
            info["video_count"],
        )

        # 업로드 플레이리스트 ID 갱신
        if info["uploads_playlist_id"]:
            db.update_uploads_playlist(cid, info["uploads_playlist_id"])

        logger.info("  [%s] 구독자: %s명", ch["name"], f"{info['subscriber_count']:,}")

    # 영상 수집 (전체 채널)
    logger.info("영상 목록 수집 중...")
    all_video_ids = []
    for ch in channels:
        cid = ch["channel_id"]
        playlist_id = channel_info.get(cid, {}).get("uploads_playlist_id") or ch.get("uploads_playlist_id")
        if not playlist_id:
            logger.warning("  [%s] 플레이리스트 ID 없음, 건너뜀", ch["name"])
            continue

        videos = fetch_playlist_videos(youtube, playlist_id, max_results=MAX_RESULTS)
        logger.info("  [%s] 영상 %d개 수집", ch["name"], len(videos))

        for v in videos:
            program_name, episode_number = extract_program(v["title"])
            db.upsert_video(
                v["video_id"], cid, v["title"], v["published_at"],
                program_name, episode_number,
            )
            all_video_ids.append(v["video_id"])

        time.sleep(0.1)  # API 쿼터 보호

    # 영상 통계 수집
    logger.info("영상 통계 수집 중... (%d개)", len(all_video_ids))
    video_stats = fetch_video_stats(youtube, all_video_ids)
    for vid, stats in video_stats.items():
        db.save_video_stats(
            vid, today,
            stats["view_count"],
            stats["like_count"],
            stats["comment_count"],
        )
        db.update_video_duration(vid, stats["duration_seconds"], stats["is_short"])
    logger.info("수집 완료!")


def extract_program(title: str) -> tuple:
    """
    영상 제목에서 프로그램명과 에피소드 번호 추출.
    패턴: '| 프로그램명' 또는 '| 프로그램명 숫자'
    예) '경제 뉴스 | B주류경제학 3' → ('B주류경제학', 3)
        '게스트 인터뷰 | B주류초대석' → ('B주류초대석', None)
    """
    import re
    pattern = r"\|\s*([\w가-힣\s\(\)&]+?)(?:\s+(\d+))?\s*(?:\||$)"
    matches = re.findall(pattern, title)
    if matches:
        name_raw, ep_raw = matches[-1]
        name = name_raw.strip()
        episode = int(ep_raw) if ep_raw else None
        if name:
            # 프로그램명 정규화
            if name.startswith("B주류초대석"):
                name = "B주류초대석"
            if "토킹 헤즈" in name or "토킹헤즈" in name:
                name = "토킹헤즈"
            return name, episode
    return None, None
