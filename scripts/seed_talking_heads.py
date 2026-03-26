#!/usr/bin/env python3
"""토킹헤즈 영상을 DB에 삽입하고 YouTube API로 stats 저장"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from src import db
from src.collect import get_youtube_client, fetch_video_stats

CHANNEL_ID = "UCwXOKS-z1t9u6Axmm3blXug"

TALKING_HEADS_VIDEOS = [
    ("1CIG4BABI4E", "[최종화] 어른이 된다는 건 뭘까? ... | 토킹 헤즈", "2025-12-23"),
    ("doQT-iEUg-M", "탈모도 이제 건강보험 적용된다? ... | 토킹 헤즈", "2025-12-16"),
    ("79u_hSgFUys", "이제 막 돈 모으기 시작한 2030 ... | 토킹 헤즈", "2025-12-09"),
    ("zmxH32uVf_A", "사람을 NPC 취급하는 시대? ... | 토킹 헤즈", "2025-12-02"),
    ("zbaGEX5RTlU", "영유 가려고 학원까지 간다?! ... | 토킹 헤즈", "2025-11-18"),
    ("6zrcYELKybg", "돈이 있어야 취향이 생긴다? ... | 토킹 헤즈", "2025-11-11"),
    ("v-uuFMUKhr8", "눈코입은 왜 모여있을까? ... | 토킹 헤즈", "2025-11-04"),
    ("VWLeSZP0VaE", "서울은 거품일까? ... | 토킹 헤즈", "2025-10-28"),
    ("hd4djS203y0", "결정사 실적은 역대급인데 ... | 토킹 헤즈", "2025-10-21"),
    ("Pfpolx_wx1E", "기업들이 신입을 더 이상 안 뽑는 ... | 토킹 헤즈", "2025-10-14"),
    ("sayBfgt8idw", "영화표는 안 팔려도 야구장은 ... | 토킹 헤즈", "2025-09-30"),
    ("XlKx68H4nmU", "AI시대에 샤머니즘이 더 떠오른다? ... | 토킹 헤즈", "2025-09-23"),
    ("sZUFQ4N5iOM", "SNS는 접어도 커뮤니티는 ... | 토킹 헤즈", "2025-09-16"),
    ("Jw1ygD7eI-E", "돈 없으면 살 못 뺀다?! ... | 토킹 헤즈", "2025-09-09"),
    ("Shf66f0FYWQ", "AI가 흉내낼 수도 대체할 수도 없는 ... | 토킹 헤즈", "2025-08-28"),
    ("MUfPUlplLO8", "[티저] 같이 수다 떠실 분? ... | 토킹 헤즈", "2025-08-28"),
]

def main():
    db.init_db()

    print(f"토킹헤즈 영상 {len(TALKING_HEADS_VIDEOS)}개 DB upsert 중...")
    for video_id, title, published_at in TALKING_HEADS_VIDEOS:
        db.upsert_video(video_id, CHANNEL_ID, title, published_at, "토킹헤즈", None)
        print(f"  upsert: {video_id} | {published_at}")

    print("\nYouTube API로 video_stats 수집 중...")
    youtube = get_youtube_client()
    video_ids = [v[0] for v in TALKING_HEADS_VIDEOS]
    stats = fetch_video_stats(youtube, video_ids)

    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for vid, s in stats.items():
        db.save_video_stats(vid, today, s["view_count"], s["like_count"], s["comment_count"])
        db.update_video_duration(vid, s["duration_seconds"], s["is_short"])
        print(f"  stats saved: {vid} | views={s['view_count']:,} | duration={s['duration_seconds']}s | is_short={s['is_short']}")

    print(f"\n완료! {len(stats)}개 영상 stats 저장됨.")

if __name__ == "__main__":
    main()
