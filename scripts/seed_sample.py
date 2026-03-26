"""
샘플 데이터 생성 스크립트 (API 키 없이 대시보드 미리보기용)
실행: python scripts/seed_sample.py
"""
import sys
import random
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db import init_db, get_all_channels, save_channel_stats, upsert_video, save_video_stats
from src.collect import extract_program
from src.generate import generate

random.seed(42)

SAMPLE_TITLES = [
    "주식 시장은 왜 무너졌나 | B주류경제학 5",
    "금리 인하의 진짜 의미 | B주류경제학 6",
    "투자의 심리학 | B주류경제학 7",
    "밸류에이션 완전 정복 | B주류경제학 8",
    "인플레이션 끝의 세계 | B주류경제학 9",
    "세계 최고의 투자자가 말하는 것 | B주류초대석",
    "스타트업 투자 전문가 | B주류초대석",
    "머니그라피 2024 결산",
    "경제 뉴스 브리핑 | 위클리머니 20",
    "경제 뉴스 브리핑 | 위클리머니 21",
    "경제 뉴스 브리핑 | 위클리머니 22",
    "미국 대선과 경제 | B주류경제학 10",
    "부동산 시장 전망 2025",
    "ETF 투자 완전 가이드",
    "암호화폐의 미래 | B주류경제학 11",
]

def make_video_id(i):
    return f"sample_vid_{i:04d}"

def seed():
    init_db()
    channels = get_all_channels()
    today = datetime.now(timezone.utc)

    for ch in channels:
        cid = ch["channel_id"]
        is_target = ch["is_target"]
        base_subs = random.randint(50000, 2000000)

        # 30일치 채널 통계
        for day_offset in range(30, -1, -1):
            date = (today - timedelta(days=day_offset)).strftime("%Y-%m-%d")
            growth = random.randint(50, 300) * (1 if is_target else random.randint(1, 5))
            subs = base_subs + (30 - day_offset) * growth
            views = subs * random.randint(50, 200)
            videos = random.randint(50, 300)
            save_channel_stats(cid, date, subs, views, videos)

        # 영상 생성 (최근 60일)
        titles = SAMPLE_TITLES if is_target else [f"{ch['name']} 영상 {i}" for i in range(15)]
        for i, title in enumerate(titles):
            vid = make_video_id(hash(cid + title) % 9999)
            days_ago = random.randint(0, 59)
            pub_date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            prog, ep = extract_program(title)
            upsert_video(vid, cid, title, pub_date, prog, ep)

            # 7일치 영상 통계
            base_views = random.randint(5000, 200000) if is_target else random.randint(1000, 500000)
            for d in range(7, -1, -1):
                stat_date = (today - timedelta(days=d)).strftime("%Y-%m-%d")
                views = max(0, base_views - d * random.randint(100, 2000))
                likes = int(views * random.uniform(0.01, 0.05))
                comments = int(views * random.uniform(0.002, 0.008))
                save_video_stats(vid, stat_date, views, likes, comments)

    print("샘플 데이터 생성 완료!")
    generate()
    print(f"대시보드 JSON 생성 완료: docs/data/dashboard.json")

if __name__ == "__main__":
    seed()
