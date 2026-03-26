import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path(__file__).parent.parent / "data" / "youtube.db"

CHANNELS = [
    ("UCwXOKS-z1t9u6Axmm3blXug", "머니그라피", 1),
    ("UCUj6rrhMTR9pipbAWBAMvUQ", "침착맨", 0),
    ("UCsJ6RuBiTVWRX156FVbeaGg", "슈카월드", 0),
    ("UCV19yBCFgPV7aJmd5sZ5lBQ", "오키키ㅇㅋㅋ", 0),
    ("UCRbjtZUrNAQe7w5c03JCMXw", "민음사TV", 0),
    ("UCePdAFPZ5985Y7nPuUcx7uw", "십이층", 0),
    ("UC2Iko5L69mFarhDJX3htJPg", "SPNS TV", 0),
    ("UC13cuDfYBb9nRpAT3GFlFlQ", "카우치포테이토클럽", 0),
    ("UC4JuIRh1xXTaxv9rkZP0f1Q", "페페스튜디오", 0),
]

TARGET_CHANNEL_ID = "UCwXOKS-z1t9u6Axmm3blXug"


def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            is_target INTEGER DEFAULT 0,
            uploads_playlist_id TEXT
        );

        CREATE TABLE IF NOT EXISTS channel_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            collected_date TEXT NOT NULL,
            subscriber_count INTEGER,
            view_count INTEGER,
            video_count INTEGER,
            UNIQUE(channel_id, collected_date),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        );

        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            title TEXT NOT NULL,
            published_at TEXT NOT NULL,
            program_name TEXT,
            episode_number INTEGER,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        );

        CREATE TABLE IF NOT EXISTS weekly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,
            subscriber_count INTEGER,
            total_view_count INTEGER,
            total_comment_count INTEGER,
            longform_count INTEGER DEFAULT 0,
            shortform_count INTEGER DEFAULT 0,
            UNIQUE(channel_id, week_start),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        );

        CREATE TABLE IF NOT EXISTS monthly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            month TEXT NOT NULL,
            subscriber_count INTEGER,
            total_view_count INTEGER,
            total_comment_count INTEGER,
            longform_count INTEGER DEFAULT 0,
            shortform_count INTEGER DEFAULT 0,
            UNIQUE(channel_id, month),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        );

        CREATE TABLE IF NOT EXISTS video_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            collected_date TEXT NOT NULL,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            UNIQUE(video_id, collected_date),
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        );

        CREATE TABLE IF NOT EXISTS daily_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            record_date TEXT NOT NULL,
            subscriber_count INTEGER DEFAULT 0,
            total_view_count INTEGER DEFAULT 0,
            total_comment_count INTEGER DEFAULT 0,
            total_like_count INTEGER DEFAULT 0,
            longform_view_count INTEGER DEFAULT 0,
            longform_comment_count INTEGER DEFAULT 0,
            longform_like_count INTEGER DEFAULT 0,
            shortform_view_count INTEGER DEFAULT 0,
            shortform_comment_count INTEGER DEFAULT 0,
            shortform_like_count INTEGER DEFAULT 0,
            longform_upload_count INTEGER DEFAULT 0,
            shortform_upload_count INTEGER DEFAULT 0,
            UNIQUE(channel_id, record_date),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        );
    """)
    for channel_id, name, is_target in CHANNELS:
        c.execute(
            "INSERT OR IGNORE INTO channels (channel_id, name, is_target) VALUES (?,?,?)",
            (channel_id, name, is_target),
        )
    conn.commit()
    conn.close()


def update_uploads_playlist(channel_id, playlist_id):
    conn = get_conn()
    conn.execute(
        "UPDATE channels SET uploads_playlist_id=? WHERE channel_id=?",
        (playlist_id, channel_id),
    )
    conn.commit()
    conn.close()


def save_channel_stats(channel_id, date_str, subscriber_count, view_count, video_count):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO channel_stats
            (channel_id, collected_date, subscriber_count, view_count, video_count)
        VALUES (?,?,?,?,?)
        ON CONFLICT(channel_id, collected_date) DO UPDATE SET
            subscriber_count=excluded.subscriber_count,
            view_count=excluded.view_count,
            video_count=excluded.video_count
        """,
        (channel_id, date_str, subscriber_count, view_count, video_count),
    )
    conn.commit()
    conn.close()


def upsert_video(video_id, channel_id, title, published_at, program_name, episode_number):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO videos
            (video_id, channel_id, title, published_at, program_name, episode_number)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(video_id) DO UPDATE SET
            title=excluded.title,
            program_name=excluded.program_name,
            episode_number=excluded.episode_number
        """,
        (video_id, channel_id, title, published_at, program_name, episode_number),
    )
    conn.commit()
    conn.close()


def save_video_stats(video_id, date_str, view_count, like_count, comment_count):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO video_stats
            (video_id, collected_date, view_count, like_count, comment_count)
        VALUES (?,?,?,?,?)
        ON CONFLICT(video_id, collected_date) DO UPDATE SET
            view_count=excluded.view_count,
            like_count=excluded.like_count,
            comment_count=excluded.comment_count
        """,
        (video_id, date_str, view_count, like_count, comment_count),
    )
    conn.commit()
    conn.close()


def get_all_channels():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM channels").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_channel_stats_history(channel_id, days=30):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT * FROM channel_stats
        WHERE channel_id=?
        ORDER BY collected_date ASC
        LIMIT ?
        """,
        (channel_id, days),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_channel_stats(channel_id):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT * FROM channel_stats
        WHERE channel_id=?
        ORDER BY collected_date DESC
        LIMIT 1
        """,
        (channel_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_recent_videos(channel_id, days=60):
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT v.*,
               vs.view_count, vs.like_count, vs.comment_count
        FROM videos v
        LEFT JOIN video_stats vs ON v.video_id = vs.video_id
            AND vs.collected_date = (
                SELECT MAX(collected_date) FROM video_stats WHERE video_id=v.video_id
            )
        WHERE v.channel_id=? AND v.published_at >= ?
        ORDER BY v.published_at DESC
        """,
        (channel_id, cutoff),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_video_stats_trend(video_id, days=7):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT * FROM video_stats
        WHERE video_id=?
        ORDER BY collected_date ASC
        LIMIT ?
        """,
        (video_id, days),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_target_videos():
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT v.*,
               vs.view_count, vs.like_count, vs.comment_count
        FROM videos v
        LEFT JOIN video_stats vs ON v.video_id = vs.video_id
            AND vs.collected_date = (
                SELECT MAX(collected_date) FROM video_stats WHERE video_id=v.video_id
            )
        WHERE v.channel_id=?
        ORDER BY v.published_at DESC
        """,
        (TARGET_CHANNEL_ID,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_video_duration(video_id, duration_seconds, is_short):
    conn = get_conn()
    conn.execute(
        "UPDATE videos SET duration_seconds=?, is_short=? WHERE video_id=?",
        (duration_seconds, is_short, video_id),
    )
    conn.commit()
    conn.close()


def get_weekly_comparison_stats(channel_id):
    """최근 7일 vs 이전 7일 채널 통계 비교"""
    conn = get_conn()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    two_weeks_ago = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")

    # 최신 구독자 수
    curr = conn.execute(
        "SELECT subscriber_count FROM channel_stats WHERE channel_id=? ORDER BY collected_date DESC LIMIT 1",
        (channel_id,)
    ).fetchone()
    # 7일 전 구독자 수
    prev = conn.execute(
        "SELECT subscriber_count FROM channel_stats WHERE channel_id=? AND collected_date <= ? ORDER BY collected_date DESC LIMIT 1",
        (channel_id, week_ago)
    ).fetchone()

    # 최근 7일 댓글/조회수 합계 (채널 영상 기준)
    curr_vs = conn.execute(
        """
        SELECT SUM(vs.comment_count) as total_comments, SUM(vs.view_count) as total_views
        FROM video_stats vs
        JOIN videos v ON v.video_id = vs.video_id
        WHERE v.channel_id=? AND vs.collected_date > ? AND vs.collected_date <= ?
        """,
        (channel_id, week_ago, today)
    ).fetchone()
    # 이전 7일
    prev_vs = conn.execute(
        """
        SELECT SUM(vs.comment_count) as total_comments, SUM(vs.view_count) as total_views
        FROM video_stats vs
        JOIN videos v ON v.video_id = vs.video_id
        WHERE v.channel_id=? AND vs.collected_date > ? AND vs.collected_date <= ?
        """,
        (channel_id, two_weeks_ago, week_ago)
    ).fetchone()
    conn.close()

    curr_subs = curr["subscriber_count"] if curr else 0
    prev_subs = prev["subscriber_count"] if prev else 0
    sub_delta = curr_subs - prev_subs
    sub_rate = round(sub_delta / prev_subs * 100, 2) if prev_subs else 0

    curr_comments = (curr_vs["total_comments"] or 0) if curr_vs else 0
    prev_comments = (prev_vs["total_comments"] or 0) if prev_vs else 0
    comment_delta = curr_comments - prev_comments
    comment_rate = round(comment_delta / prev_comments * 100, 2) if prev_comments else 0

    curr_views = (curr_vs["total_views"] or 0) if curr_vs else 0
    prev_views = (prev_vs["total_views"] or 0) if prev_vs else 0
    view_delta = curr_views - prev_views
    view_rate = round(view_delta / prev_views * 100, 2) if prev_views else 0

    return {
        "subscriber": {"current": curr_subs, "delta": sub_delta, "rate": sub_rate},
        "comments": {"current": curr_comments, "delta": comment_delta, "rate": comment_rate},
        "views": {"current": curr_views, "delta": view_delta, "rate": view_rate},
    }


def save_weekly_snapshot(channel_id, week_start, week_end, subscriber_count,
                          total_view_count, total_comment_count, longform_count, shortform_count):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO weekly_snapshots
            (channel_id, week_start, week_end, subscriber_count, total_view_count,
             total_comment_count, longform_count, shortform_count)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(channel_id, week_start) DO UPDATE SET
            week_end=excluded.week_end,
            subscriber_count=excluded.subscriber_count,
            total_view_count=excluded.total_view_count,
            total_comment_count=excluded.total_comment_count,
            longform_count=excluded.longform_count,
            shortform_count=excluded.shortform_count
        """,
        (channel_id, week_start, week_end, subscriber_count,
         total_view_count, total_comment_count, longform_count, shortform_count),
    )
    conn.commit()
    conn.close()


def save_monthly_snapshot(channel_id, month, subscriber_count,
                           total_view_count, total_comment_count, longform_count, shortform_count):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO monthly_snapshots
            (channel_id, month, subscriber_count, total_view_count,
             total_comment_count, longform_count, shortform_count)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(channel_id, month) DO UPDATE SET
            subscriber_count=excluded.subscriber_count,
            total_view_count=excluded.total_view_count,
            total_comment_count=excluded.total_comment_count,
            longform_count=excluded.longform_count,
            shortform_count=excluded.shortform_count
        """,
        (channel_id, month, subscriber_count,
         total_view_count, total_comment_count, longform_count, shortform_count),
    )
    conn.commit()
    conn.close()


def get_all_weekly_snapshots(channel_id):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM weekly_snapshots WHERE channel_id=? ORDER BY week_start ASC",
        (channel_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_monthly_snapshots(channel_id):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM monthly_snapshots WHERE channel_id=? ORDER BY month ASC",
        (channel_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_weekly_video_count(channel_id):
    cutoff = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    conn = get_conn()
    rows = conn.execute(
        """SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_short=0 THEN 1 ELSE 0 END) as longform,
            SUM(CASE WHEN is_short=1 THEN 1 ELSE 0 END) as shortform
           FROM videos WHERE channel_id=? AND published_at>=?""",
        (channel_id, cutoff),
    ).fetchone()
    conn.close()
    return {"total": rows["total"] or 0, "longform": rows["longform"] or 0, "shortform": rows["shortform"] or 0}


def save_daily_record(channel_id, record_date, subscriber_count,
                      total_view, total_comment, total_like,
                      lf_view, lf_comment, lf_like,
                      sf_view, sf_comment, sf_like,
                      lf_upload, sf_upload):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO daily_records
            (channel_id, record_date, subscriber_count,
             total_view_count, total_comment_count, total_like_count,
             longform_view_count, longform_comment_count, longform_like_count,
             shortform_view_count, shortform_comment_count, shortform_like_count,
             longform_upload_count, shortform_upload_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(channel_id, record_date) DO UPDATE SET
            subscriber_count=excluded.subscriber_count,
            total_view_count=excluded.total_view_count,
            total_comment_count=excluded.total_comment_count,
            total_like_count=excluded.total_like_count,
            longform_view_count=excluded.longform_view_count,
            longform_comment_count=excluded.longform_comment_count,
            longform_like_count=excluded.longform_like_count,
            shortform_view_count=excluded.shortform_view_count,
            shortform_comment_count=excluded.shortform_comment_count,
            shortform_like_count=excluded.shortform_like_count,
            longform_upload_count=excluded.longform_upload_count,
            shortform_upload_count=excluded.shortform_upload_count
        """,
        (channel_id, record_date, subscriber_count,
         total_view, total_comment, total_like,
         lf_view, lf_comment, lf_like,
         sf_view, sf_comment, sf_like,
         lf_upload, sf_upload),
    )
    conn.commit()
    conn.close()


def get_daily_records(channel_id, days=30):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT * FROM daily_records
        WHERE channel_id=?
        ORDER BY record_date DESC
        LIMIT ?
        """,
        (channel_id, days),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
