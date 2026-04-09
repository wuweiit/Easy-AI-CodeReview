from datetime import datetime, timedelta
import pandas as pd

from src.entity.review_entity import MergeRequestReviewEntity, PushReviewEntity
from src.service.db_factory import get_db_connection, DatabaseFactory


class ReviewService:

    @staticmethod
    def _get_create_table_sql(table_name: str) -> str:
        """获取创建表的SQL语句（兼容MySQL和SQLite）"""
        is_mysql = DatabaseFactory.is_mysql()

        if table_name == "mr_review_log":
            if is_mysql:
                return '''
                    CREATE TABLE IF NOT EXISTS mr_review_log (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        project_name VARCHAR(255),
                        author VARCHAR(255),
                        source_branch VARCHAR(255),
                        target_branch VARCHAR(255),
                        updated_at BIGINT,
                        commit_messages TEXT,
                        score INT,
                        url TEXT,
                        review_result LONGTEXT,
                        additions INT DEFAULT 0,
                        deletions INT DEFAULT 0,
                        INDEX idx_author (author),
                        INDEX idx_project_name (project_name),
                        INDEX idx_updated_at (updated_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                '''
            else:
                return '''
                    CREATE TABLE IF NOT EXISTS mr_review_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        project_name TEXT,
                        author TEXT,
                        source_branch TEXT,
                        target_branch TEXT,
                        updated_at INTEGER,
                        commit_messages TEXT,
                        score INTEGER,
                        url TEXT,
                        review_result TEXT,
                        additions INTEGER DEFAULT 0,
                        deletions INTEGER DEFAULT 0
                    )
                '''
        elif table_name == "push_review_log":
            if is_mysql:
                return '''
                    CREATE TABLE IF NOT EXISTS push_review_log (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        project_name VARCHAR(255),
                        author VARCHAR(255),
                        branch VARCHAR(255),
                        updated_at BIGINT,
                        commit_messages TEXT,
                        score INT,
                        review_result LONGTEXT,
                        additions INT DEFAULT 0,
                        deletions INT DEFAULT 0,
                        INDEX idx_author (author),
                        INDEX idx_project_name (project_name),
                        INDEX idx_updated_at (updated_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                '''
            else:
                return '''
                    CREATE TABLE IF NOT EXISTS push_review_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        project_name TEXT,
                        author TEXT,
                        branch TEXT,
                        updated_at INTEGER,
                        commit_messages TEXT,
                        score INTEGER,
                        review_result TEXT,
                        additions INTEGER DEFAULT 0,
                        deletions INTEGER DEFAULT 0
                    )
                '''
        elif table_name == "dingtalk_user_map":
            if is_mysql:
                return '''
                    CREATE TABLE IF NOT EXISTS dingtalk_user_map (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        git_username VARCHAR(255) NOT NULL COMMENT 'Git平台用户名(GitLab/GitHub/Gitea)',
                        dingtalk_userid VARCHAR(255) DEFAULT NULL COMMENT '钉钉用户ID',
                        dingtalk_mobile VARCHAR(20) DEFAULT NULL COMMENT '钉钉绑定的手机号',
                        remark VARCHAR(255) DEFAULT NULL COMMENT '备注(如姓名)',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE INDEX idx_git_username (git_username)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                '''
            else:
                return '''
                    CREATE TABLE IF NOT EXISTS dingtalk_user_map (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        git_username TEXT NOT NULL UNIQUE,
                        dingtalk_userid TEXT,
                        dingtalk_mobile TEXT,
                        remark TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                '''
        return ""

    @staticmethod
    def init_db():
        """初始化数据库及表结构"""
        try:
            with get_db_connection() as conn:
                # 创建mr_review_log表
                conn.execute(ReviewService._get_create_table_sql("mr_review_log"))

                # 创建push_review_log表
                conn.execute(ReviewService._get_create_table_sql("push_review_log"))

                # 确保旧版本的表添加additions、deletions列（仅SQLite需要，MySQL在创建时已包含）
                if DatabaseFactory.is_sqlite():
                    tables = ["mr_review_log", "push_review_log"]
                    columns = ["additions", "deletions"]
                    for table in tables:
                        for column in columns:
                            if not conn.column_exists(table, column):
                                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} INTEGER DEFAULT 0")

                # 创建 dingtalk_user_map 表
                conn.execute(ReviewService._get_create_table_sql("dingtalk_user_map"))

                conn.commit()
                print(f"Database initialized successfully using {DatabaseFactory.get_db_type()}")
        except Exception as e:
            print(f"Database initialization failed: {e}")

    @staticmethod
    def insert_mr_review_log(entity: MergeRequestReviewEntity):
        """插入合并请求审核日志"""
        try:
            with get_db_connection() as conn:
                conn.execute('''
                    INSERT INTO mr_review_log (project_name, author, source_branch, target_branch, updated_at, commit_messages, score, url, review_result, additions, deletions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (entity.project_name, entity.author, entity.source_branch,
                      entity.target_branch, entity.updated_at, entity.commit_messages,
                      entity.score, entity.url, entity.review_result, entity.additions, entity.deletions))
                conn.commit()
                print(f"[ReviewService] MR review log inserted successfully: {entity.project_name} by {entity.author}")
        except Exception as e:
            import traceback
            print(f"[ReviewService] Error inserting MR review log: {e}")
            print(f"[ReviewService] Traceback: {traceback.format_exc()}")

    @staticmethod
    def get_mr_review_logs(authors: list = None, project_names: list = None, updated_at_gte: int = None,
                           updated_at_lte: int = None) -> pd.DataFrame:
        """获取符合条件的合并请求审核日志"""
        try:
            with get_db_connection() as conn:
                query = """
                    SELECT project_name, author, source_branch, target_branch, updated_at, commit_messages, score, url, review_result, additions, deletions
                    FROM mr_review_log
                    WHERE 1=1
                """
                params = []

                if authors:
                    placeholders = ','.join(['%s'] * len(authors))
                    query += f" AND author IN ({placeholders})"
                    params.extend(authors)

                if project_names:
                    placeholders = ','.join(['%s'] * len(project_names))
                    query += f" AND project_name IN ({placeholders})"
                    params.extend(project_names)

                if updated_at_gte is not None:
                    query += " AND updated_at >= %s"
                    params.append(updated_at_gte)

                if updated_at_lte is not None:
                    query += " AND updated_at <= %s"
                    params.append(updated_at_lte)

                query += " ORDER BY updated_at DESC"

                # 使用pandas读取数据
                pandas_conn = conn.get_connection_for_pandas()
                df = pd.read_sql_query(sql=query, con=pandas_conn, params=params)

                print(df)
                return df
        except Exception as e:
            print(f"Error retrieving review logs: {e}")
            return pd.DataFrame()

    @staticmethod
    def insert_push_review_log(entity: PushReviewEntity):
        """插入推送审核日志"""
        try:
            with get_db_connection() as conn:
                conn.execute('''
                    INSERT INTO push_review_log (project_name, author, branch, updated_at, commit_messages, score, review_result, additions, deletions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (entity.project_name, entity.author, entity.branch,
                      entity.updated_at, entity.commit_messages, entity.score,
                      entity.review_result, entity.additions, entity.deletions))
                conn.commit()
                print(f"[ReviewService] Push review log inserted successfully: {entity.project_name} by {entity.author}")
        except Exception as e:
            import traceback
            print(f"[ReviewService] Error inserting push review log: {e}")
            print(f"[ReviewService] Traceback: {traceback.format_exc()}")

    @staticmethod
    def get_push_review_logs(authors: list = None, project_names: list = None, updated_at_gte: int = None,
                             updated_at_lte: int = None) -> pd.DataFrame:
        """获取符合条件的推送审核日志"""
        try:
            with get_db_connection() as conn:
                query = """
                    SELECT project_name, author, branch, updated_at, commit_messages, score, review_result, additions, deletions
                    FROM push_review_log
                    WHERE 1=1
                """
                params = []

                if authors:
                    placeholders = ','.join(['%s'] * len(authors))
                    query += f" AND author IN ({placeholders})"
                    params.extend(authors)

                if project_names:
                    placeholders = ','.join(['%s'] * len(project_names))
                    query += f" AND project_name IN ({placeholders})"
                    params.extend(project_names)

                if updated_at_gte is not None:
                    query += " AND updated_at >= %s"
                    params.append(updated_at_gte)

                if updated_at_lte is not None:
                    query += " AND updated_at <= %s"
                    params.append(updated_at_lte)

                query += " ORDER BY updated_at DESC"

                # 使用pandas读取数据
                pandas_conn = conn.get_connection_for_pandas()
                df = pd.read_sql_query(sql=query, con=pandas_conn, params=params)
                return df
        except Exception as e:
            print(f"Error retrieving push review logs: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_yesterday_mr_review_logs() -> pd.DataFrame:
        """获取昨天(前一日)的 Merge Request 审核日志"""
        try:
            # 计算昨天的时间范围
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
            # yesterday_start = (today - timedelta(days=1)).timestamp()
            yesterday_start = (today).timestamp() ## 今日
            yesterday_end = (today_end).timestamp()

            with get_db_connection() as conn:
                query = """
                    SELECT project_name, author, source_branch, target_branch, updated_at, commit_messages, score, url, review_result, additions, deletions
                    FROM mr_review_log
                    WHERE updated_at >= %s AND updated_at <= %s
                    ORDER BY updated_at DESC
                    limit 100
                """
                params = [yesterday_start, yesterday_end]

                pandas_conn = conn.get_connection_for_pandas()
                df = pd.read_sql_query(sql=query, con=pandas_conn, params=params)
                print(f"[ReviewService] Retrieved {len(df)} MR review logs for yesterday")
                return df
        except Exception as e:
            print(f"Error retrieving yesterday MR review logs: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_top10_mr_by_score(df: pd.DataFrame) -> pd.DataFrame:
        """获取分数最低的 Top10 MR 记录（分数越低表示问题越多）"""
        if df.empty:
            return df
        # 按分数升序排序，获取前10条（分数最低的）
        return df.nsmallest(10, 'score')

    # ==================== 钉钉用户映射相关方法 ====================

    @staticmethod
    def get_dingtalk_user_by_git_username(git_username: str) -> dict:
        """
        根据 Git 用户名查询钉钉用户映射信息

        :param git_username: Git 平台用户名
        :return: {'dingtalk_userid': ..., 'dingtalk_mobile': ..., 'remark': ...} 或空字典
        """
        try:
            with get_db_connection() as conn:
                result = conn.fetchone(
                    "SELECT dingtalk_userid, dingtalk_mobile, remark FROM dingtalk_user_map WHERE git_username = ?",
                    (git_username,)
                )
                if result:
                    return {
                        'dingtalk_userid': result[0],
                        'dingtalk_mobile': result[1],
                        'remark': result[2]
                    }
                return {}
        except Exception as e:
            print(f"[ReviewService] Error querying dingtalk user map: {e}")
            return {}

    @staticmethod
    def get_all_dingtalk_user_maps() -> list:
        """获取所有钉钉用户映射"""
        try:
            with get_db_connection() as conn:
                rows = conn.fetchall(
                    "SELECT id, git_username, dingtalk_userid, dingtalk_mobile, remark FROM dingtalk_user_map ORDER BY id"
                )
                return [
                    {
                        'id': row[0],
                        'git_username': row[1],
                        'dingtalk_userid': row[2],
                        'dingtalk_mobile': row[3],
                        'remark': row[4]
                    }
                    for row in rows
                ]
        except Exception as e:
            print(f"[ReviewService] Error querying all dingtalk user maps: {e}")
            return []

    @staticmethod
    def upsert_dingtalk_user_map(git_username: str, dingtalk_userid: str = None,
                                  dingtalk_mobile: str = None, remark: str = None) -> bool:
        """
        新增或更新钉钉用户映射

        :param git_username: Git 平台用户名
        :param dingtalk_userid: 钉钉用户 ID
        :param dingtalk_mobile: 钉钉绑定的手机号
        :param remark: 备注
        :return: 是否成功
        """
        try:
            with get_db_connection() as conn:
                existing = conn.fetchone(
                    "SELECT id FROM dingtalk_user_map WHERE git_username = ?",
                    (git_username,)
                )
                if existing:
                    conn.execute(
                        "UPDATE dingtalk_user_map SET dingtalk_userid = ?, dingtalk_mobile = ?, remark = ? WHERE git_username = ?",
                        (dingtalk_userid, dingtalk_mobile, remark, git_username)
                    )
                else:
                    conn.execute(
                        "INSERT INTO dingtalk_user_map (git_username, dingtalk_userid, dingtalk_mobile, remark) VALUES (?, ?, ?, ?)",
                        (git_username, dingtalk_userid, dingtalk_mobile, remark)
                    )
                conn.commit()
                print(f"[ReviewService] Dingtalk user map upserted: {git_username}")
                return True
        except Exception as e:
            print(f"[ReviewService] Error upserting dingtalk user map: {e}")
            return False

    @staticmethod
    def delete_dingtalk_user_map(git_username: str) -> bool:
        """
        删除钉钉用户映射

        :param git_username: Git 平台用户名
        :return: 是否成功
        """
        try:
            with get_db_connection() as conn:
                conn.execute(
                    "DELETE FROM dingtalk_user_map WHERE git_username = ?",
                    (git_username,)
                )
                conn.commit()
                print(f"[ReviewService] Dingtalk user map deleted: {git_username}")
                return True
        except Exception as e:
            print(f"[ReviewService] Error deleting dingtalk user map: {e}")
            return False

    @staticmethod
    def get_latest_authors_by_project(project_name: str, limit: int = 5) -> list:
        """
        根据项目名称查询最近的 PR/Push 提交者

        :param project_name: 项目名称
        :param limit: 返回数量
        :return: 去重的作者列表
        """
        try:
            with get_db_connection() as conn:
                # 先从 MR 日志中查找
                rows = conn.fetchall(
                    """SELECT DISTINCT author FROM mr_review_log
                       WHERE project_name = ?
                       ORDER BY updated_at DESC LIMIT ?""",
                    (project_name, limit)
                )
                authors = [row[0] for row in rows if row[0]]

                # 如果 MR 中没找到，从 Push 日志中查找
                if not authors:
                    rows = conn.fetchall(
                        """SELECT DISTINCT author FROM push_review_log
                           WHERE project_name = ?
                           ORDER BY updated_at DESC LIMIT ?""",
                        (project_name, limit)
                    )
                    authors = [row[0] for row in rows if row[0]]

                return authors
        except Exception as e:
            print(f"[ReviewService] Error querying latest authors: {e}")
            return []


# Initialize database
ReviewService.init_db()
