from dotenv import load_dotenv

load_dotenv("config/.env")

import atexit
import json
import os
import traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, request, jsonify, send_from_directory
import os

from src.gitlab.webhook_handler import slugify_url
from src.queue.worker import handle_merge_request_event, handle_push_event, handle_github_pull_request_event, \
    handle_github_push_event, handle_gitea_push_event, handle_gitea_pull_request_event
from src.service.review_service import ReviewService
from src.utils.code_reviewer import CodeReviewer
from src.utils.messaging import notifier
from src.utils.log import logger
from src.utils.queue import handle_queue
from src.utils.reporter import Reporter

from src.utils.config_checker import check_config
from src.llm.factory import Factory
import requests

api_app = Flask(__name__, static_folder='web', static_url_path='')

push_review_enabled = os.environ.get('PUSH_REVIEW_ENABLED', '0') == '1'


@api_app.route('/')
def home():
    return "<h2>The server is running.</h2>"


@api_app.route('/api/review/logs', methods=['GET'])
def get_review_logs():
    """获取审查日志数据"""
    try:
        # 获取查询参数
        review_type = request.args.get('type', 'mr')  # 'mr' 或 'push'
        authors = request.args.getlist('authors') if request.args.get('authors') else None
        project_names = request.args.getlist('project_names') if request.args.get('project_names') else None
        
        # 时间范围
        updated_at_gte = request.args.get('updated_at_gte')
        updated_at_lte = request.args.get('updated_at_lte')
        
        if updated_at_gte:
            updated_at_gte = int(updated_at_gte)
        else:
            updated_at_gte = None
            
        if updated_at_lte:
            updated_at_lte = int(updated_at_lte)
        else:
            updated_at_lte = None
        
        # 根据类型获取数据
        if review_type == 'push':
            df = ReviewService().get_push_review_logs(
                authors=authors,
                project_names=project_names,
                updated_at_gte=updated_at_gte,
                updated_at_lte=updated_at_lte
            )
        else:
            df = ReviewService().get_mr_review_logs(
                authors=authors,
                project_names=project_names,
                updated_at_gte=updated_at_gte,
                updated_at_lte=updated_at_lte
            )
        
        # 转换数据格式
        if df.empty:
            return jsonify({
                'data': [],
                'total': 0,
                'average_score': 0
            })
        
        # 格式化时间戳
        if 'updated_at' in df.columns:
            df['updated_at'] = df['updated_at'].apply(
                lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(ts, (int, float)) else ts
            )
        
        # 格式化代码变更
        if 'additions' in df.columns and 'deletions' in df.columns:
            df['delta'] = df.apply(
                lambda row: f"+{int(row['additions'])}  -{int(row['deletions'])}"
                if not pd.isna(row['additions']) and not pd.isna(row['deletions'])
                else "",
                axis=1
            )
        
        # 转换为字典列表
        records = df.to_dict(orient='records')
        
        # 计算统计信息
        total = len(records)
        average_score = df['score'].mean() if 'score' in df.columns and not df.empty else 0
        
        return jsonify({
            'data': records,
            'total': total,
            'average_score': float(average_score) if not pd.isna(average_score) else 0
        })
    except Exception as e:
        logger.error(f"Failed to get review logs: {e}")
        return jsonify({'error': str(e)}), 500


@api_app.route('/api/review/stats', methods=['GET'])
def get_review_stats():
    """获取统计数据用于图表"""
    try:
        review_type = request.args.get('type', 'mr')
        authors = request.args.getlist('authors') if request.args.get('authors') else None
        project_names = request.args.getlist('project_names') if request.args.get('project_names') else None
        updated_at_gte = request.args.get('updated_at_gte')
        updated_at_lte = request.args.get('updated_at_lte')
        
        if updated_at_gte:
            updated_at_gte = int(updated_at_gte)
        else:
            updated_at_gte = None
            
        if updated_at_lte:
            updated_at_lte = int(updated_at_lte)
        else:
            updated_at_lte = None
        
        if review_type == 'push':
            df = ReviewService().get_push_review_logs(
                authors=authors,
                project_names=project_names,
                updated_at_gte=updated_at_gte,
                updated_at_lte=updated_at_lte
            )
        else:
            df = ReviewService().get_mr_review_logs(
                authors=authors,
                project_names=project_names,
                updated_at_gte=updated_at_gte,
                updated_at_lte=updated_at_lte
            )
        
        if df.empty:
            return jsonify({
                'project_counts': [],
                'project_scores': [],
                'author_counts': [],
                'author_scores': [],
                'author_code_lines': []
            })
        
        # 项目提交次数
        project_counts = df['project_name'].value_counts().reset_index()
        project_counts.columns = ['name', 'count']
        
        # 项目平均分数
        project_scores = df.groupby('project_name')['score'].mean().reset_index()
        project_scores.columns = ['name', 'average_score']
        
        # 人员提交次数
        author_counts = df['author'].value_counts().reset_index()
        author_counts.columns = ['name', 'count']
        
        # 人员平均分数
        author_scores = df.groupby('author')['score'].mean().reset_index()
        author_scores.columns = ['name', 'average_score']
        
        # 人员代码行数
        author_code_lines = []
        if 'additions' in df.columns and 'deletions' in df.columns:
            df['total_lines'] = df['additions'] + df['deletions']
            author_code_lines_df = df.groupby('author')['total_lines'].sum().reset_index()
            author_code_lines_df.columns = ['name', 'code_lines']
            author_code_lines = author_code_lines_df.to_dict(orient='records')
        
        return jsonify({
            'project_counts': project_counts.to_dict(orient='records'),
            'project_scores': project_scores.to_dict(orient='records'),
            'author_counts': author_counts.to_dict(orient='records'),
            'author_scores': author_scores.to_dict(orient='records'),
            'author_code_lines': author_code_lines
        })
    except Exception as e:
        logger.error(f"Failed to get review stats: {e}")
        return jsonify({'error': str(e)}), 500


@api_app.route('/review/daily_report', methods=['GET'])
def daily_report():
    # 获取当前日期0点和23点59分59秒的时间戳
    start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    end_time = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0).timestamp()

    try:
        if push_review_enabled:
            df = ReviewService().get_push_review_logs(updated_at_gte=start_time, updated_at_lte=end_time)
        else:
            df = ReviewService().get_mr_review_logs(updated_at_gte=start_time, updated_at_lte=end_time)

        if df.empty:
            logger.info("No data to process.")
            return jsonify({'message': 'No data to process.'}), 200
        # 去重：基于 (author, message) 组合
        df_unique = df.drop_duplicates(subset=["author", "commit_messages"])
        # 按照 author 排序
        df_sorted = df_unique.sort_values(by="author")
        # 转换为适合生成日报的格式
        commits = df_sorted.to_dict(orient="records")
        # 生成日报内容
        report_txt = Reporter().generate_report(json.dumps(commits))
        # 发送钉钉通知
        notifier.send_notification(content=report_txt, msg_type="markdown", title="代码提交日报")

        # 返回生成的日报内容
        return json.dumps(report_txt, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Failed to generate daily report: {e}")
        return jsonify({'message': f"Failed to generate daily report: {e}"}), 500


@api_app.route('/api/sonarqube/quality-summary', methods=['GET'])
def get_sonarqube_quality_summary():
    """
    获取 SonarQube 质量汇总报告（基于 facets 数据）
    
    查询参数:
    - query: 搜索查询语句 (可选，默认："*-service")
    - page_size: 每页数量 (可选，默认：50)
    - send_notification: 是否发送钉钉通知 (可选，默认：false)
    
    返回:
    - total: 项目总数
    - facets: 各个维度的统计信息
    - projects: 项目列表
    """
    try:
        # 获取配置
        sonar_url = os.getenv('SONAR_URL', 'http://dev.jinliwangluo.com:9001')
        sonar_token = os.getenv('SONAR_TOKEN')
        
        if not sonar_token:
            return jsonify({
                'error': 'SonarQube token is required. Please set SONAR_TOKEN environment variable.'
            }), 400
        
        # 获取查询参数
        query = '*-service'
        page_size = 50
        send_notification = 'true'
        
        # 构建 API URL
        api_url = f"{sonar_url}/api/components/search_projects"
        params = {
            'ps': page_size,
            'facets': 'new_reliability_rating,new_security_rating,new_security_review_rating,new_maintainability_rating,new_coverage,new_duplicated_lines_density,new_lines,alert_status,languages,tags,qualifier',
            'f': 'analysisDate,leakPeriodDate',
            'filter': f'query = "{query}"',
            'asc': 'false'
        }
        
        headers = {
            'Authorization': f'Bearer {sonar_token}'
        }
        
        logger.info(f"Fetching SonarQube quality summary with query: {query}")
        
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 401:
            return jsonify({
                'error': 'Authentication failed. Please check your SONAR_TOKEN.'
            }), 401
        elif response.status_code != 200:
            return jsonify({
                'error': f'SonarQube API error: {response.status_code}',
                'details': response.text
            }), response.status_code
        
        data = response.json()
        
        # 提取 facets 数据
        components = data.get('components', [])
        facets = data.get('facets', [])
        
        # 整理 facets 统计信息
        facets_summary = {}
        for facet in facets:
            facet_key = facet.get('property')
            values = facet.get('values', [])
            facets_summary[facet_key] = [
                {
                    'value': v.get('val'),
                    'count': v.get('count')
                }
                for v in values
            ]
        
        # 整理项目列表
        projects = []
        for comp in components:
            project = {
                'key': comp.get('key'),
                'name': comp.get('name'),
                'analysisDate': comp.get('analysisDate'),
                'leakPeriodDate': comp.get('leakPeriodDate'),
                'metrics': {}
            }
            # 提取指标
            if 'qualifier' in comp:
                project['qualifier'] = comp.get('qualifier')
            projects.append(project)
        
        # 构建返回数据
        result = {
            'total': data.get('paging', {}).get('total', len(components)),
            'facets': facets_summary,
            'projects': projects[:10],  # 只返回前 10 个项目详情
            'timestamp': datetime.now().isoformat()
        }
        
        # 如果需要发送钉钉通知
        if send_notification:
            try:
                notification_content = generate_sonarqube_notification(facets_summary, len(components), query)
                # 打印通知内容到日志
                logger.info(f"Generated notification content:\n{notification_content}")
                # 发送钉钉通知
                notifier.send_notification(
                    content=notification_content,
                    msg_type="markdown",
                    title="SonarQube 质量汇总报告"
                )
                logger.info("SonarQube notification sent successfully")
            except Exception as e:
                logger.error(f"Failed to send notification: {e}")
        
        return jsonify(result), 200
        
    except requests.exceptions.Timeout:
        logger.error("SonarQube API request timeout")
        return jsonify({
            'error': 'SonarQube API request timeout. Please try again later.'
        }), 504
    except requests.exceptions.RequestException as e:
        logger.error(f"SonarQube API request failed: {str(e)}")
        return jsonify({
            'error': f'Failed to connect to SonarQube: {str(e)}'
        }), 503
    except Exception as e:
        logger.error(f"Failed to get SonarQube quality summary: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


def generate_sonarqube_notification(facets_summary, total_projects, query):
    """
    生成钉钉通知内容
    
    :param facets_summary: facets 统计数据
    :param total_projects: 总项目数
    :param query: 搜索查询
    :return: Markdown 格式的通知内容
    """
    # 获取 SonarQube URL
    sonar_url = os.getenv('SONAR_URL', 'http://dev.jinliwangluo.com:9001')
    
    # 提取各项统计数据
    alert_status = facets_summary.get('alert_status', [])
    reliability_rating = facets_summary.get('new_reliability_rating', [])
    security_rating = facets_summary.get('new_security_rating', [])
    coverage = facets_summary.get('new_coverage', [])
    duplications = facets_summary.get('new_duplicated_lines_density', [])
    maintainability = facets_summary.get('new_maintainability_rating', [])
    
    # 统计通过质量门的项目数
    quality_gate_ok = sum(v['count'] for v in alert_status if v['value'] == 'OK')
    # quality_gate_warn = sum(v['count'] for v in alert_status if v['value'] == 'WARN')
    quality_gate_error = sum(v['count'] for v in alert_status if v['value'] == 'ERROR')
    
    # 统计可靠性评级分布
    reliability_a = sum(v['count'] for v in reliability_rating if v['value'] == '1')
    reliability_b = sum(v['count'] for v in reliability_rating if v['value'] == '2')
    reliability_cde = sum(v['count'] for v in reliability_rating if v['value'] in ['3', '4', '5'])
    
    # 统计安全性评级分布
    security_a = sum(v['count'] for v in security_rating if v['value'] == '1')
    security_b = sum(v['count'] for v in security_rating if v['value'] == '2')
    security_cde = sum(v['count'] for v in security_rating if v['value'] in ['3', '4', '5'])
    
    # 计算平均覆盖率（取中间值）
    avg_coverage = 0
    if coverage:
        coverage_values = []
        for v in coverage:
            val = v.get('value')
            if val is not None:
                try:
                    coverage_values.append(float(val))
                except (ValueError, TypeError):
                    continue
        if coverage_values:
            avg_coverage = sum(coverage_values) / len(coverage_values)
    
    # 计算平均重复率
    avg_duplications = 0
    if duplications:
        duplication_values = []
        for v in duplications:
            val = v.get('value')
            if val is not None:
                try:
                    duplication_values.append(float(val))
                except (ValueError, TypeError):
                    continue
        if duplication_values:
            avg_duplications = sum(duplication_values) / len(duplication_values)
    
    # 生成 Markdown 内容
    content = f"""# 📊 SonarQube 质量汇总报告

**查询条件**: `{query}`  
**总项目数**: {total_projects}  
**质量要求**: 宽松模式（管控严重问题）\n
**统计时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🎯 █ 质量门禁状态

| 状态 | 项目数 | 占比 |
|------|--------|------|
| <font color='green'>**☑ 通过**</font> | {quality_gate_ok} | {quality_gate_ok/total_projects*100:.1f}% |
| <font color='red'>**✘ 失败**</font> | **{quality_gate_error}** | {quality_gate_error/total_projects*100:.1f}% |

## 🔒 █ 安全性评级 

| 等级 | 项目数 | 占比 |
|------|--------|------|
| A | {security_a} | {security_a/total_projects*100:.1f}% |
| B | {security_b} | {security_b/total_projects*100:.1f}% |
| <font color='red'>**C-E**</font> | **{security_cde}** | {security_cde/total_projects*100:.1f}% |

### 📈 █ 可靠性评级

| 等级 | 项目数 | 说明 |
|------|--------|------|
| A | {reliability_a} | 优秀 |
| B | {reliability_b} | 良好 |
| <font color='red'>**C-E**</font> | **{reliability_cde}** | 需改进 |

## ▓ 关键指标

- **平均单元测试覆盖率**: {avg_coverage:.1f}%
- **平均代码重复率**: {avg_duplications:.1f}%

## ⚠ █ 优化建议

"""
    
    # 添加建议
    suggestions = []
    if quality_gate_error > 0:
        suggestions.append(f"- <font color='red'>**✘**</font> {quality_gate_error} 个项目质量门失败，需要优先处理")
    
    if reliability_cde > 0:
        suggestions.append(f"- 🐛 {reliability_cde} 个项目可靠性评级较差 (C-E)")
    
    if security_cde > 0:
        suggestions.append(f"- 🔒 {security_cde} 个项目安全性评级较差 (C-E)")
    
    if avg_coverage < 80:
        suggestions.append(f"- 📊 平均测试覆盖率低于 80% ({avg_coverage:.1f}%)")
    
    if avg_duplications > 5:
        suggestions.append(f"- 📄 平均代码重复率高于 5% ({avg_duplications:.1f}%)")
    
    if not suggestions:
        suggestions.append("- ✅ 整体质量状况良好，继续保持！")
    
    content += "\n".join(suggestions)
    
    content += f"\n\n查看详情：[{sonar_url}]({sonar_url}/projects)"
    
    return content



# 昨日 code review
@api_app.route('/review/yesterday_report', methods=['GET'])
def yesterday_mr_top10():
    """
    获取昨天 GitLab Merge Request 的 AI 分析记录明细，整合并投递给 AI 分析 Top10
    按分数排序（分数越低表示问题越多），返回 Top10 需要重点关注的 MR
    """
    try:
        # 获取昨天的 MR 记录
        df = ReviewService.get_yesterday_mr_review_logs()

        if df.empty:
            logger.info("No MR review logs for yesterday.")
            return jsonify({
                'message': 'No MR review logs for yesterday.',
                'date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                'total_count': 0,
                'top10': []
            }), 200

        # 获取 Top10（分数最低的）
        top10_df = ReviewService.get_top10_mr_by_score(df)

        # 格式化时间戳
        if 'updated_at' in top10_df.columns:
            top10_df['updated_at'] = top10_df['updated_at'].apply(
                lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(ts, (int, float)) else ts
            )

        # 格式化代码变更
        if 'additions' in top10_df.columns and 'deletions' in top10_df.columns:
            top10_df['delta'] = top10_df.apply(
                lambda row: f"+{int(row['additions'])}  -{int(row['deletions'])}"
                if not pd.isna(row['additions']) and not pd.isna(row['deletions'])
                else "",
                axis=1
            )

        # 转换为适合 AI 分析的格式
        top10_records = top10_df.to_dict(orient='records')

        # 构建投递给 AI 的数据
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        ai_analysis_prompt = f"""
时间: ({yesterday_str}) GitLab Merge Request 代码审查记录中，分数最低的 Top10 MR 明细如下
"""
        for i, record in enumerate(top10_records, 1):
            ai_analysis_prompt += f"""
### Top {i}
- **项目**: {record.get('project_name', 'N/A')}
- **作者**: {record.get('author', 'N/A')}
- **目标分支**: {record.get('target_branch', 'N/A')}
- **分数**: {record.get('score', 0)} 分
- **代码变更**: +{int(record.get('additions', 0))} -{int(record.get('deletions', 0))}
- **AI审查结果**: {record.get('review_result', 'N/A')[:500]}...

"""

        ai_analysis_prompt += """
请分析以上 Top10 代码审查记录，总结以下内容：
1. 主要存在的问题和代码质量趋势
2. 需要重点关注的项目或开发者
3. 改进建议
请以 Markdown 格式返回分析报告。
"""

        # 调用 AI 进行分析
        client = Factory().getClient()
        ai_result = client.completions(
            messages=[
                {"role": "user", "content": ai_analysis_prompt}
            ],
        )

        # 发送通知
        notifier.send_notification(
            content=f"{yesterday_str} GitLab MR 代码审查 Top10 分析报告:\n\n{ai_result}",
            msg_type="markdown",
            title=f"{yesterday_str} MR 代码审查 Top10 分析"
        )

        return jsonify({
            'date': yesterday_str,
            'total_count': len(df),
            'top10_count': len(top10_records),
            'top10': top10_records,
            'ai_analysis': ai_result
        }), 200

    except Exception as e:
        logger.error(f"Failed to generate yesterday MR top10 analysis: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500



def setup_scheduler():
    """
    配置并启动定时任务调度器
    """
    try:
        scheduler = BackgroundScheduler()
        
        # 1. GitLab MR 日报定时任务
        crontab_expression = os.getenv('REPORT_CRONTAB_EXPRESSION', '0 17 * * 1-5')
        cron_parts = crontab_expression.split()
        cron_minute, cron_hour, cron_day, cron_month, cron_day_of_week = cron_parts

        scheduler.add_job(
            yesterday_mr_top10,
            trigger=CronTrigger(
                minute=cron_minute,
                hour=cron_hour,
                day=cron_day,
                month=cron_month,
                day_of_week=cron_day_of_week
            )
        )
        
        # 2. SonarQube 质量日报定时任务
        sonar_crontab = os.getenv('SONAR_REPORT_CRONTAB_EXPRESSION', '0 16 * * 1-5')  # 工作日
        sonar_cron_parts = sonar_crontab.split()
        sonar_minute, sonar_hour, sonar_day, sonar_month, sonar_day_of_week = sonar_cron_parts
        
        scheduler.add_job(
            get_sonarqube_quality_summary,
            trigger=CronTrigger(
                minute=sonar_minute,
                hour=sonar_hour,
                day=sonar_day,
                month=sonar_month,
                day_of_week=sonar_day_of_week
            ),
            id='sonarqube_daily_report',
            name='SonarQube Daily Report'
        )

        # Start the scheduler
        scheduler.start()
        logger.info("Scheduler started successfully.")

        # Shut down the scheduler when exiting the app
        atexit.register(lambda: scheduler.shutdown())
    except Exception as e:
        logger.error(f"Error setting up scheduler: {e}")
        logger.error(traceback.format_exc())


# 处理 GitLab Merge Request Webhook
@api_app.route('/review/webhook', methods=['POST'])
def handle_webhook():
    # 记录请求头信息，用于调试
    logger.debug(f'Request headers: {dict(request.headers)}')
    logger.debug(f'Content-Type: {request.content_type}')
    
    # 获取请求的JSON数据
    # 尝试多种方式获取 JSON 数据
    data = None
    if request.is_json:
        data = request.get_json()
    else:
        # 如果 Content-Type 不是 application/json，尝试直接解析
        try:
            if request.data:
                data = json.loads(request.data)
                logger.info('Parsed JSON from request.data')
        except Exception as e:
            logger.error(f'Failed to parse JSON: {str(e)}')
            return jsonify({"error": f"Invalid JSON format: {str(e)}"}), 400
    
    if not data:
        logger.error('No data found in request')
        return jsonify({"error": "Invalid JSON or empty data"}), 400

    # 判断 webhook 来源
    # 注意：Gitea 为了兼容性会同时发送 X-GitHub-Event 和 X-Gitea-Event
    # 所以需要优先检查 X-Gitea-Event（如果存在，一定是 Gitea）
    github_event = request.headers.get('X-GitHub-Event')
    gitea_event = request.headers.get('X-Gitea-Event')
    
    logger.debug(f'GitHub event: {github_event}, Gitea event: {gitea_event}')

    # 优先识别 Gitea（因为 Gitea 会同时发送两种 header，但 GitHub 不会发送 Gitea header）
    if gitea_event:  # Gitea webhook（优先）
        return handle_gitea_webhook(gitea_event, data)
    elif github_event:  # GitHub webhook
        return handle_github_webhook(github_event, data)
    else:  # GitLab webhook（默认）
        return handle_gitlab_webhook(data)


def handle_github_webhook(event_type, data):
    # 获取GitHub配置
    github_token = os.getenv('GITHUB_ACCESS_TOKEN') or request.headers.get('X-GitHub-Token')
    if not github_token:
        return jsonify({'message': 'Missing GitHub access token'}), 400

    github_url = os.getenv('GITHUB_URL') or 'https://github.com'
    github_url_slug = slugify_url(github_url)

    # 打印整个payload数据
    logger.info(f'Received GitHub event: {event_type}')
    logger.info(f'Payload: {json.dumps(data)}')

    if event_type == "pull_request":
        # 使用handle_queue进行异步处理
        handle_queue(handle_github_pull_request_event, data, github_token, github_url, github_url_slug)
        # 立马返回响应
        return jsonify(
            {'message': f'GitHub request received(event_type={event_type}), will process asynchronously.'}), 200
    elif event_type == "push":
        # 使用handle_queue进行异步处理
        handle_queue(handle_github_push_event, data, github_token, github_url, github_url_slug)
        # 立马返回响应
        return jsonify(
            {'message': f'GitHub request received(event_type={event_type}), will process asynchronously.'}), 200
    else:
        error_message = f'Only pull_request and push events are supported for GitHub webhook, but received: {event_type}.'
        logger.error(error_message)
        return jsonify(error_message), 400


def handle_gitlab_webhook(data):
    object_kind = data.get("object_kind")

    # 优先从请求头获取，如果没有，则从环境变量获取，如果没有，则从推送事件中获取
    gitlab_url = os.getenv('GITLAB_URL') or request.headers.get('X-Gitlab-Instance')
    if not gitlab_url:
        repository = data.get('repository')
        if not repository:
            return jsonify({'message': 'Missing GitLab URL'}), 400
        homepage = repository.get("homepage")
        if not homepage:
            return jsonify({'message': 'Missing GitLab URL'}), 400
        try:
            parsed_url = urlparse(homepage)
            gitlab_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
        except Exception as e:
            return jsonify({"error": f"Failed to parse homepage URL: {str(e)}"}), 400

    # 优先从环境变量获取，如果没有，则从请求头获取
    gitlab_token = os.getenv('GITLAB_ACCESS_TOKEN') or request.headers.get('X-Gitlab-Token')
    # 如果gitlab_token为空，返回错误
    if not gitlab_token:
        return jsonify({'message': 'Missing GitLab access token'}), 400

    gitlab_url_slug = slugify_url(gitlab_url)

    # 打印整个payload数据，或根据需求进行处理
    logger.info(f'Received event: {object_kind}')
    logger.info(f'Payload: {json.dumps(data)}')

    # 处理Merge Request Hook
    if object_kind == "merge_request":
        # 创建一个新进程进行异步处理
        handle_queue(handle_merge_request_event, data, gitlab_token, gitlab_url, gitlab_url_slug)
        # 立马返回响应
        return jsonify(
            {'message': f'Request received(object_kind={object_kind}), will process asynchronously.'}), 200
    elif object_kind == "push":
        # 创建一个新进程进行异步处理
        # TODO check if PUSH_REVIEW_ENABLED is needed here
        handle_queue(handle_push_event, data, gitlab_token, gitlab_url, gitlab_url_slug)
        # 立马返回响应
        return jsonify(
            {'message': f'Request received(object_kind={object_kind}), will process asynchronously.'}), 200
    else:
        error_message = f'Only merge_request and push events are supported (both Webhook and System Hook), but received: {object_kind}.'
        logger.error(error_message)
        return jsonify(error_message), 400


def handle_gitea_webhook(event_type, data):
    logger.info(f'Processing Gitea webhook, event_type: {event_type}')
    
    # 获取 Gitea 配置
    gitea_token = os.getenv('GITEA_ACCESS_TOKEN') or request.headers.get('X-Gitea-Token')
    if not gitea_token:
        error_msg = 'Missing Gitea access token. Please set GITEA_ACCESS_TOKEN environment variable or provide X-Gitea-Token header.'
        logger.error(error_msg)
        return jsonify({'message': error_msg}), 400

    # 获取 Gitea URL
    gitea_url = os.getenv('GITEA_URL') or request.headers.get('X-Gitea-Instance')
    if not gitea_url:
        # 从 payload 中提取
        repository = data.get('repository', {})
        logger.debug(f'Repository data: {repository}')
        if repository:
            html_url = repository.get('html_url', '')
            logger.debug(f'HTML URL from repository: {html_url}')
            if html_url:
                try:
                    parsed_url = urlparse(html_url)
                    gitea_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    logger.info(f'Extracted Gitea URL from payload: {gitea_url}')
                except Exception as e:
                    error_msg = f"Failed to parse repository URL: {str(e)}"
                    logger.error(error_msg)
                    return jsonify({"error": error_msg}), 400
        if not gitea_url:
            error_msg = 'Missing Gitea URL. Please set GITEA_URL environment variable, provide X-Gitea-Instance header, or ensure repository.html_url is present in payload.'
            logger.error(error_msg)
            logger.debug(f'Payload keys: {list(data.keys())}')
            return jsonify({'message': error_msg}), 400

    # URL Slug 用于队列隔离和日志标识
    gitea_url_slug = slugify_url(gitea_url)

    logger.info(f'Received Gitea event: {event_type}')
    logger.info(f'Gitea URL: {gitea_url}')
    logger.debug(f'Payload: {json.dumps(data, ensure_ascii=False)}')

    # Push 事件优先级更高，先处理 Push
    if event_type == "push":
        handle_queue(handle_gitea_push_event, data, gitea_token, gitea_url, gitea_url_slug)
        return jsonify(
            {'message': f'Gitea request received(event_type={event_type}), will process asynchronously.'}), 200
    elif event_type == "pull_request":
        # 只处理 opened 和 synchronize action
        action = data.get('action', '')
        logger.debug(f'Pull Request action: {action}')
        if action not in ['opened', 'synchronize']:
            logger.info(f"Gitea Pull Request event, action={action}, ignored.")
            return jsonify(
                {'message': f'Gitea Pull Request event with action={action} is ignored, only opened and synchronize are supported.'}), 200
        handle_queue(handle_gitea_pull_request_event, data, gitea_token, gitea_url, gitea_url_slug)
        return jsonify(
            {'message': f'Gitea request received(event_type={event_type}), will process asynchronously.'}), 200
    elif event_type == "issue_comment":
        # issue_comment 事件：当 Issue 或 PR 上添加评论时触发
        # 由于我们的系统会自动在 Issue 上添加评论，Gitea 会发送这个 webhook
        # 我们不需要处理这个事件，静默忽略即可
        logger.debug(f'Gitea issue_comment event received, ignored (auto-generated by our system).')
        return jsonify(
            {'message': f'Gitea issue_comment event received and ignored (auto-generated by review system).'}), 200
    else:
        error_message = f'Only pull_request, push, and issue_comment events are supported for Gitea webhook, but received: {event_type}.'
        logger.error(error_message)
        return jsonify(error_message), 400


if __name__ == '__main__':
    check_config()
    # 启动定时任务调度器
    setup_scheduler()

    # 启动Flask API服务
    port = int(os.environ.get('SERVER_PORT', 5001))
    api_app.run(host='0.0.0.0', port=port)
