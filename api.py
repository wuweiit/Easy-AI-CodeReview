from dotenv import load_dotenv

load_dotenv("config/.env")

import atexit
import json
import os
import re
import traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, request, jsonify, send_from_directory
import os

from src.gitlab.webhook_handler import slugify_url, get_mr_commit_authors, get_mr_submit_author
from src.queue.worker import handle_merge_request_event, handle_push_event, handle_github_pull_request_event, \
    handle_github_push_event, handle_gitea_push_event, handle_gitea_pull_request_event
from src.service.review_service import ReviewService
from src.utils.code_reviewer import CodeReviewer
from src.utils.messaging import notifier
from src.utils.messaging.notifier import send_dingtalk_work_notification
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


@api_app.route('/api/sonarqube/webhook', methods=['POST'])
def handle_sonarqube_webhook():
    """
    接收 SonarQube 质量门禁 Webhook 通知
    触发时机：项目分析完成后，根据质量门状态发送通知

    SonarQube Webhook 发送的数据格式：
    {
        "serverUrl": "http://localhost:9000",
        "taskId": "e5aff8b8-1daa-4a9e-95d4-e6eba53868b7",
        "status": "SUCCESS",
        "project": {
            "key": "my-project",
            "name": "My Project",
            "url": "http://localhost:9000/dashboard?id=my-project"
        },
        "qualityGate": {
            "status": "OK" | "ERROR",
            "name": "Quality Gate Name",
            "conditions": [
                {
                    "metric": "new_coverage",
                    "operator": "LESS_THAN",
                    "value": "80",
                    "threshold": "80",
                    "status": "OK" | "ERROR"
                }
            ]
        },
        "analysisId": "e5aff8b8-1daa-4a9e-95d4-e6eba53868b7",
        "analysis": {
            "date": "2023-01-01T12:00:00+0000"
        }
    }
    """
    try:
        # 获取 JSON 数据
        data = request.get_json()
        if not data:
            logger.error("SonarQube webhook: No JSON data received")
            return jsonify({"error": "No JSON data received"}), 400

        logger.info(f"Received SonarQube webhook: {json.dumps(data, ensure_ascii=False)}")

        # 提取关键信息
        project = data.get('project', {})
        branch = data.get('branch', {})
        project_key = project.get('key', 'Unknown')
        project_name = project.get('name', project_key)
        project_url = branch.get('url', '')

        quality_gate = data.get('qualityGate', {})
        gate_status = quality_gate.get('status', 'UNKNOWN')
        gate_name = quality_gate.get('name', 'Unknown')
        conditions = quality_gate.get('conditions', [])

        analysis_date = data.get('analysis', {}).get('date', '')
        server_url = data.get('serverUrl', os.getenv('SONAR_URL', ''))

        # 解析分析时间
        # SonarQube 的时间格式如: 2024-01-01T12:00:00+0800
        analysis_time_str = ''
        try:
            if analysis_date:
                # 兼容多种时区格式: +0800, +0000, Z
                clean_date = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', analysis_date)
                analysis_dt = datetime.fromisoformat(clean_date)
                analysis_time_str = analysis_dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            analysis_time_str = analysis_date or datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 判断是否需要发送通知（只在 ERROR 状态时发送，或者配置为总是发送）
        notify_on_success = os.environ.get('SONAR_WEBHOOK_NOTIFY_ON_SUCCESS', 'false').lower() == 'true'

        if gate_status != 'ERROR' and not notify_on_success:
            logger.info(f"SonarQube quality gate passed for {project_name}, skipping notification")
            return jsonify({
                "message": "Quality gate passed, notification skipped",
                "project": project_name,
                "status": gate_status
            }), 200

        # ========== 通过 PR 提交者精准通知 ==========
        # 从 SonarQube webhook payload 提取分支/PR 信息
        branch_info = data.get('branch', {})
        branch_name = branch_info.get('name', '')
        branch_type = branch_info.get('type', '')  # PULL_REQUEST 或 BRANCH

        # 当 SonarQube 分析的是 PR 时，branch.name 就是 MR 编号
        mr_iid = None
        if branch_type == 'PULL_REQUEST' and branch_name:
            mr_iid = branch_name
            logger.info(f"SonarQube webhook: detected PR analysis, MR IID={mr_iid}")
            # 补充 pullRequest 参数，使链接跳转到 PR 分析页
            if project_url and 'pullRequest' not in project_url:
                separator = '&' if '?' in project_url else '?'
                project_url = f"{project_url}{separator}pullRequest={mr_iid}"
        else:
            # 非 PR 分析场景，尝试从 properties 中获取 MR 编号
            properties = data.get('properties', {})
            mr_iid = properties.get('sonar.pullrequest.key') or properties.get('sonar.analysis.mrIid')
            if mr_iid:
                logger.info(f"SonarQube webhook: found MR IID from properties: {mr_iid}")

        # 1. 根据 MR 编号，通过 GitLab API 查询该 MR 内所有 commit 的开发者 + MR 发起者
        authors = []
        mr_submitter = None
        if mr_iid:
            # 查询 MR 内 commit 的开发者
            # authors = get_mr_commit_authors(project_key, mr_iid)
            # if not authors and project_name != project_key:
            #     authors = get_mr_commit_authors(project_name, mr_iid)

            # 查询 MR 发起者
            mr_submitter = get_mr_submit_author(project_key, mr_iid)
            if not mr_submitter and project_name != project_key:
                mr_submitter = get_mr_submit_author(project_name, mr_iid)

            # 将 MR 发起者合并到 authors 列表（去重）
            if mr_submitter and mr_submitter.get('username'):
                submitter_name = mr_submitter['username']
                if submitter_name not in authors:
                    authors.append(submitter_name)
        else:
            logger.warning(f"SonarQube webhook: no MR IID found in payload for {project_key}, cannot identify PR authors")

        logger.info(f"SonarQube webhook: MR !{mr_iid} authors for {project_key}: {authors} (submitter: {mr_submitter})")

        # 2. 从映射表中查询每个提交者的钉钉用户信息
        target_user_ids = []
        target_mobiles = []
        matched_authors = []
        unmatched_authors = []

        for author in authors:
            user_map = ReviewService.get_dingtalk_user_by_git_username(author)
            if user_map:
                matched_authors.append(author)
                if user_map.get('dingtalk_userid'):
                    target_user_ids.append(user_map['dingtalk_userid'])
                elif user_map.get('dingtalk_mobile'):
                    target_mobiles.append(user_map['dingtalk_mobile'])
            else:
                unmatched_authors.append(author)

        if unmatched_authors:
            logger.warning(f"SonarQube webhook: no DingTalk mapping for git users: {unmatched_authors}")

        # 3. 确定最终的通知方式
        #    优先精准通知到人，如果没有匹配到任何钉钉用户则跳过通知
        user_ids = target_user_ids if target_user_ids else None
        mobiles = target_mobiles if target_mobiles else None
        is_to_all = False
        dept_ids = None

        if not user_ids and not mobiles:
            logger.info(f"No matched DingTalk users for authors {unmatched_authors}, skipping notification")
            return jsonify({
                "message": "Webhook processed, but no DingTalk user mapping found, notification skipped",
                "project": project_name,
                "status": gate_status,
                "notification_sent": False,
                "matched_authors": matched_authors,
                "unmatched_authors": unmatched_authors
            }), 200
        else:
            logger.info(f"SonarQube webhook: sending to matched authors: {matched_authors}, user_ids={user_ids}, mobiles={mobiles}")

        # 生成通知标题和内容
        if gate_status == 'ERROR':
            title = f"🚨 质量门禁失败 - {project_name}"
        else:
            title = f"✅ 质量门禁通过 - {project_name}"

        # 从 MR 详情中获取源分支和 web_url（mr_submitter 已在前面查询过）
        mr_web_url = ''
        mr_source_branch = ''
        if mr_submitter:
            mr_web_url = mr_submitter.get('web_url', '')
            mr_source_branch = mr_submitter.get('source_branch', '')

        content = generate_sonarqube_webhook_content(
            project_name=project_name,
            project_key=project_key,
            project_url=project_url,
            gate_status=gate_status,
            gate_name=gate_name,
            conditions=conditions,
            analysis_time=analysis_time_str,
            server_url=server_url,
            mr_iid=mr_iid,
            mr_web_url=mr_web_url,
            branch_name=mr_source_branch or branch_name
        )

        # 发送钉钉工作通知
        success = send_dingtalk_work_notification(
            title=title,
            content=content,
            user_ids=user_ids,
            dept_ids=dept_ids,
            mobiles=mobiles,
            is_to_all=is_to_all,
            msg_type='markdown'
        )

        if success:
            logger.info(f"SonarQube webhook notification sent successfully for {project_name}")
        else:
            logger.warning(f"Failed to send SonarQube webhook notification for {project_name}")

        return jsonify({
            "message": "Webhook processed",
            "project": project_name,
            "status": gate_status,
            "notification_sent": success,
            "matched_authors": matched_authors,
            "unmatched_authors": unmatched_authors
        }), 200

    except Exception as e:
        logger.error(f"Error processing SonarQube webhook: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


def generate_sonarqube_webhook_content(project_name, project_key, project_url, gate_status,
                                       gate_name, conditions, analysis_time, server_url,
                                       mr_iid=None, mr_web_url=None, branch_name=None):
    """
    生成 SonarQube Webhook 的 Markdown 通知内容

    :param project_name: 项目名称
    :param project_key: 项目 Key
    :param project_url: 项目链接
    :param gate_status: 质量门状态（OK/ERROR）
    :param gate_name: 质量门名称
    :param conditions: 质量门条件列表
    :param analysis_time: 分析时间
    :param server_url: SonarQube 服务器地址
    :param mr_iid: MR 编号
    :param mr_web_url: MR 的 GitLab 页面链接
    :param branch_name: 分支名称
    :return: Markdown 格式的通知内容
    """
    # 状态相关的样式
    if gate_status == 'ERROR':
        status_icon = "🔴"
        status_color = "red"
        status_text = "**失败**"
    else:
        status_icon = "🟢"
        status_color = "green"
        status_text = "**通过**"

    # 构建通知内容
    content = f"""## {status_icon} PR-代码质量通知\n

**项目名称**: {project_name}\n
"""

    if mr_iid:
        if mr_web_url:
            content += f"""**合并请求**: [!{mr_iid}]({mr_web_url})\n
            """
        else:
            content += f"""**合并请求**: !{mr_iid}\n
            """
    if branch_name:
        content += f"""**Pull分支**: {branch_name}\n
        """

    content += f"""
**质量门**: {gate_name}\n
**质量门状态**: {status_icon} {status_text}\n

"""

    # 添加失败的条件详情
    if conditions:
        content += "\n\n"
        content += "| 指标 | 操作符 | 阈值 | 实际值 | 状态 |\n"
        content += "|------|--------|------|--------|------|\n"

        failed_conditions = []
        for cond in conditions:
            metric = cond.get('metric', 'Unknown')
            operator = cond.get('operator', '')
            threshold = cond.get('threshold', '-')
            value = cond.get('value', '-')
            status = cond.get('status', 'UNKNOWN')

            # 映射指标名称为中文
            metric_cn = {
                'new_coverage': '新代码覆盖率',
                'new_bugs': '新增 Bug',
                'new_vulnerabilities': '新增漏洞',
                'new_code_smells': '新增代码异味',
                'new_duplicated_lines_density': '新增重复行密度',
                'duplicated_blocks': '重复代码块',
                'coverage': '代码覆盖率',
                'bugs': 'Bug 数',
                'vulnerabilities': '漏洞数',
                'code_smells': '代码异味数',
                'sqale_rating': '可维护性评级',
                'reliability_rating': '可靠性评级',
                'security_rating': '安全性评级',
                'security_hotspots_reviewed': '安全热点已审查',
                'new_security_hotspots_reviewed': '新增安全热点已审查',
                'new_critical_violations': '新增严重违规',
            }.get(metric, metric)

            # 映射操作符为中文
            operator_cn = {
                'LESS_THAN': '<',
                'GREATER_THAN': '>',
                'EQUALS': '=',
            }.get(operator, operator)

            # 状态图标
            # NO_VALUE 表示该指标无数据（如没有安全热点），视为通过
            if status in ('OK', 'NO_VALUE'):
                cond_status = "✅"
            else:
                cond_status = "❌"
                failed_conditions.append((metric_cn, value, threshold))

            content += f"| {metric_cn} | {operator_cn} | {threshold} | {value} | {cond_status} |\n"

    # 添加失败原因说明
    if failed_conditions:
        content += "\n### ⚠️ 未达标项\n\n"
        for metric, val, threshold in failed_conditions:
            content += f"- **{metric}**: 实际值 `{val}` 超过阈值 `{threshold}`\n"

    # 添加项目链接
    if project_url:
        content += f"\n[**查看详情**]({project_url})\n"

    return content


# ==================== 钉钉用户映射管理 API ====================

@api_app.route('/api/dingtalk/user-map', methods=['GET'])
def get_dingtalk_user_maps():
    """获取所有 Git 用户名到钉钉用户的映射列表"""
    try:
        maps = ReviewService.get_all_dingtalk_user_maps()
        return jsonify({'data': maps, 'total': len(maps)}), 200
    except Exception as e:
        logger.error(f"Failed to get dingtalk user maps: {e}")
        return jsonify({'error': str(e)}), 500


@api_app.route('/api/dingtalk/user-map', methods=['POST'])
def create_or_update_dingtalk_user_map():
    """
    新增或更新 Git 用户名到钉钉用户的映射

    请求体:
    {
        "git_username": "zhangsan",
        "dingtalk_userid": "xxx",     // 可选，钉钉用户ID
        "dingtalk_mobile": "138xxxx", // 可选，手机号（至少填一个）
        "remark": "张三"               // 可选
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data received'}), 400

        git_username = data.get('git_username', '').strip()
        if not git_username:
            return jsonify({'error': 'git_username is required'}), 400

        dingtalk_userid = data.get('dingtalk_userid', '').strip() or None
        dingtalk_mobile = data.get('dingtalk_mobile', '').strip() or None
        remark = data.get('remark', '').strip() or None

        if not dingtalk_userid and not dingtalk_mobile:
            return jsonify({'error': 'At least one of dingtalk_userid or dingtalk_mobile is required'}), 400

        success = ReviewService.upsert_dingtalk_user_map(
            git_username=git_username,
            dingtalk_userid=dingtalk_userid,
            dingtalk_mobile=dingtalk_mobile,
            remark=remark
        )

        if success:
            return jsonify({'message': f'User map for {git_username} saved successfully'}), 200
        else:
            return jsonify({'error': 'Failed to save user map'}), 500
    except Exception as e:
        logger.error(f"Failed to create/update dingtalk user map: {e}")
        return jsonify({'error': str(e)}), 500


@api_app.route('/api/dingtalk/user-map/<git_username>', methods=['DELETE'])
def delete_dingtalk_user_map(git_username):
    """删除 Git 用户名到钉钉用户的映射"""
    try:
        success = ReviewService.delete_dingtalk_user_map(git_username)
        if success:
            return jsonify({'message': f'User map for {git_username} deleted successfully'}), 200
        else:
            return jsonify({'error': 'Failed to delete user map'}), 500
    except Exception as e:
        logger.error(f"Failed to delete dingtalk user map: {e}")
        return jsonify({'error': str(e)}), 500


@api_app.route('/api/dingtalk/user-map/batch', methods=['POST'])
def batch_create_dingtalk_user_maps():
    """
    批量导入 Git 用户名到钉钉用户的映射

    请求体:
    {
        "mappings": [
            {"git_username": "zhangsan", "dingtalk_mobile": "138xxxx", "remark": "张三"},
            {"git_username": "lisi", "dingtalk_userid": "xxx", "remark": "李四"}
        ]
    }
    """
    try:
        data = request.get_json()
        if not data or 'mappings' not in data:
            return jsonify({'error': 'mappings field is required'}), 400

        mappings = data['mappings']
        success_count = 0
        failed = []

        for item in mappings:
            git_username = item.get('git_username', '').strip()
            if not git_username:
                failed.append({'item': item, 'reason': 'git_username is required'})
                continue

            dingtalk_userid = item.get('dingtalk_userid', '').strip() or None
            dingtalk_mobile = item.get('dingtalk_mobile', '').strip() or None
            remark = item.get('remark', '').strip() or None

            if not dingtalk_userid and not dingtalk_mobile:
                failed.append({'item': item, 'reason': 'At least one of dingtalk_userid or dingtalk_mobile is required'})
                continue

            if ReviewService.upsert_dingtalk_user_map(
                git_username=git_username,
                dingtalk_userid=dingtalk_userid,
                dingtalk_mobile=dingtalk_mobile,
                remark=remark
            ):
                success_count += 1
            else:
                failed.append({'item': item, 'reason': 'Database error'})

        return jsonify({
            'message': f'Batch import completed: {success_count} succeeded, {len(failed)} failed',
            'success_count': success_count,
            'failed_count': len(failed),
            'failed': failed
        }), 200
    except Exception as e:
        logger.error(f"Failed to batch create dingtalk user maps: {e}")
        return jsonify({'error': str(e)}), 500


@api_app.route('/api/dingtalk/user-map/sync-gitlab', methods=['POST'])
def sync_gitlab_users_to_dingtalk_map():
    """
    将 GitLab 用户清单同步到 dingtalk_user_map 表中
    已存在的用户跳过，仅插入新用户

    请求体（可选）:
    {
        "gitlab_url": "https://gitlab.example.com",   // 可选，默认从环境变量读取
        "gitlab_token": "xxx"                          // 可选，默认从环境变量读取
    }
    """
    try:
        data = request.get_json(silent=True) or {}
        gitlab_url = data.get('gitlab_url', '').strip() or os.getenv('GITLAB_URL', '')
        gitlab_token = data.get('gitlab_token', '').strip() or os.getenv('GITLAB_ACCESS_TOKEN', '')

        if not gitlab_url or not gitlab_token:
            return jsonify({'error': 'GitLab URL and token are required. Set GITLAB_URL/GITLAB_ACCESS_TOKEN or pass in request body.'}), 400

        # 分页拉取所有 GitLab 用户
        headers = {'Private-Token': gitlab_token}
        all_users = []
        page = 1
        per_page = 100

        while True:
            url = f"{gitlab_url.rstrip('/')}/api/v4/users"
            params = {'per_page': per_page, 'page': page, 'active': True}
            response = requests.get(url, headers=headers, params=params, verify=False, timeout=15)

            if response.status_code != 200:
                logger.error(f"GitLab users API error: {response.status_code}, {response.text}")
                return jsonify({'error': f'GitLab API error: {response.status_code}', 'details': response.text}), 502

            users = response.json()
            if not users:
                break

            all_users.extend(users)
            page += 1

        logger.info(f"Fetched {len(all_users)} active users from GitLab")

        # 获取已有映射的 git_username 集合
        existing_maps = ReviewService.get_all_dingtalk_user_maps()
        existing_usernames = {m['git_username'] for m in existing_maps}

        # 同步：已存在则跳过
        inserted = 0
        skipped = 0
        failed_list = []

        for user in all_users:
            username = user.get('username', '')
            if not username:
                continue

            if username in existing_usernames:
                skipped += 1
                continue

            name = user.get('name', '')
            success = ReviewService.upsert_dingtalk_user_map(
                git_username=username,
                dingtalk_userid=None,
                dingtalk_mobile=None,
                remark=name
            )
            if success:
                inserted += 1
            else:
                failed_list.append(username)

        return jsonify({
            'message': f'Sync completed: {inserted} inserted, {skipped} skipped, {len(failed_list)} failed',
            'total_gitlab_users': len(all_users),
            'inserted': inserted,
            'skipped': skipped,
            'failed_count': len(failed_list),
            'failed': failed_list
        }), 200

    except Exception as e:
        logger.error(f"Failed to sync GitLab users: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


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
