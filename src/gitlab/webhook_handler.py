import os
import re
import time
from urllib.parse import urljoin, quote
import fnmatch
import requests

from src.utils.log import logger


def get_mr_commit_authors(project_path: str, mr_iid: str, gitlab_url: str = None,
                          gitlab_token: str = None) -> list:
    """
    根据 MR 编号查询该 MR 内所有 commit 的开发者清单

    :param project_path: GitLab 项目路径（如 group/project-name）
    :param mr_iid: MR 的 IID（项目内编号）
    :param gitlab_url: GitLab 服务器地址，默认从环境变量获取
    :param gitlab_token: GitLab Access Token，默认从环境变量获取
    :return: 去重的开发者用户名列表
    """
    gitlab_url = gitlab_url or os.getenv('GITLAB_URL', '')
    gitlab_token = gitlab_token or os.getenv('GITLAB_ACCESS_TOKEN', '')

    if not gitlab_url or not gitlab_token:
        logger.warning("GitLab URL or token not configured, cannot query MR commit authors")
        return []

    if not mr_iid:
        logger.warning("MR IID is empty, cannot query commit authors")
        return []

    # 先通过项目路径获取项目 ID
    project_id = _resolve_project_id(project_path, gitlab_url, gitlab_token)
    if not project_id:
        return []

    # 查询 MR 内的所有 commits
    try:
        url = urljoin(
            f"{gitlab_url}/",
            f"api/v4/projects/{project_id}/merge_requests/{mr_iid}/commits"
        )
        headers = {'Private-Token': gitlab_token}
        params = {'per_page': 100}

        response = requests.get(url, headers=headers, params=params, verify=False, timeout=10)

        if response.status_code == 200:
            commits = response.json()
            authors = []
            seen = set()
            for commit in commits:
                # 优先取 author_email 对应的 GitLab 用户名
                author_name = commit.get('author_name', '')
                author_email = commit.get('author_email', '')
                if author_name and author_name not in seen:
                    authors.append(author_name)
                    seen.add(author_name)
            logger.info(f"GitLab API: found {len(authors)} commit authors in MR !{mr_iid} of project {project_path}: {authors}")
            return authors
        elif response.status_code == 404:
            logger.warning(f"GitLab MR !{mr_iid} not found in project {project_path} (id={project_id})")
            return []
        else:
            logger.warning(f"GitLab API error querying MR !{mr_iid} commits: {response.status_code}, {response.text}")
            return []
    except Exception as e:
        logger.error(f"Error querying MR commit authors: {e}")
        return []


def get_mr_submit_author(project_path: str, mr_iid: str, gitlab_url: str = None,
                         gitlab_token: str = None) -> dict:
    """
    根据 MR 编号查询该 MR 的发起者（提交 PR 的人）

    :param project_path: GitLab 项目路径（如 group/project-name）
    :param mr_iid: MR 的 IID（项目内编号）
    :param gitlab_url: GitLab 服务器地址，默认从环境变量获取
    :param gitlab_token: GitLab Access Token，默认从环境变量获取
    :return: {'username': ..., 'name': ...} 或空字典
    """
    gitlab_url = gitlab_url or os.getenv('GITLAB_URL', '')
    gitlab_token = gitlab_token or os.getenv('GITLAB_ACCESS_TOKEN', '')

    if not gitlab_url or not gitlab_token:
        logger.warning("GitLab URL or token not configured, cannot query MR submit author")
        return {}

    if not mr_iid:
        logger.warning("MR IID is empty, cannot query submit author")
        return {}

    project_id = _resolve_project_id(project_path, gitlab_url, gitlab_token)
    if not project_id:
        return {}

    try:
        url = urljoin(
            f"{gitlab_url}/",
            f"api/v4/projects/{project_id}/merge_requests/{mr_iid}"
        )
        headers = {'Private-Token': gitlab_token}

        response = requests.get(url, headers=headers, verify=False, timeout=10)

        if response.status_code == 200:
            mr_data = response.json()
            author = mr_data.get('author', {})
            result = {
                'username': author.get('username', ''),
                'name': author.get('name', ''),
                'web_url': mr_data.get('web_url', ''),
                'source_branch': mr_data.get('source_branch', ''),
                'target_branch': mr_data.get('target_branch', ''),
            }
            logger.info(f"GitLab API: MR !{mr_iid} submit author: {result['username']} ({result['name']}), web_url: {result['web_url']}")
            return result
        elif response.status_code == 404:
            logger.warning(f"GitLab MR !{mr_iid} not found in project {project_path} (id={project_id})")
            return {}
        else:
            logger.warning(f"GitLab API error querying MR !{mr_iid}: {response.status_code}, {response.text}")
            return {}
    except Exception as e:
        logger.error(f"Error querying MR submit author: {e}")
        return {}


def _resolve_project_id(project_path: str, gitlab_url: str, gitlab_token: str):
    """
    根据项目路径解析 GitLab 项目 ID

    :param project_path: 项目路径（如 group/project-name）
    :param gitlab_url: GitLab 服务器地址
    :param gitlab_token: GitLab Access Token
    :return: 项目 ID 或 None
    """
    headers = {'Private-Token': gitlab_token}

    # 方式1：通过 URL-encoded 路径精确查询
    try:
        encoded_path = quote(project_path, safe='')
        url = urljoin(f"{gitlab_url}/", f"api/v4/projects/{encoded_path}")
        response = requests.get(url, headers=headers, verify=False, timeout=10)

        if response.status_code == 200:
            project_id = response.json().get('id')
            logger.info(f"GitLab API: resolved project '{project_path}' -> id={project_id}")
            return project_id
    except Exception as e:
        logger.debug(f"Failed to resolve project by path '{project_path}': {e}")

    # 方式2：搜索项目名（兼容 SonarQube key 格式如 com.example:project-name）
    search_name = project_path.split(':')[-1].split('/')[-1]
    try:
        url = urljoin(f"{gitlab_url}/", "api/v4/projects")
        params = {
            'search': search_name,
            'per_page': 5,
            'order_by': 'last_activity_at',
            'sort': 'desc'
        }
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=10)

        if response.status_code == 200 and response.json():
            project = response.json()[0]
            project_id = project.get('id')
            logger.info(f"GitLab API: search matched project '{project.get('path_with_namespace')}' (id={project_id}) for '{search_name}'")
            return project_id
    except Exception as e:
        logger.debug(f"Failed to search project '{search_name}': {e}")

    logger.warning(f"GitLab API: could not resolve project for '{project_path}'")
    return None


def filter_changes(changes: list):
    '''
    过滤数据，只保留支持的文件类型以及必要的字段信息
    '''
    # 从环境变量中获取支持的文件扩展名
    supported_extensions = os.getenv('SUPPORTED_EXTENSIONS', '.java,.py,.php').split(',')

    filter_deleted_files_changes = [change for change in changes if not change.get("deleted_file")]

    # 过滤 `new_path` 以支持的扩展名结尾的元素, 仅保留diff和new_path字段
    filtered_changes = [
        {
            'diff': item.get('diff', ''),
            'new_path': item['new_path'],
            'additions': len(re.findall(r'^\+(?!\+\+)', item.get('diff', ''), re.MULTILINE)),
            'deletions': len(re.findall(r'^-(?!--)', item.get('diff', ''), re.MULTILINE))
        }
        for item in filter_deleted_files_changes
        if any(item.get('new_path', '').endswith(ext) for ext in supported_extensions)
    ]
    return filtered_changes


def slugify_url(original_url: str) -> str:
    """
    将原始URL转换为适合作为文件名的字符串，其中非字母或数字的字符会被替换为下划线，举例：
    slugify_url("http://example.com/path/to/repo/") => example_com_path_to_repo
    slugify_url("https://gitlab.com/user/repo.git") => gitlab_com_user_repo_git
    """
    # Remove URL scheme (http, https, etc.) if present
    original_url = re.sub(r'^https?://', '', original_url)

    # Replace non-alphanumeric characters (except underscore) with underscores
    target = re.sub(r'[^a-zA-Z0-9]', '_', original_url)

    # Remove trailing underscore if present
    target = target.rstrip('_')

    return target


class MergeRequestHandler:
    def __init__(self, webhook_data: dict, gitlab_token: str, gitlab_url: str):
        self.merge_request_iid = None
        self.webhook_data = webhook_data
        self.gitlab_token = gitlab_token
        self.gitlab_url = gitlab_url
        self.event_type = None
        self.project_id = None
        self.action = None
        self.parse_event_type()

    def parse_event_type(self):
        # 提取 event_type
        self.event_type = self.webhook_data.get('object_kind', None)
        if self.event_type == 'merge_request':
            self.parse_merge_request_event()

    def parse_merge_request_event(self):
        # 提取 Merge Request 的相关参数
        merge_request = self.webhook_data.get('object_attributes', {})
        self.merge_request_iid = merge_request.get('iid')
        self.project_id = merge_request.get('target_project_id')
        self.action = merge_request.get('action')

    def get_merge_request_changes(self) -> list:
        # 检查是否为 Merge Request Hook 事件
        if self.event_type != 'merge_request':
            logger.warn(f"Invalid event type: {self.event_type}. Only 'merge_request' event is supported now.")
            return []

        # Gitlab merge request changes API可能存在延迟，多次尝试
        max_retries = 3  # 最大重试次数
        retry_delay = 10  # 重试间隔时间（秒）
        for attempt in range(max_retries):
            # 调用 GitLab API 获取 Merge Request 的 changes
            url = urljoin(f"{self.gitlab_url}/",
                          f"api/v4/projects/{self.project_id}/merge_requests/{self.merge_request_iid}/changes")
            headers = {
                'Private-Token': self.gitlab_token
            }
            response = requests.get(url, headers=headers, verify=False)
            logger.debug(
                f"Get changes response from GitLab (attempt {attempt + 1}): {response.status_code}, {response.text}, URL: {url}")

            # 检查请求是否成功
            if response.status_code == 200:
                changes = response.json().get('changes', [])
                if changes:
                    return changes
                else:
                    logger.info(
                        f"Changes is empty, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries}), URL: {url}")
                    time.sleep(retry_delay)
            else:
                logger.warn(f"Failed to get changes from GitLab (URL: {url}): {response.status_code}, {response.text}")
                return []

        logger.warning(f"Max retries ({max_retries}) reached. Changes is still empty.")
        return []  # 达到最大重试次数后返回空列表

    def get_merge_request_commits(self) -> list:
        # 检查是否为 Merge Request Hook 事件
        if self.event_type != 'merge_request':
            return []

        # 调用 GitLab API 获取 Merge Request 的 commits
        url = urljoin(f"{self.gitlab_url}/",
                      f"api/v4/projects/{self.project_id}/merge_requests/{self.merge_request_iid}/commits")
        headers = {
            'Private-Token': self.gitlab_token
        }
        response = requests.get(url, headers=headers, verify=False)
        logger.debug(f"Get commits response from gitlab: {response.status_code}, {response.text}")
        # 检查请求是否成功
        if response.status_code == 200:
            return response.json()
        else:
            logger.warn(f"Failed to get commits: {response.status_code}, {response.text}")
            return []

    def add_merge_request_notes(self, review_result):
        url = urljoin(f"{self.gitlab_url}/",
                      f"api/v4/projects/{self.project_id}/merge_requests/{self.merge_request_iid}/notes")
        headers = {
            'Private-Token': self.gitlab_token,
            'Content-Type': 'application/json'
        }
        data = {
            'body': review_result
        }
        response = requests.post(url, headers=headers, json=data, verify=False)
        logger.debug(f"Add notes to gitlab {url}: {response.status_code}, {response.text}")
        if response.status_code == 201:
            logger.info("Note successfully added to merge request.")
        else:
            logger.error(f"Failed to add note: {response.status_code}")
            logger.error(response.text)

    def target_branch_protected(self) -> bool:
        url = urljoin(f"{self.gitlab_url}/",
                      f"api/v4/projects/{self.project_id}/protected_branches")
        headers = {
            'Private-Token': self.gitlab_token,
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers, verify=False)
        logger.debug(f"Get protected branches response from gitlab: {response.status_code}, {response.text}")
        # 检查请求是否成功
        if response.status_code == 200:
            data = response.json()
            target_branch = self.webhook_data['object_attributes']['target_branch']
            return any(fnmatch.fnmatch(target_branch, item['name']) for item in data)
        else:
            logger.warn(f"Failed to get protected branches: {response.status_code}, {response.text}")
            return False


class PushHandler:
    def __init__(self, webhook_data: dict, gitlab_token: str, gitlab_url: str):
        self.webhook_data = webhook_data
        self.gitlab_token = gitlab_token
        self.gitlab_url = gitlab_url
        self.event_type = None
        self.project_id = None
        self.branch_name = None
        self.commit_list = []
        self.parse_event_type()

    def parse_event_type(self):
        # 提取 event_type
        self.event_type = self.webhook_data.get('event_name', None)
        if self.event_type == 'push':
            self.parse_push_event()

    def parse_push_event(self):
        # 提取 Push 事件的相关参数
        self.project_id = self.webhook_data.get('project', {}).get('id')
        self.branch_name = self.webhook_data.get('ref', '').replace('refs/heads/', '')
        self.commit_list = self.webhook_data.get('commits', [])

    def get_push_commits(self) -> list:
        # 检查是否为 Push 事件
        if self.event_type != 'push':
            logger.warn(f"Invalid event type: {self.event_type}. Only 'push' event is supported now.")
            return []

        # 提取提交信息
        commit_details = []
        for commit in self.commit_list:
            commit_info = {
                'message': commit.get('message'),
                'author': commit.get('author', {}).get('name'),
                'timestamp': commit.get('timestamp'),
                'url': commit.get('url'),
            }
            commit_details.append(commit_info)

        logger.info(f"Collected {len(commit_details)} commits from push event.")
        return commit_details

    def add_push_notes(self, message: str):
        # 添加评论到 GitLab Push 请求的提交中（此处假设是在最后一次提交上添加注释）
        if not self.commit_list:
            logger.warn("No commits found to add notes to.")
            return

        # 获取最后一个提交的ID
        last_commit_id = self.commit_list[-1].get('id')
        if not last_commit_id:
            logger.error("Last commit ID not found.")
            return

        url = urljoin(f"{self.gitlab_url}/",
                      f"api/v4/projects/{self.project_id}/repository/commits/{last_commit_id}/comments")
        headers = {
            'Private-Token': self.gitlab_token,
            'Content-Type': 'application/json'
        }
        data = {
            'note': message
        }
        response = requests.post(url, headers=headers, json=data, verify=False)
        logger.debug(f"Add comment to commit {last_commit_id}: {response.status_code}, {response.text}")
        if response.status_code == 201:
            logger.info("Comment successfully added to push commit.")
        else:
            logger.error(f"Failed to add comment: {response.status_code}")
            logger.error(response.text)

    def __repository_commits(self, ref_name: str = "", since: str = "", until: str = "", pre_page: int = 100,
                             page: int = 1):
        # 获取仓库提交信息
        url = f"{urljoin(f'{self.gitlab_url}/', f'api/v4/projects/{self.project_id}/repository/commits')}?ref_name={ref_name}&since={since}&until={until}&per_page={pre_page}&page={page}"
        headers = {
            'Private-Token': self.gitlab_token
        }
        response = requests.get(url, headers=headers, verify=False)
        logger.debug(
            f"Get commits response from GitLab for repository_commits: {response.status_code}, {response.text}, URL: {url}")

        if response.status_code == 200:
            return response.json()
        else:
            logger.warn(
                f"Failed to get commits for ref {ref_name}: {response.status_code}, {response.text}")
            return []

    def get_parent_commit_id(self, commit_id: str) -> str:
        commits = self.__repository_commits(ref_name=commit_id, pre_page=1, page=1)
        if commits and commits[0].get('parent_ids', []):
            return commits[0].get('parent_ids', [])[0]
        return ""

    def repository_compare(self, before: str, after: str):
        # 比较两个提交之间的差异
        url = f"{urljoin(f'{self.gitlab_url}/', f'api/v4/projects/{self.project_id}/repository/compare')}?from={before}&to={after}"
        headers = {
            'Private-Token': self.gitlab_token
        }
        response = requests.get(url, headers=headers, verify=False)
        logger.debug(
            f"Get changes response from GitLab for repository_compare: {response.status_code}, {response.text}, URL: {url}")

        if response.status_code == 200:
            return response.json().get('diffs', [])
        else:
            logger.warn(
                f"Failed to get changes for repository_compare: {response.status_code}, {response.text}")
            return []

    def get_push_changes(self) -> list:
        # 检查是否为 Push 事件
        if self.event_type != 'push':
            logger.warn(f"Invalid event type: {self.event_type}. Only 'push' event is supported now.")
            return []

        # 如果没有提交，返回空列表
        if not self.commit_list:
            logger.info("No commits found in push event.")
            return []
        headers = {
            'Private-Token': self.gitlab_token
        }

        # 优先尝试compare API获取变更
        before = self.webhook_data.get('before', '')
        after = self.webhook_data.get('after', '')
        if before and after:
            if after.startswith('0000000'):
                # 删除分支处理
                return []
            if before.startswith('0000000'):
                # 创建分支处理
                first_commit_id = self.commit_list[0].get('id')
                parent_commit_id = self.get_parent_commit_id(first_commit_id)
                if parent_commit_id:
                    before = parent_commit_id
            return self.repository_compare(before, after)
        else:
            return []
