import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
from typing import List, Optional

import requests

from src.utils.log import logger


class DingTalkNotifier:
    def __init__(self, webhook_url=None):
        self.enabled = os.environ.get('DINGTALK_ENABLED', '0') == '1'
        self.default_webhook_url = webhook_url or os.environ.get('DINGTALK_WEBHOOK_URL')
        
        # 工作通知相关配置
        self.client_id = os.environ.get('DINGTALK_CLIENT_ID')
        self.client_secret = os.environ.get('DINGTALK_CLIENT_SECRET')
        self.agent_id = os.environ.get('DINGTALK_AGENT_ID')
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0

    def _get_webhook_url(self, project_name=None, url_slug=None):
        """
        获取项目对应的 Webhook URL
        :param project_name: 项目名称
        :param url_slug: 由 gitlab 项目的 url 转换而来的 slug
        :return: Webhook URL
        :raises ValueError: 如果未找到 Webhook URL
        """
        # 如果未提供 project_name，直接返回默认的 Webhook URL
        if not project_name:
            if self.default_webhook_url:
                return self.default_webhook_url
            else:
                raise ValueError("未提供项目名称，且未设置默认的钉钉 Webhook URL。")

        # 构造目标键
        target_key_project = f"DINGTALK_WEBHOOK_URL_{project_name.upper()}"
        target_key_url_slug = f"DINGTALK_WEBHOOK_URL_{url_slug.upper()}"

        # 遍历环境变量
        for env_key, env_value in os.environ.items():
            env_key_upper = env_key.upper()
            if env_key_upper == target_key_project:
                return env_value  # 找到项目名称对应的 Webhook URL，直接返回
            if env_key_upper == target_key_url_slug:
                return env_value  # 找到 GitLab URL 对应的 Webhook URL，直接返回

        # 如果未找到匹配的环境变量，降级使用全局的 Webhook URL
        if self.default_webhook_url:
            return self.default_webhook_url

        # 如果既未找到匹配项，也没有默认值，抛出异常
        raise ValueError(f"未找到项目 '{project_name}' 对应的钉钉Webhook URL，且未设置默认的 Webhook URL。")

    def send_message(self, content: str, msg_type='text', title='通知', is_at_all=False, project_name=None, url_slug = None):
        if not self.enabled:
            logger.info("钉钉推送未启用")
            return

        try:
            post_url = self._get_webhook_url(project_name=project_name, url_slug=url_slug)
            headers = {
                "Content-Type": "application/json",
                "Charset": "UTF-8"
            }
            if msg_type == 'markdown':
                message = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": title,  # Customize as needed
                        "text": content
                    },
                    "at": {
                        "isAtAll": is_at_all
                    }
                }
            else:
                message = {
                    "msgtype": "text",
                    "text": {
                        "content": content
                    },
                    "at": {
                        "isAtAll": is_at_all
                    }
                }
            response = requests.post(url=post_url, data=json.dumps(message), headers=headers)
            response_data = response.json()
            if response_data.get('errmsg') == 'ok':
                logger.info(f"钉钉消息发送成功! webhook_url:{post_url}")
            else:
                logger.error(f"钉钉消息发送失败! webhook_url:{post_url},errmsg:{response_data.get('errmsg')}")
        except Exception as e:
            logger.error(f"钉钉消息发送失败! ", e)

    def _get_access_token(self) -> Optional[str]:
        """
        获取钉钉 access_token，使用缓存机制避免频繁请求

        :return: access_token 或 None（获取失败时）
        """
        # 检查缓存是否有效（token 有效期为 2 小时，提前 5 分钟刷新）
        if self._access_token and time.time() < self._token_expires_at - 300:
            return self._access_token

        if not self.client_id or not self.client_secret:
            logger.error("钉钉 ClientId 或 ClientSecret 未配置")
            return None

        try:
            token_url = f"https://oapi.dingtalk.com/gettoken?appkey={self.client_id}&appsecret={self.client_secret}"
            response = requests.get(token_url, timeout=10)
            data = response.json()

            if data.get('errcode') == 0:
                self._access_token = data.get('access_token')
                # token 有效期为 7200 秒（2小时）
                self._token_expires_at = time.time() + data.get('expires_in', 7200)
                logger.info("钉钉 access_token 获取成功")
                return self._access_token
            else:
                logger.error(f"钉钉 access_token 获取失败: {data.get('errmsg')}")
                return None
        except Exception as e:
            logger.error(f"钉钉 access_token 获取异常: {e}")
            return None

    def send_work_notification(
        self,
        title: str,
        content: str,
        user_ids: Optional[List[str]] = None,
        dept_ids: Optional[List[str]] = None,
        is_to_all: bool = False,
        msg_type: str = 'markdown'
    ) -> bool:
        """
        发送钉钉工作通知（支持标题和内容）

        :param title: 消息标题
        :param content: 消息内容（Markdown 格式或纯文本）
        :param user_ids: 接收人的用户 ID 列表
        :param dept_ids: 接收人的部门 ID 列表
        :param is_to_all: 是否发送给全部员工（与 user_ids/dept_ids 互斥）
        :param msg_type: 消息类型，支持 'text' 和 'markdown'
        :return: 是否发送成功
        """
        if not self.client_id or not self.client_secret or not self.agent_id:
            logger.error("钉钉工作通知配置不完整（需要 DINGTALK_CLIENT_ID, DINGTALK_CLIENT_SECRET, DINGTALK_AGENT_ID）")
            return False

        access_token = self._get_access_token()
        if not access_token:
            return False

        try:
            url = f"https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2?access_token={access_token}"
            headers = {"Content-Type": "application/json"}

            # 构建消息体
            if msg_type == 'markdown':
                msg_content = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": title,
                        "text": content
                    }
                }
            else:
                # 纯文本消息，标题+内容合并
                msg_content = {
                    "msgtype": "text",
                    "text": {
                        "content": f"{title}\n{content}"
                    }
                }

            # 构建请求参数
            payload = {
                "agent_id": self.agent_id,
                "msg": msg_content
            }

            # 设置接收人
            if is_to_all:
                payload["to_all_user"] = False
            else:
                if user_ids:
                    payload["userid_list"] = ",".join(user_ids) if isinstance(user_ids, list) else user_ids
                if dept_ids:
                    payload["dept_id_list"] = ",".join(str(d) for d in dept_ids) if isinstance(dept_ids, list) else dept_ids

            response = requests.post(url, json=payload, headers=headers, timeout=10)
            data = response.json()

            if data.get('errcode') == 0:
                logger.info(f"钉钉工作通知发送成功! task_id: {data.get('task_id')}")
                return True
            else:
                logger.error(f"钉钉工作通知发送失败: {data.get('errmsg')}")
                # 如果是 token 过期错误，清除缓存的 token，下次会重新获取
                if data.get('errcode') == 40001:
                    self._access_token = None
                    self._token_expires_at = 0
                return False

        except Exception as e:
            logger.error(f"钉钉工作通知发送异常: {e}")
            return False

    def send_work_notification_by_mobile(
        self,
        title: str,
        content: str,
        mobiles: Optional[List[str]] = None,
        dept_ids: Optional[List[str]] = None,
        is_to_all: bool = False,
        msg_type: str = 'markdown'
    ) -> bool:
        """
        通过手机号发送钉钉工作通知（先查询 userid，再发送）

        :param title: 消息标题
        :param content: 消息内容
        :param mobiles: 接收人的手机号列表
        :param dept_ids: 接收人的部门 ID 列表
        :param is_to_all: 是否发送给全部员工
        :param msg_type: 消息类型，支持 'text' 和 'markdown'
        :return: 是否发送成功
        """
        if not mobiles:
            return self.send_work_notification(title, content, dept_ids=dept_ids, is_to_all=is_to_all, msg_type=msg_type)

        user_ids = []
        for mobile in mobiles:
            user_id = self._get_user_id_by_mobile(mobile)
            if user_id:
                user_ids.append(user_id)

        if not user_ids:
            logger.error("未能通过手机号获取到任何用户ID")
            return False

        return self.send_work_notification(title, content, user_ids=user_ids, msg_type=msg_type)

    def _get_user_id_by_mobile(self, mobile: str) -> Optional[str]:
        """
        通过手机号获取用户 ID

        :param mobile: 手机号
        :return: 用户 ID 或 None
        """
        access_token = self._get_access_token()
        if not access_token:
            return None

        try:
            url = f"https://oapi.dingtalk.com/topapi/v2/user/getbymobile?access_token={access_token}"
            headers = {"Content-Type": "application/json"}
            payload = {"mobile": mobile}

            response = requests.post(url, json=payload, headers=headers, timeout=10)
            data = response.json()

            if data.get('errcode') == 0:
                return data.get('result', {}).get('userid')
            else:
                logger.warning(f"通过手机号 {mobile} 获取用户ID失败: {data.get('errmsg')}")
                return None
        except Exception as e:
            logger.error(f"通过手机号获取用户ID异常: {e}")
            return None
