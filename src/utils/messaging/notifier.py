from typing import List, Optional

from src.utils.messaging.dingtalk import DingTalkNotifier
from src.utils.messaging.feishu import FeishuNotifier
from src.utils.messaging.webhook import ExtraWebhookNotifier
from src.utils.messaging.wecom import WeComNotifier


def send_notification(content, msg_type='text', title="通知", is_at_all=False, project_name=None, url_slug=None,
                      webhook_data: dict={}):
    """
    发送通知消息到配置的平台(钉钉和企业微信)
    :param content: 消息内容
    :param msg_type: 消息类型，支持text和markdown
    :param title: 消息标题(markdown类型时使用)
    :param is_at_all: 是否@所有人
    :param url_slug: 由gitlab服务器的url地址(如:http://www.gitlab.com)转换成的slug格式，如: www_gitlab_com
    :param webhook_data: push event、merge event的数据内容
    """
    # 钉钉推送
    dingtalk_notifier = DingTalkNotifier()
    dingtalk_notifier.send_message(content=content, msg_type=msg_type, title=title, is_at_all=is_at_all,
                                   project_name=project_name, url_slug=url_slug)

    # 企业微信推送
    wecom_notifier = WeComNotifier()
    wecom_notifier.send_message(content=content, msg_type=msg_type, title=title, is_at_all=is_at_all,
                                project_name=project_name, url_slug=url_slug)

    # 飞书推送
    feishu_notifier = FeishuNotifier()
    feishu_notifier.send_message(content=content, msg_type=msg_type, title=title, is_at_all=is_at_all,
                                 project_name=project_name, url_slug=url_slug)

    # 额外自定义webhook通知
    extra_webhook_notifier = ExtraWebhookNotifier()
    system_data = {
        "content": content,
        "msg_type": msg_type,
        "title": title,
        "is_at_all": is_at_all,
        "project_name": project_name,
        "url_slug": url_slug
    }
    extra_webhook_notifier.send_message(system_data=system_data, webhook_data=webhook_data)


def send_dingtalk_work_notification(
    title: str,
    content: str,
    user_ids: Optional[List[str]] = None,
    dept_ids: Optional[List[str]] = None,
    mobiles: Optional[List[str]] = None,
    is_to_all: bool = False,
    msg_type: str = 'markdown'
) -> bool:
    """
    发送钉钉工作通知（通过 ClientId/ClientSecret 认证）
    
    :param title: 消息标题
    :param content: 消息内容（Markdown 格式或纯文本）
    :param user_ids: 接收人的用户 ID 列表
    :param dept_ids: 接收人的部门 ID 列表
    :param mobiles: 接收人的手机号列表（通过手机号自动查询用户ID）
    :param is_to_all: 是否发送给全部员工
    :param msg_type: 消息类型，支持 'text' 和 'markdown'
    :return: 是否发送成功
    """
    notifier = DingTalkNotifier()
    
    # 如果提供了手机号，先转换为用户ID
    if mobiles:
        return notifier.send_work_notification_by_mobile(
            title=title,
            content=content,
            mobiles=mobiles,
            dept_ids=dept_ids,
            is_to_all=is_to_all,
            msg_type=msg_type
        )
    else:
        return notifier.send_work_notification(
            title=title,
            content=content,
            user_ids=user_ids,
            dept_ids=dept_ids,
            is_to_all=is_to_all,
            msg_type=msg_type
        )