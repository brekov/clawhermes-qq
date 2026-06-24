"""
ClawHermes-QQ — QQ Bot 渠道适配器
基于 QQ Bot HTTP API + WebSocket 长连接

设计对齐 clawhermes-lark：
  - WebSocket 长连接事件订阅（自动重连 + 心跳）
  - HTTP API 消息发送（文本/Markdown/富媒体）
  - ChannelAdapter 标准接口

QQ Bot API 参考: https://bot.q.qq.com/wiki
"""
from clawhermes_qq.adapter import (
    QQAdapter,
    QQConfig,
    QQEventType,
    create_qq_adapter,
)

__version__ = "0.1.0"
__all__ = ["QQAdapter", "QQConfig", "QQEventType", "create_qq_adapter"]
