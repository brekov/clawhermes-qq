"""
ClawHermes-QQ — QQ Bot 渠道适配器

架构：
  Layer 1: aiohttp HTTP API — 消息发送、用户信息查询
  Layer 2: WebSocket 长连接 — 事件接收、心跳保活、自动重连
  Layer 3: ChannelAdapter — ClawHermes 统一适配器接口

QQ Bot API:
  - 沙箱: https://sandbox.api.sgroup.qq.com
  - 正式: https://api.sgroup.qq.com
  - WebSocket: wss://api.sgroup.qq.com/websocket
  - 认证: BotAppId + BotToken (Header: Authorization: QQBot {token})
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import IntEnum, Enum
from typing import Any, Callable

import aiohttp

from clawhermes.channel.adapter import (
    ChannelAdapter,
    ChannelMessage,
    ChannelResponse,
    ChannelType,
    ChannelUser,
)

logger = logging.getLogger("clawhermes.qq")


# ============================================================================
# 配置
# ============================================================================

@dataclass
class QQConfig:
    """QQ Bot 应用配置"""
    app_id: str            # BotAppID (uint64 string)
    token: str             # BotToken
    secret: str = ""       # BotSecret (用于签名校验)
    sandbox: bool = True   # 沙箱模式
    max_retries: int = 3
    retry_delay: float = 1.0
    heartbeat_interval: int = 40000  # ms, default 40s
    auto_reconnect: bool = True


# ============================================================================
# 事件类型 / Opcodes / Intents
# ============================================================================

class QQOpcode(IntEnum):
    """QQ Bot WebSocket Opcodes"""
    DISPATCH = 0
    HEARTBEAT = 1
    IDENTIFY = 2
    RESUME = 6
    RECONNECT = 7
    INVALID_SESSION = 9
    HELLO = 10
    HEARTBEAT_ACK = 11


class QQIntent(IntEnum):
    """QQ Bot WebSocket Intents"""
    GUILDS = 1 << 0
    GUILD_MEMBERS = 1 << 1
    GUILD_MESSAGES = 1 << 9
    GUILD_MESSAGE_REACTIONS = 1 << 10
    DIRECT_MESSAGE = 1 << 12
    GROUP_AND_C2C_EVENT = 1 << 25
    INTERACTION = 1 << 26
    MESSAGE_AUDIT = 1 << 27
    FORUMS_EVENT = 1 << 28
    AUDIO_ACTION = 1 << 29
    PUBLIC_GUILD_MESSAGES = 1 << 30


class QQEventType(str, Enum):
    """QQ Bot 事件类型"""
    C2C_MESSAGE_CREATE = "C2C_MESSAGE_CREATE"
    GROUP_AT_MESSAGE_CREATE = "GROUP_AT_MESSAGE_CREATE"
    MESSAGE_CREATE = "MESSAGE_CREATE"
    DIRECT_MESSAGE_CREATE = "DIRECT_MESSAGE_CREATE"
    AT_MESSAGE_CREATE = "AT_MESSAGE_CREATE"


class QQMsgType(IntEnum):
    """QQ Bot 消息类型"""
    TEXT = 0
    MARKDOWN = 2
    ARK = 3
    EMBED = 4
    MEDIA = 7


# ============================================================================
# QQAdapter — QQ 渠道适配器
# ============================================================================

class QQAdapter(ChannelAdapter):
    """
    QQ Bot 渠道适配器

    Layer 1 (HTTP API):
      - aiohttp ClientSession with Authorization token
      - POST /v2/users/{openid}/messages — C2C 私信
      - POST /v2/groups/{openid}/messages — 群聊消息
      - GET /v2/users/{openid} — 用户信息

    Layer 2 (WebSocket):
      - wss://api.sgroup.qq.com/websocket
      - Opcode 10 HELLO → Opcode 2 IDENTIFY
      - Opcode 1 HEARTBEAT → Opcode 11 HEARTBEAT_ACK
      - Opcode 0 DISPATCH → 事件处理
      - 自动重连 + 断线恢复
    """

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(ChannelType.WECHAT, config)  # 复用 WECHAT channel_type 占位
        cfg = config or {}

        self._qq_config = QQConfig(
            app_id=cfg.get("app_id", ""),
            token=cfg.get("token", ""),
            secret=cfg.get("secret", ""),
            sandbox=cfg.get("sandbox", True),
            max_retries=int(cfg.get("max_retries", 3)),
            retry_delay=float(cfg.get("retry_delay", 1.0)),
            heartbeat_interval=int(cfg.get("heartbeat_interval", 40000)),
            auto_reconnect=cfg.get("auto_reconnect", True),
        )

        self._http_session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._session_id: str = ""
        self._last_seq: int = 0
        self._should_reconnect = True
        self._ws_error_count = 0
        self._max_ws_errors = 10

    # ==================================================================
    # ChannelAdapter 接口 — start / stop
    # ==================================================================

    async def start(self) -> None:
        """启动适配器：创建 HTTP session + WebSocket 长连接"""
        if self._running:
            return

        if not self._qq_config.app_id or not self._qq_config.token:
            logger.warning("QQ: 未配置 app_id/token，跳过启动")
            return

        # Layer 1: 创建 HTTP session
        connector = aiohttp.TCPConnector(limit=10)
        self._http_session = aiohttp.ClientSession(
            connector=connector,
            headers={
                "Authorization": f"QQBot {self._qq_config.token}",
                "Content-Type": "application/json",
            },
        )

        logger.info(
            "QQ client initialized: app_id=%s sandbox=%s",
            self._qq_config.app_id[:12] + "***",
            self._qq_config.sandbox,
        )

        # Layer 2: 启动 WebSocket 长连接
        self._running = True
        self._should_reconnect = True
        self._ws_error_count = 0
        self._ws_task = asyncio.create_task(self._ws_loop(), name="qq_ws")
        logger.info("QQ adapter started")

    async def stop(self) -> None:
        """停止适配器：断开 WebSocket + 清理 HTTP session"""
        self._running = False
        self._should_reconnect = False

        # 取消 WebSocket/心跳任务
        for task in [self._ws_task, self._heartbeat_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        # 关闭 WebSocket
        if self._ws and not self._ws.closed:
            await self._ws.close()

        # 关闭 HTTP session
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

        self._ws = None
        logger.info("QQ adapter stopped")

    # ==================================================================
    # ChannelAdapter 接口 — send_response
    # ==================================================================

    async def send_response(self, response: ChannelResponse, original: ChannelMessage) -> None:
        """
        向 QQ 发送响应消息

        策略:
          - 文本消息: msg_type=0 (text)
          - Markdown 消息: msg_type=2 (markdown)
          - 群聊: POST /v2/groups/{openid}/messages
          - 私聊: POST /v2/users/{openid}/messages
        """
        target_id = self._resolve_send_target(original)
        if not target_id:
            logger.error("QQ send_response: 无法解析 target_id")
            return

        msg_type = response.metadata.get("msg_type", 0)
        if msg_type == "markdown" or self._has_markdown_formatting(response.content):
            msg_type = QQMsgType.MARKDOWN
        else:
            msg_type = QQMsgType.TEXT

        msg_id = original.metadata.get("msg_id", "") or original.message_id

        try:
            await self._send_message(
                target_id=target_id,
                content=response.content,
                msg_type=int(msg_type),
                msg_id=msg_id,
            )
        except Exception as e:
            logger.exception("QQ send_response failed: target=%s", target_id)

    # ==================================================================
    # ChannelAdapter 接口 — get_user_info
    # ==================================================================

    async def get_user_info(self, user_id: str) -> ChannelUser | None:
        """获取 QQ 用户信息"""
        if not self._http_session or not self._running:
            return None

        try:
            base = self._base_url()
            async with self._http_session.get(
                f"{base}/v2/users/{user_id}"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    user_data = data.get("data", data)
                    return ChannelUser(
                        user_id=user_id,
                        display_name=(
                            user_data.get("username")
                            or user_data.get("nick")
                            or user_data.get("id", f"QQ User ({user_id[:12]})")
                        ),
                        metadata={
                            "avatar": user_data.get("avatar", ""),
                            "bot": user_data.get("bot", False),
                        },
                    )
        except Exception as e:
            logger.exception("QQ get_user_info error: %s", user_id)

        return ChannelUser(
            user_id=user_id,
            display_name=f"QQ User ({user_id[:12]})",
        )

    # ==================================================================
    # WebSocket 连接管理 (Layer 2)
    # ==================================================================

    def _base_url(self) -> str:
        """获取 HTTP API 基础 URL"""
        if self._qq_config.sandbox:
            return "https://sandbox.api.sgroup.qq.com"
        return "https://api.sgroup.qq.com"

    async def _ws_loop(self) -> None:
        """WebSocket 长连接循环"""
        while self._running and self._should_reconnect:
            try:
                # Step 1: 获取网关地址
                base = self._base_url()
                if not self._http_session:
                    return
                async with self._http_session.get(f"{base}/gateway") as resp:
                    gw_data = await resp.json()
                    ws_url = gw_data.get("url", "")
                    if not ws_url:
                        logger.error("QQ: 获取 gateway URL 失败: %s", gw_data)
                        await asyncio.sleep(5)
                        continue

                # Step 2: 建立 WebSocket 连接
                logger.info("QQ WebSocket connecting to %s...", ws_url[:50] + "...")
                self._ws = await self._http_session.ws_connect(ws_url)

                # Step 3: 事件循环
                await self._ws_event_loop()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._ws_error_count += 1
                logger.warning(
                    "QQ WebSocket error #%d, reconnect in 5s: %s",
                    self._ws_error_count, e,
                )
                if self._ws_error_count >= self._max_ws_errors:
                    logger.error("QQ WebSocket: too many errors, stopping")
                    self._running = False
                    break
                await asyncio.sleep(5)

    async def _ws_event_loop(self) -> None:
        """WebSocket 事件接收循环"""
        if not self._ws:
            return

        # QQ Bot WebSocket 需要先发送 IDENTIFY
        identify_sent = False
        identify_payload = json.dumps({
            "op": QQOpcode.IDENTIFY,
            "d": {
                "token": f"QQBot {self._qq_config.token}",
                "intents": QQIntent.GROUP_AND_C2C_EVENT,
                "shard": [0, 1],
                "properties": {},
            },
        })

        async for msg in self._ws:
            if not self._running:
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                op = payload.get("op", -1)
                data = payload.get("d", {})
                seq = payload.get("s", 0)
                if seq:
                    self._last_seq = seq

                if op == QQOpcode.HELLO:
                    # 收到 HELLO，发送 IDENTIFY
                    logger.info("QQ WS: HELLO received, sending IDENTIFY")
                    await self._ws.send_str(identify_payload)
                    identify_sent = True

                    # 启动心跳
                    heartbeat_interval = data.get(
                        "heartbeat_interval", self._qq_config.heartbeat_interval
                    )
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(int(heartbeat_interval)),
                        name="qq_heartbeat",
                    )

                elif op == QQOpcode.DISPATCH:
                    event_type = payload.get("t", "")
                    await self._handle_event(event_type, data)

                elif op == QQOpcode.HEARTBEAT_ACK:
                    logger.debug("QQ WS: heartbeat ack")

                elif op == QQOpcode.RECONNECT:
                    logger.info("QQ WS: server requested reconnect")
                    break  # 退出循环触发重连

                elif op == QQOpcode.INVALID_SESSION:
                    logger.warning("QQ WS: invalid session, will re-identify")
                    identify_sent = False
                    await self._ws.send_str(identify_payload)
                    identify_sent = True

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.info("QQ WS: connection closed")
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error("QQ WS: error")
                break

    async def _heartbeat_loop(self, interval_ms: int) -> None:
        """心跳保活循环"""
        heartbeat = json.dumps({"op": QQOpcode.HEARTBEAT, "d": self._last_seq})
        try:
            while self._running and self._ws and not self._ws.closed:
                await asyncio.sleep(interval_ms / 1000)
                if self._ws and not self._ws.closed:
                    try:
                        await self._ws.send_str(heartbeat)
                        logger.debug("QQ WS: heartbeat sent")
                    except Exception as e:
                        logger.warning("QQ WS: heartbeat failed: %s", e)
                        break
        except asyncio.CancelledError:
            pass

    # ==================================================================
    # 事件处理
    # ==================================================================

    async def _handle_event(self, event_type: str, data: dict) -> None:
        """处理 QQ Bot 事件"""
        try:
            if event_type in (
                QQEventType.C2C_MESSAGE_CREATE,
                QQEventType.DIRECT_MESSAGE_CREATE,
            ):
                await self._handle_c2c_message(data)

            elif event_type in (
                QQEventType.GROUP_AT_MESSAGE_CREATE,
                QQEventType.AT_MESSAGE_CREATE,
                QQEventType.MESSAGE_CREATE,
            ):
                await self._handle_group_message(data)

            else:
                logger.debug("QQ unhandled event: %s", event_type)

        except Exception:
            logger.exception("QQ event handler error [%s]", event_type)

    async def _handle_c2c_message(self, data: dict) -> None:
        """处理私聊消息"""
        author = data.get("author", {})
        content = data.get("content", "")
        msg_id = data.get("id", "")
        user_id = author.get("id", "") or author.get("user_openid", "")

        if not content or not user_id:
            return

        # 提取纯文本
        text = self._extract_text(content)

        channel_msg = ChannelMessage(
            message_id=msg_id,
            channel_type=ChannelType.WECHAT,
            user=ChannelUser(
                user_id=user_id,
                display_name=author.get("username", "") or f"QQ:{user_id[:12]}",
            ),
            content=text,
            session_id=user_id,
            reply_to=None,
            metadata={
                "msg_type": "c2c",
                "msg_id": msg_id,
                "user_id": user_id,
                "raw_content": content,
            },
        )
        self._dispatch_message(channel_msg)

    async def _handle_group_message(self, data: dict) -> None:
        """处理群聊 @消息"""
        author = data.get("author", {})
        group_id = data.get("group_openid", "") or data.get("group_id", "")
        content = data.get("content", "")
        msg_id = data.get("id", "")
        user_id = author.get("id", "") or author.get("member_openid", "")

        if not content or not group_id:
            return

        text = self._extract_text(content)

        channel_msg = ChannelMessage(
            message_id=msg_id,
            channel_type=ChannelType.WECHAT,
            user=ChannelUser(
                user_id=user_id,
                display_name=author.get("username", "") or f"QQ:{user_id[:12]}",
            ),
            content=text,
            session_id=group_id,
            reply_to=None,
            metadata={
                "msg_type": "group",
                "msg_id": msg_id,
                "group_id": group_id,
                "user_id": user_id,
                "raw_content": content,
            },
        )
        self._dispatch_message(channel_msg)

    # ==================================================================
    # HTTP API — 消息发送
    # ==================================================================

    async def _send_message(
        self,
        target_id: str,
        content: str,
        msg_type: int = QQMsgType.TEXT,
        msg_id: str = "",
        retries: int = 0,
    ) -> str:
        """发送消息到 QQ"""
        if not self._http_session:
            raise RuntimeError("QQ HTTP session not initialized")

        base = self._base_url()

        # 判断是群聊还是私聊
        is_group = msg_type == "group" or (
            hasattr(self, "_last_msg_meta")
            and self._last_msg_meta.get("msg_type") == "group"
        )

        # 默认发送到私聊
        endpoint = f"{base}/v2/users/{target_id}/messages"
        payload = {
            "content": content,
            "msg_type": msg_type,
            "msg_id": msg_id or str(int(time.time() * 1000)),
            "msg_seq": 1,
        }

        try:
            async with self._http_session.post(endpoint, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    sent_id = data.get("id", "") or data.get("message_id", "")
                    logger.debug("QQ message sent: id=%s", sent_id)
                    return sent_id
                else:
                    error_text = await resp.text()
                    logger.error(
                        "QQ send_message failed: status=%s body=%s",
                        resp.status, error_text[:200],
                    )
                    # 尝试群聊端点
                    if "user" in error_text.lower() or resp.status == 404:
                        return await self._send_group_message(
                            target_id, content, msg_type, msg_id, retries
                        )
                    if retries < self._qq_config.max_retries:
                        delay = self._qq_config.retry_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                        return await self._send_message(
                            target_id, content, msg_type, msg_id, retries + 1
                        )
                    return ""

        except Exception as e:
            logger.exception("QQ send_message exception: %s", target_id)
            if retries < self._qq_config.max_retries:
                delay = self._qq_config.retry_delay * (2 ** retries)
                await asyncio.sleep(delay)
                return await self._send_message(
                    target_id, content, msg_type, msg_id, retries + 1
                )
            raise

    async def _send_group_message(
        self,
        group_id: str,
        content: str,
        msg_type: int = QQMsgType.TEXT,
        msg_id: str = "",
        retries: int = 0,
    ) -> str:
        """发送群聊消息"""
        if not self._http_session:
            raise RuntimeError("QQ HTTP session not initialized")

        base = self._base_url()
        endpoint = f"{base}/v2/groups/{group_id}/messages"
        payload = {
            "content": content,
            "msg_type": msg_type,
            "msg_id": msg_id or str(int(time.time() * 1000)),
            "msg_seq": 1,
        }

        try:
            async with self._http_session.post(endpoint, json=payload) as resp:
                data = await resp.json()
                if resp.status == 200:
                    return data.get("id", "")
                logger.error("QQ send_group_message failed: %s", await resp.text()[:200])
                return ""
        except Exception as e:
            logger.exception("QQ send_group_message error")
            if retries < self._qq_config.max_retries:
                await asyncio.sleep(self._qq_config.retry_delay * (2 ** retries))
                return await self._send_group_message(
                    group_id, content, msg_type, msg_id, retries + 1
                )
            raise

    # ==================================================================
    # 工具方法
    # ==================================================================

    @staticmethod
    def _extract_text(content: str | dict) -> str:
        """从 QQ 消息内容提取纯文本"""
        if isinstance(content, dict):
            content = content.get("text", content.get("content", str(content)))
        return str(content).strip() if content else ""

    @staticmethod
    def _has_markdown_formatting(text: str) -> bool:
        """检测是否包含 Markdown 格式"""
        markers = ["**", "__", "*", "`", "```", "#", "- ", "1. ", "> ", "![", "["]
        return any(m in text for m in markers)

    def _resolve_send_target(self, message: ChannelMessage) -> str:
        """解析发送目标"""
        group_id = message.metadata.get("group_id", "")
        if group_id:
            return group_id
        return message.session_id or message.user.user_id


# ============================================================================
# 工厂函数
# ============================================================================

def create_qq_adapter(
    app_id: str = "",
    token: str = "",
    secret: str = "",
    sandbox: bool = True,
    **kwargs: Any,
) -> QQAdapter:
    """快速创建 QQ 适配器"""
    return QQAdapter({
        "app_id": app_id,
        "token": token,
        "secret": secret,
        "sandbox": sandbox,
        **kwargs,
    })
