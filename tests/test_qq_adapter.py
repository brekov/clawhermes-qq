"""
Tests for QQ Adapter
"""
import pytest

from clawhermes_qq import QQAdapter, QQConfig, create_qq_adapter


class TestQQAdapter:
    """QQ 适配器单元测试"""

    def test_create_adapter_factory(self):
        adapter = create_qq_adapter(
            app_id="test_app_123",
            token="test_token",
            sandbox=True,
        )
        assert adapter is not None
        assert adapter._qq_config.app_id == "test_app_123"
        assert adapter._qq_config.token == "test_token"
        assert adapter._qq_config.sandbox is True

    def test_adapter_not_running_initially(self):
        adapter = QQAdapter({
            "app_id": "test",
            "token": "token",
        })
        assert adapter.is_running is False

    def test_adapter_skip_start_without_credentials(self):
        adapter = QQAdapter({})
        assert adapter.is_running is False

    def test_extract_text_string(self):
        assert QQAdapter._extract_text("hello") == "hello"

    def test_extract_text_dict(self):
        assert QQAdapter._extract_text({"text": "hello"}) == "hello"

    def test_has_markdown_detection(self):
        assert QQAdapter._has_markdown_formatting("**bold**")
        assert QQAdapter._has_markdown_formatting("```code```")
        assert not QQAdapter._has_markdown_formatting("plain text")

    def test_config_defaults(self):
        cfg = QQConfig(app_id="1", token="t")
        assert cfg.sandbox is True
        assert cfg.max_retries == 3
        assert cfg.heartbeat_interval == 40000

    def test_msg_type_values(self):
        from clawhermes_qq.adapter import QQMsgType
        assert QQMsgType.TEXT == 0
        assert QQMsgType.MARKDOWN == 2
