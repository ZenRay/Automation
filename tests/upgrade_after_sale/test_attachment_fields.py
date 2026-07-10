# coding:utf8
from __future__ import annotations

from workers.lib.models import LarkFieldType
from workers.upgrade_after_sale.config import (
    ATTACHMENT_BAK_SOURCE_FIELDS,
    ATTACHMENT_BAK_SUFFIX,
    ENABLE_ATTACHMENT_BAK,
    TARGET_AFTER_SALE,
)


def test_after_sale_attachment_fields_configured():
    mapping = {m.source_col: m for m in TARGET_AFTER_SALE.field_mappings}

    assert mapping["客户申请举证图片"].lark_type == LarkFieldType.ATTACHMENT
    assert mapping["客户申请举证视频"].lark_type == LarkFieldType.ATTACHMENT
    assert mapping["送达签收照片"].lark_type == LarkFieldType.ATTACHMENT


def test_after_sale_bad_rate_is_text():
    mapping = {m.source_col: m for m in TARGET_AFTER_SALE.field_mappings}
    assert mapping["商品不良率"].lark_type == LarkFieldType.TEXT


def test_after_sale_attachment_bak_fields_configured():
    mapping = {m.source_col: m for m in TARGET_AFTER_SALE.field_mappings}

    if not ENABLE_ATTACHMENT_BAK:
        for source_col in ATTACHMENT_BAK_SOURCE_FIELDS:
            assert f"{source_col}{ATTACHMENT_BAK_SUFFIX}" not in mapping
        return

    for source_col in ATTACHMENT_BAK_SOURCE_FIELDS:
        bak_col = f"{source_col}{ATTACHMENT_BAK_SUFFIX}"
        assert bak_col in mapping
        assert mapping[bak_col].lark_type == LarkFieldType.TEXT
