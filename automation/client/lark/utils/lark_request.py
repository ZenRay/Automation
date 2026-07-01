# coding:utf8
import requests
import logging
import time


from ..exceptions import LarkException

logger = logging.getLogger("automation.lark.utils.lark_request")

# 飞书频控错误码 & 最大自动重试次数
_RATE_LIMIT_CODE = 99991400
_RATE_LIMIT_MAX_RETRIES = 5


def request(
    method,
    url,
    headers,
    payload={},
    params=None,
    refresh_client=None,
    data=None,
    proxy={},
    timeout=(10, 60),
):
    """Lark API Request

    Args:
        timeout: 请求超时时间，默认 (10, 60) 元组。
                 connect_timeout=10s（连接建立），read_timeout=60s（读取响应）。
                 防止 API 无响应时无限挂起。

    频控处理:
        当收到 99991400 (frequency limit) 或 HTTP 429 时，
        读取 x-ogw-ratelimit-reset 响应头自动等待后重试，
        最多重试 _RATE_LIMIT_MAX_RETRIES 次。
    """
    for rate_limit_attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
        response = requests.request(
            method,
            url,
            headers=headers,
            json=payload,
            params=params,
            data=data,
            proxies=proxy,
            timeout=timeout,
        )

        resp = {}
        if response.text[0] == "{":
            resp = response.json()
        else:
            logger.info("response:\n" + response.text)
        code = resp.get("code", -1)
        if code == -1:
            code = resp.get("StatusCode", -1)
        if code == -1 and response.status_code != 200:
            response.raise_for_status()

        # ---- 频控自动重试 ----
        is_rate_limited = code == _RATE_LIMIT_CODE or response.status_code == 429
        if is_rate_limited and rate_limit_attempt < _RATE_LIMIT_MAX_RETRIES:
            retry_after = int(response.headers.get("x-ogw-ratelimit-reset", 0))
            # 指数退避兜底：header 缺失时 2^attempt 秒
            wait = max(retry_after, 2**rate_limit_attempt)
            logger.warning(
                "Rate limited (code=%s, attempt=%d). "
                "Waiting %ds per x-ogw-ratelimit-reset header ...",
                code,
                rate_limit_attempt + 1,
                wait,
            )
            time.sleep(wait)
            continue

        if code != 0:
            logger.error("Error Response: {0}".format(resp))
        return resp

    # 所有重试均被限频，最后一次仍然返回响应让调用方处理
    logger.error(
        "Rate limit persisted after %d retries for url=%s",
        _RATE_LIMIT_MAX_RETRIES,
        url,
    )
    return resp
