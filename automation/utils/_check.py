#coding:utf8
"""
校验用工具

"""


def _post_content_validate(msg_type, **kwargs):
	"""校验推送消息类型有效性
	
	"""
	# TODO: 发送文本内容，富文本内容、图片内容、群名片
	if msg_type in ("text", "post") and "content" in kwargs:
		return {
			"content": kwargs.get("content")
		}
	elif msg_type in ("image") and "content" in kwargs:
		if "image_key" not in kwargs.get("content"):
			raise Exception("缺少图片信息")
		return {
			"content": kwargs.get("content")
		}
	# TODO: 发送文件、多媒体、音频和飞书表情包
	elif msg_type in ("media", "file", "audio", "sticker") and "content" in kwargs:
		if "file_key" not in kwargs.get("content"):
			raise Exception("缺少文件信息")
		return {
			"content": kwargs.get("content")
		}
	# TODO: 发送消息卡片内容
	elif msg_type == "interactive" and "card" in kwargs:
		return {
			"card": kwargs.get("card")
		}
	
	else:
		raise Exception(f"发送信息类型 '{msg_type}' 和内容信息不匹配: '{kwargs}'")