#coding:utf8
"""
设置飞书机器人对象
"""

from __future__ import absolute_import

import hashlib
import base64
import hmac
import time
import requests
from requests_toolbelt import MultipartEncoder


from .._check import _post_content_validate
class LarkRobot:
	def __init__(self, webhook, secret=None, app_id=None, app_secret=None) -> None:
		self._webhook = webhook
		# 机器人密钥可能为空
		self._secret = secret

		# 获取 token 相关信息
		self._app_id = app_id
		self._app_secret = app_secret

		self._token = None

	
	@property
	def token(self):
		if self._token is None:
		
			url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
			headers = {"Content-Type": "text/plain"}
			res = requests.post(
				url = url,
				headers=headers,
				json={
					"app_id": self._app_id,
					"app_secret": self._app_secret
				}
			)
			
			self._token = res.json()["tenant_access_token"]
		return self._token

	

	def gen_sign(self, timestamp=None):
		"""生成校验签名
		
		签名的算法：把 timestamp + "\n" + 密钥 当做签名字符串，使用 HmacSHA256 算法计算
		签名，再进行 Base64 编码。
		"""

		timesamp = timestamp if timestamp is None else int(time.time())
		hmac_code = hmac.new(
			f"{timesamp}\n{self._secret}".encode("utf-8"), digestmod=hashlib.sha256
		).digest()

		# 签名后编码
		return base64.b64encode(hmac_code).decode('utf-8')

	
	def post(self, msg_type, **kwargs):
		"""推送消息

		推送相关消息，可以推送简单文本和富文本
		"""
		params = {
			"timestamp": int(time.time()),
			"sign": self.gen_sign(),
			"msg_type": msg_type
		}

		params.update(_post_content_validate(msg_type, **kwargs))

		# 发送请求信息
		res = requests.post(self._webhook, json=params)

		if res.status_code == 200:
			raise Exception("请求失败")
		
		body = res.json()
		if body.get("code") != 0:
			# TODO: 修改为日志
			raise Exception(f"发送消息失败, 返回消息: '{body}'")
		
		# TODO: 修改为日志
		print(f"消息发送成功, 内容为 '{params}'")
	

	def _upload_image(self, file):
		"""图片上传"""
		url = "https://open.feishu.cn/open-apis/im/v1/images"

		form = {'image_type': 'message',
				'image': (open(file, 'rb'))}
		multi_form = MultipartEncoder(form)
		headers = {
			'Authorization': f'Bearer {self.token}', 
		}
		headers['Content-Type'] = multi_form.content_type
		response = requests.request("POST", url, headers=headers, data=multi_form)
		# print(response.headers['X-Tt-Logid'])  # for debug or oncall
		# print(response.content)  # Print Response
		return response.json()