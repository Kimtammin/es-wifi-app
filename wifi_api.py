import urllib.request as rq
import json

url = 'http://openapi.seoul.go.kr:8080/(인증키)/TbPublicWifiInfo/1/500'
response = rq.urlopen(url)
json_str = response.read().decode('utf-8')
data = json.loads(json_str)
print(json.dumps(data, indent=4, ensure_ascii = False))
