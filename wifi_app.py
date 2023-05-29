import urllib.request as rq
from xml.etree.ElementTree import fromstring, ElementTree   # xml을 파싱하기 위해서 import
from elasticsearch import Elasticsearch

"""
    한글 분석기와 위경도 데이터 타입을 이용해야 하기 때문에 먼저 mapping이 필요하다.
    kibana console 
    PUT seoul_wifi
    {
        "settings":{
            "analysis":{
                "analyzer":{
                    "korean":{
                        "tokenizer":"nori_tokenizer"
                    }
                }
            }
        },
        "mappings":{
            "properties":{
                "gu_nm":{"type":"keyword"},
                "place_nm":{"type":"text", "analyzer":"korean"},
                "instl_xy":{"type":"geo_point"}
            }
        }
    }
"""

es = Elasticsearch()

# 데이터 개수 19207개
# 1000개씩 나누어서 API 호출
for i in range(1, 21):
    iStart = (i-1) * 1000 + 1
    iEnd = i * 1000

    url = f'http://openapi.seoul.go.kr:8080/(인증키)/xml/TbPublicWifiInfo/{iStart}/{iEnd}/'
    response = rq.urlopen(url)
    xml_str = response.read().decode('utf-8')

    tree = ElementTree(fromstring(xml_str))     # XML 파싱
    root = tree.getroot()                       # getroot()로 최상단 루트태그 가져오기

    for row in root.iter("row"):
        gu_nm = row.find('X_SWIFI_WRDOFC').text
        place_nm = row.find('X_SWIFI_MAIN_NM').text
        place_x = float(row.find('LAT').text)
        place_y = float(row.find('LNT').text)

        doc = {
            "gu_nm": gu_nm,
            "place_nm": place_nm,
            "instl_xy": {
                "lat": place_y,
                "lon": place_x
            }
        }

        res = es.index(index="seoul_wifi", doc_type="_doc", body=doc)
    print("END", iStart, "~", iEnd)
print("END")
