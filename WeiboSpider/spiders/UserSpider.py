from scrapy.spiders import CrawlSpider, Rule, Request
from scrapy.linkextractors import LinkExtractor
import WeiboSpider.project_setting as settings
from WeiboSpider.MyScrapyRedis.spiders import RedisCrawlSpider
import json
from scrapy.utils.serialize import ScrapyJSONEncoder
import pymongo

connection = pymongo.MongoClient('120.25.75.23', 27017)
tdb = connection.weibo
post = tdb.user


class UserSpider(RedisCrawlSpider):
    name = 'u'

    redis_key = 'u:start_ids'

    user_info_url = "https://api.weibo.com/2/users/show.json?access_token={0}&uid={1}"
    token = settings.Token

    def __init__(self, *a, **kwargs):
        super().__init__(*a, **kwargs)

    def make_request_from_data_str(self, data_str):
        return Request(url=self.datastr_to_url(data_str), meta={'id': data_str}, dont_filter=False)

    def datastr_to_url(self, data_str):
        return self.user_info_url.format(self.token, data_str)

    def parse(self, response):
        user_info_json = None
        if isinstance(response.body,bytes):
            user_info_json =json.loads( response.body.decode('utf-8'))
        else:
            user_info_json = json.loads(response.body)
        user_info_json['_id'] = user_info_json['id']
        post.update({"_id": user_info_json['_id']}, {"$setOnInsert":user_info_json}, upsert=True)
