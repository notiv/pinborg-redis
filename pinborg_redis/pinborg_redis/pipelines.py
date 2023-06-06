# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

import json
import pathlib
import pyarrow.feather as feather

from itemadapter import ItemAdapter

DEFAULT_USERS_FOLDER = './parsed/users'
DEFAULT_URLSLUGS_FOLDER = './parsed/urlslugs'
DEFAULT_PAGES_FOLDER = './parsed/pages'

def get_item_type(item):
    return type(item).__name__.replace('Item', '').lower()  # PinItem => pin

class PinborgJsonPipeline:
    def open_spider(self, spider):
        pathlib.Path(DEFAULT_USERS_FOLDER).mkdir(parents=True, exist_ok=True)
        pathlib.Path(DEFAULT_URLSLUGS_FOLDER).mkdir(parents=True, exist_ok=True)
        pathlib.Path(DEFAULT_PAGES_FOLDER).mkdir(parents=True, exist_ok=True)

    def close_spider(self, spider):
        requests_to_be_parsed = len(spider.crawler.engine.slot.scheduler)
        if requests_to_be_parsed:
            spider.logger.info(f'''[PINBORG_REDIS] There are {requests_to_be_parsed} requests 
            in the queue that will not be parsed''')

    def process_item(self, item, spider):
        item_type = get_item_type(item)

        if item_type == 'pin':
            author = item['author']
            url_slug = item['url_slug']
            file = open(f'{DEFAULT_USERS_FOLDER}/{item_type}_{author}_{url_slug}.jl', 'w')
            line = json.dumps(dict(item)) + '\n'
            file.write(line)
            file.close()
        elif item_type == 'urlslug':
            url_slug = item['url_slug']
            file = open(f'{DEFAULT_URLSLUGS_FOLDER}/{item_type}_{url_slug}.jl', 'w')
            line = json.dumps(dict(item)) + '\n'
            file.write(line)
            file.close()
        elif item_type == 'page':
            url_slug = item['page_url_slug']
            file = open(f'{DEFAULT_PAGES_FOLDER}/{item_type}_{url_slug}.jl', 'w')
            line = json.dumps(dict(item)) + '\n'
            file.write(line)
            file.close()
        
        return item
    
