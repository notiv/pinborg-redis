
import datetime
import json
import math
import re

from bs4 import BeautifulSoup
from ..items import PageItem, PinItem, UrlSlugItem

from scrapy_redis.spiders import RedisSpider

from scrapy import Request
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor

DIFF_MAX_DATE_TO_1970_IN_SECS = math.floor((
    datetime.datetime.max - 
    datetime.datetime.min).total_seconds())

DEFAULT_USER = 'notiv'

class PinSpider(RedisSpider):
    """Spider that reads urls from redis queue (myspider:start_urls)."""
    name = 'pinborg_redis'
    redis_key = 'pinborg_spider:start_urls'

    def __init__(self, 
                 user=DEFAULT_USER,
                 before=DIFF_MAX_DATE_TO_1970_IN_SECS,
                 *args, 
                 **kwargs):
        super(PinSpider, self).__init__(*args, **kwargs)
        self.start_urls = [f'https://pinboard.in/u:{user}/before:{before}']
        self.rules = (
            Rule(LinkExtractor(deny=('twitter\.com')))
        )

        self.start_user = user
        self.before = before
        self.re_url_extract = re.compile('url:(.*)')


    def parse(self, response):
        bookmarks = re.findall(
            'bmarks\[\d+\] = (\{.*?\});',
            response.body.decode('utf-8'),
            re.DOTALL | re.MULTILINE
        )

        for b in bookmarks:
            bookmark = json.loads(b)
            yield from self.parse_bookmark(bookmark)
        
        # Get bookmarks in previous pages
        previous_page = response.css('a#top_earlier::attr(href)').extract_first()
        if previous_page:
            previous_page = response.urljoin(previous_page)
            self.logger.info(f'[PINBORG_REDIS] Fetching previous page: {previous_page}')
            yield Request(previous_page, callback=self.parse)

    def parse_bookmark(self, bookmark):
        pin = PinItem()

        pin['id'] = bookmark['id']
        pin['url'] = bookmark['url']
        pin['url_slug'] = bookmark['url_slug']
        pin['url_count'] = bookmark['url_count']
        pin['title'] = bookmark['title']

        created_at = datetime.datetime.strptime(bookmark['created'], 
            '%Y-%m-%d %H:%M:%S')
        pin['created_at'] = created_at.isoformat()
        pin['pin_fetch_date'] = datetime.datetime.utcnow().isoformat()

        pin['tags'] = bookmark['tags']
        pin['author'] = bookmark['author']

        yield pin

        if self.settings.get('PARSE_EXTERNAL_LINKS'):
            yield Request(pin['url'], callback=self.parse_external_page, 
                meta={'url_slug': pin['url_slug']}, priority=2)
        
        yield Request('https://pinboard.in/url:' + pin['url_slug'], 
            callback=self.parse_url_slug, priority=1)


    def parse_url_slug(self, response):
        url_slug = UrlSlugItem()

        if response.body:
            soup = BeautifulSoup(response.body, 'html.parser')

            self.crawler.stats.inc_value('url_slug_count')

            pin_url = soup.find('a', href=re.compile('^https?://'))['href']
            tagcloud = soup.find_all('div', id='tag_cloud')
            all_tags = [element.get_text() 
                for element in tagcloud[0].find_all(class_='tag')]
            
            users = soup.find_all('div', class_='bookmark')
            user_list = [re.findall('/u:(.*)/t:', element.a['href'], re.DOTALL) for element in users]
            user_list = sum(user_list, []) # Change from list of lists to list

            url_slug['url_slug'] = re.findall('url:(.*)', response.url)[0]
            url_slug['url'] = response.url
            url_slug['pin_url'] = pin_url
            url_slug['user_list'] = user_list
            url_slug['user_list_length'] = len(user_list)
            url_slug['all_tags'] = all_tags
            url_slug['url_slug_fetch_date'] = datetime.datetime.utcnow().isoformat()

            yield url_slug

            for user in user_list:
                # We ignore any new pins from users already parsed. 
                if user in self.users_parsed: 
                    self.logger.info(f'[PINBORG_REDIS] User {user} already parsed.')
                else:
                    yield Request(
                        f'https://pinboard.in/u:{user}/before:{self.before}', 
                        callback=self.parse)