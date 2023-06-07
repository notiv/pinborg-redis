# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join


class PinItem(Item):
    # define the fields for your item here like:
    url_id = Field()
    url = Field()
    url_slug = Field()
    url_count = Field()
    title = Field()
    created_at = Field()
    pin_fetch_date = Field()
    tags = Field() # array of tags
    author = Field()

class PageItem(Item):
    page_url = Field()
    page_url_slug = Field()
    page_fetch_date = Field()
    page_code = Field()
    page_content = Field()
    page_content_size = Field()

class UrlSlugItem(Item):
    # fields that we get when parsing the
    # url_slug (e.g. https://pinboard.in/url:f81a7954a8ab701aa47ddaef236d90fea167dfae/)
    url_slug = Field()
    url = Field()
    pin_url = Field()
    user_list = Field()  # array of users who have saved this pin as well
    user_list_length = Field() # number of users who have saved this pin as well
    all_tags = Field()  # array of tags from all users
    url_slug_fetch_date = Field()