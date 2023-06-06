# Script to use for debugging pinborg-redis in VS Code
import os
from scrapy import cmdline

os.chdir(os.path.dirname(os.path.realpath(__file__)))

cmdline.execute('crapy crawl pinborg_redis'.split())