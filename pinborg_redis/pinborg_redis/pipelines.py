# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

import json
import pathlib
import psycopg2

from datetime import datetime
from itemadapter import ItemAdapter
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
DATETIME2_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

DEFAULT_PINS_FOLDER = './parsed/pins'
DEFAULT_URLSLUGS_FOLDER = './parsed/urlslugs'
DEFAULT_PAGES_FOLDER = './parsed/pages'

def get_item_type(item):
    return type(item).__name__.replace('Item', '').lower()  # PinItem => pin

class PinborgJsonPipeline:
    def open_spider(self, spider):
        pathlib.Path(DEFAULT_PINS_FOLDER).mkdir(parents=True, exist_ok=True)
        pathlib.Path(DEFAULT_URLSLUGS_FOLDER).mkdir(parents=True, exist_ok=True)
        pathlib.Path(DEFAULT_PAGES_FOLDER).mkdir(parents=True, exist_ok=True)

    def close_spider(self, spider):
        requests_to_be_parsed = len(spider.crawler.engine.slot.scheduler)
        if requests_to_be_parsed:
            spider.logger.info(f'''[PINBORG_REDIS] There are {requests_to_be_parsed} requests 
            in the queue that will not be parsed''')

        spider.logger.info('[PINBORG_POSTGRES] Closing spider')


    def process_item(self, item, spider):
        item_type = get_item_type(item)

        if item_type == 'pin':
            author = item['author']
            url_slug = item['url_slug']
            file = open(f'{DEFAULT_PINS_FOLDER}/{item_type}_{author}_{url_slug}.jl', 'w')
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
    
class PinborgPostgresPipeline:
    def __init__(self, db_hostname='localhost', db_username='notiv', 
                 db_password='', database='pinborg'):
        # Connection details
        self.hostname = db_hostname
        self.db_username = db_username
        self.db_password = db_password 
        self.default_admin_database = 'postgres'
        self.database = database

        if not self._check_if_database_exists(self.database):
            self._create_database(self.database)

        # Create a persistent connection to the database
        self.connection = psycopg2.connect(
            host = self.hostname,
            user = self.db_username,
            password = self.db_password,
            dbname = self.database
        )

        # Create a persistent cursor to perform database operations
        self.cursor = self.connection.cursor()

        if not self._check_if_table_exists('PIN'):
            self._create_pin_table()
        
        if not self._check_if_table_exists('URLSLUG'):
            self._create_urlslug_table()

        if not self._check_if_table_exists('PAGE'):
            self._create_page_table()

    def _check_if_database_exists(self, database):
        # NOTE: The connection and the cursor are temporary (try to access
        # the default_admin_database). Outside this function we connect to
        # the pinborg database with a "persistent" connection. 
        # Connect to the server
        temp_conn = psycopg2.connect(
            host = self.hostname,
            user = self.db_username,
            password = self.db_password,
            dbname = self.default_admin_database
        )

        # Open a cursor to perform database operations
        temp_cur = temp_conn.cursor()

        # Query the list of databases
        temp_cur.execute('SELECT datname FROM pg_database WHERE datistemplate = false;')

        # Fetch all the rows
        rows = temp_cur.fetchall()

        # Check if the database exists
        database_exists = False
        if (database,) in rows:
            database_exists = True
            print(f'The {database} database exists.')
        else:
            print(f'The {database} database does not exist.')

        # Close the cursor and the connection
        temp_cur.close()
        temp_conn.close()

        return database_exists

    def _create_database(self, database):
        # NOTE: The connection and the cursor are temporary (try to access
        # the default_admin_database). Outside this function we connect to
        # the pinborg database with a "persistent" connection. 
        #
        # Connect to the server
        temp_conn = psycopg2.connect(
            host=self.hostname,
            user=self.db_username,
            password=self.db_password,
            dbname=self.default_admin_database
        )

        temp_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Open a cursor to perform database operations
        temp_cur = temp_conn.cursor()

        # Create the database
        temp_cur.execute(f'''CREATE DATABASE {database}
                    WITH 
                    OWNER = {self.db_username}
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1
                    IS_TEMPLATE = False;''')

        # Close the cursor and the connection
        temp_cur.close()
        temp_conn.close()

    def _check_if_table_exists(self, table_name):

        # Query the list of tables
        self.cursor.execute('SELECT table_name FROM information_schema.tables;')

        # Fetch all the rows
        rows = self.cursor.fetchall()

        # Check if the table exists
        table_exists = False
        if (table_name,) in rows:
            table_exists = True
            print(f'The {table_name} table exists.')
        else:
            print(f'The {table_name} table does not exist.')

        return table_exists
    
    def _create_pin_table(self):

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS PIN(
                url_id integer PRIMARY KEY, 
                url text,
                url_slug text,
                url_count integer,
                title text,
                created_at timestamp,
                pin_fetch_date timestamp,
                tags text[],
                author character(255)
            );

            CREATE INDEX IF NOT EXISTS pin_url_id ON PIN (url_id);
            """
        )
        
        # Commit, close the cursor and the connection
        self.connection.commit()

    def _create_urlslug_table(self):

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS URLSLUG(
                url_slug text PRIMARY KEY, 
                url text,
                pin_url text,
                user_list text[],
                user_list_length integer,
                all_tags text[],
                url_slug_fetch_date timestamp
            );

            CREATE INDEX IF NOT EXISTS urlslug_url_slug ON URLSLUG (url_slug);
            """
        )
        
        # Commit, close the cursor and the connection
        self.connection.commit()

    def _create_page_table(self):

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS PAGE(
                page_url_slug text PRIMARY KEY,
                page_url text,
                page_fetch_date timestamp,
                page_code text,
                page_content text,
                page_content_size integer
            );

            CREATE INDEX IF NOT EXISTS page_url_slug ON PAGE (page_url_slug);
            """
        )
       
        # Commit, close the cursor and the connection
        self.connection.commit()

    def close_spider(self, spider):
        requests_to_be_parsed = len(spider.crawler.engine.slot.scheduler)
        if requests_to_be_parsed:
            spider.logger.info(f'''[PINBORG_REDIS] There are {requests_to_be_parsed} requests 
            in the queue that will not be parsed''')

        spider.logger.info('[PINBORG_POSTGRES] Closing spider')

        # Close the "persistent" connection and the cursor
        self.connection.close()
        self.cursor.close()

    def process_item(self, item, spider):
        item_type = get_item_type(item)

        if item_type == 'pin':
            self._insert_into_pin_table(item)
        elif item_type == 'urlslug':
            self._insert_into_urlslug_table(item)
        elif item_type == 'page':
            self._insert_into_page_table(item)
        
        return item
    
    def _insert_into_pin_table(self, item):
        url_id = item['url_id']
        url = item['url']
        url_slug = item['url_slug'] 
        url_count = item['url_count']
        title = item['title']
        created_at = datetime.strptime(item['created_at'], DATETIME_FORMAT)
        pin_fetch_date = datetime.strptime(item['pin_fetch_date'], DATETIME2_FORMAT)
        tags = item['tags']
        author = item['author']

        self.cursor.execute(f"""
            INSERT INTO PIN(
                url_id, url, url_slug, url_count, title, created_at, pin_fetch_date, tags, author
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, ARRAY[%s], %s)
            ON CONFLICT (url_id) DO NOTHING;""",
            (url_id, url, url_slug, url_count, title, created_at, pin_fetch_date, tags, author)
            )

        self.connection.commit()

    def _insert_into_urlslug_table(self, item):
        url_slug = item['url_slug']
        url = item['url']
        pin_url = item['pin_url']
        user_list = item['user_list']
        user_list_length = item['user_list_length']
        all_tags = item['all_tags']
        url_slug_fetch_date = datetime.strptime(item['url_slug_fetch_date'], DATETIME2_FORMAT)

        self.cursor.execute(f"""
            INSERT INTO URLSLUG(
                url_slug, url, pin_url, user_list, user_list_length, all_tags, url_slug_fetch_date)
            VALUES(%s, %s, %s, ARRAY[%s], %s, ARRAY[%s], %s)
            ON CONFLICT (url_slug) DO NOTHING;""",
            (url_slug, url, pin_url, user_list, user_list_length, all_tags, url_slug_fetch_date))
        
        self.connection.commit()

    def _insert_into_page_table(self, item):
        page_url_slug = item['page_url_slug']
        page_url = item['page_url']
        page_fetch_date = datetime.strptime(item['page_fetch_date'], DATETIME2_FORMAT)
        page_code = item['page_code']
        page_content = item['page_content']
        page_content_size = item['page_content_size']

        self.cursor.execute(f"""
            INSERT INTO PAGE(
                page_url_slug, page_url, page_fetch_date, page_code, page_content, page_content_size)
            VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT (page_url_slug) DO NOTHING;""",
            (page_url_slug, page_url, page_fetch_date, page_code, page_content, page_content_size))
        
        self.connection.commit()
