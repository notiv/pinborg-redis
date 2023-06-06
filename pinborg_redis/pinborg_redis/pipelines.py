# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

import json
import pathlib
import psycopg2

from itemadapter import ItemAdapter
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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

        spider.logger.info('[PINBORG_POSTGRES] Closing spider')


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

        if not self._check_if_table_exists('PIN', self.database):
            self._create_pin_table(self.database)
        
        if not self._check_if_table_exists('URLSLUG', self.database):
            self._create_urlslug_table(self.database)

    def _check_if_database_exists(self, database):

        # Connect to the server
        conn = psycopg2.connect(
            host = self.hostname,
            user = self.db_username,
            password = self.db_password,
            dbname = self.default_admin_database
        )

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Query the list of databases
        cur.execute('SELECT datname FROM pg_database WHERE datistemplate = false;')

        # Fetch all the rows
        rows = cur.fetchall()

        # Check if the database exists
        database_exists = False
        if (database,) in rows:
            database_exists = True
            print(f'The {database} database exists.')
        else:
            print(f'The {database} database does not exist.')

        # Close the cursor and the connection
        cur.close()
        conn.close()

        return database_exists

    def _create_database(self, database):
        # Connect to the server
        conn = psycopg2.connect(
            host=self.hostname,
            user=self.db_username,
            password=self.db_password,
            dbname=self.default_admin_database
        )

        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Create the database
        cur.execute(f'''CREATE DATABASE {database}
                    WITH 
                    OWNER = {self.db_username}
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1
                    IS_TEMPLATE = False;''')

        # Close the cursor and the connection
        cur.close()
        conn.close()

    def _check_if_table_exists(self, table_name, database):
        # Connect to the server
        conn = psycopg2.connect(
            host=self.hostname,
            user=self.db_username,
            password=self.db_password,
            dbname=database
        )

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Query the list of tables
        cur.execute('SELECT table_name FROM information_schema.tables;')

        # Fetch all the rows
        rows = cur.fetchall()

        # Check if the table exists
        table_exists = False
        if (table_name,) in rows:
            table_exists = True
            print(f'The {table_name} table exists.')
        else:
            print(f'The {table_name} table does not exist.')

        # Close the cursor and the connection
        cur.close()
        conn.close()

        return table_exists
    
    def _create_pin_table(self, database):
        conn = psycopg2.connect(
            host=self.hostname,
            user=self.db_username,
            password=self.db_password,
            dbname=database
        )

        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS PIN(
                id serial PRIMARY KEY, 
                url text,
                url_id text,
                url_slug text,
                url_count integer,
                title text,
                created_at timestamp,
                pin_fetch_date timestamp,
                tags text[],
                author character(255)
            )
            """
        )
        
        # Commit, close the cursor and the connection
        conn.commit()
        cur.close()
        conn.close()

    def _create_urlslug_table(self, database):
        conn = psycopg2.connect(
            host=self.hostname,
            user=self.db_username,
            password=self.db_password,
            dbname=database
        )

        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS URLSLUG(
                url_slug text PRIMARY KEY, 
                url text,
                pin_url text,
                user_list text[],
                user_list_length integer,
                all_tags text[],
                url_slug_fetch_date timestamp
            )
            """
        )
        
        # Commit, close the cursor and the connection
        conn.commit()
        cur.close()
        conn.close()

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
        elif item_type == 'urlslug':
            url_slug = item['url_slug']
            pass
        elif item_type == 'page':
            pass
        
        return item
    
