import math
import re

import aiomysql

from fairy.config import db_settings


class BaseDB:

    def __init__(self):
        self._pool = None

    async def connect(self):
        assert self._pool is None, 'database is already running'
        self._pool = await aiomysql.create_pool(
            host=db_settings.DB_HOST,
            port=db_settings.DB_PROT,
            db=db_settings.DB_NAME,
            user=db_settings.DB_USER,
            password=db_settings.DB_PASS,
            minsize=5,
            maxsize=20
        )

    async def disconnect(self):
        assert self._pool is not None, 'database is not running'
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None


db = BaseDB()


class BaseModel:

    def __init__(self):
        self._conn = None
        self.cursor = None

    async def conn(self):
        if not self.cursor:
            if not self._conn:
                assert db._pool is not None, 'database is not running'
                self._conn = await db._pool.acquire()
            self.cursor = await self._conn.cursor()

    async def begin(self):
        await self._conn.begin()

    async def rollback(self):
        await self._conn.rollback()

    async def execute(self, sql_str, values=None):
        try:
            await self.cursor.execute(sql_str, values)
        except Exception as e:
            await self.rollback()
            await self.close()
            raise ConnectionAbortedError(str(e))

    async def executemany(self, sql_str, values=None):
        try:
            await self.cursor.executemany(sql_str, values)
        except Exception as e:
            await self.rollback()
            await self.close()
            raise ConnectionAbortedError(str(e))

    async def commit(self):
        try:
            await self._conn.commit()
        except Exception as e:
            await self.rollback()
            raise ConnectionAbortedError(str(e))

    async def close(self):
        try:
            if self.cursor:
                await self.cursor.execute('UNLOCK TABLES;')
                await self.cursor.close()
        finally:
            db._pool.release(self._conn)
            self.cursor = None
            self._conn = None


class Common:

    @staticmethod
    async def parse_limit_str(page_info=None):
        if page_info is None:
            page_info = {}
        page = int(page_info.get('page', 1))
        page_size = int(page_info.get('rows', 20))
        limit_str = ' LIMIT %s, %s ' % ((page - 1) * page_size, page_size)
        return limit_str

    @staticmethod
    async def parse_update_str(table, p_key, p_id, update_dict):
        sql_str = ' UPDATE %s SET ' % (table,)
        temp_str = []
        sql_values = []
        for key, value in update_dict.items():
            temp_str.append(key + ' = %s ')
            sql_values.append(value)
        sql_str += ', '.join(r for r in temp_str) + ' WHERE ' + p_key + ' = %s '
        sql_values.append(p_id)
        return sql_str, sql_values

    @staticmethod
    async def parse_where_str(filter_fields, request_data):
        if not isinstance(filter_fields, tuple) and not isinstance(filter_fields, list):
            filter_fields = (filter_fields,)
        where_str = ' WHERE 1 = %s '
        where_values = [1]
        for key in filter_fields:
            value = request_data.get(key)
            if value:
                where_str += ' AND ' + key + ' = %s '
                where_values.append(value)
        return where_str, where_values

    @staticmethod
    async def parse_where_like_str(filter_fields, request_data):
        if not isinstance(filter_fields, tuple) and not isinstance(filter_fields, list):
            filter_fields = (filter_fields,)
        where_str = ' WHERE 1 = %s '
        where_values = [1]
        for key in filter_fields:
            value = request_data.get(key)
            if value:
                where_str += ' AND ' + key + ' LIKE %s '
                where_values.append('%%%%%s%%%%' % value)
        return where_str, where_values

    @staticmethod
    async def get_page_info(cursor, sql_str, where_values=None, truncate=False, page=1, per_page=20):
        page = int(page)
        per_page = int(per_page)

        if truncate:
            if 'GROUP BY' in sql_str:
                sql_str = 'SELECT COUNT(*) total FROM (%s) AS TEMP' % sql_str
            else:
                sql_str = re.sub(r'SELECT[\s\S]*?FROM', 'SELECT COUNT(*) total FROM', sql_str, count=1)

        # 从原始 SQL 删除 ORDER BY 和 LIMIT （用于计算总数）
        if 'ORDER BY' in sql_str:
            sql_str = sql_str[:sql_str.find('ORDER BY')]
        if 'LIMIT' in sql_str:
            sql_str = sql_str[:sql_str.find('LIMIT')]

        # 执行查询以获得总记录数
        if where_values:
            await cursor.execute(sql_str, where_values)
        else:
            await cursor.execute(sql_str)
        (total,) = await cursor.fetchone()

        # 计算分页信息
        pages = math.ceil(total / per_page)
        next_num = page + 1 if page < pages else None
        has_next = page < pages
        prev_num = page - 1 if page > 1 else None
        has_prev = page > 1

        # 构建并返回分页信息字典
        page_info = {
            'page': page,
            'per_page': per_page,  # 每页显示的记录数
            'pages': pages,  # 总页数
            'total': total,
            'next_num': next_num,
            'has_next': has_next,
            'prev_num': prev_num,
            'has_prev': has_prev
        }
        return page_info
