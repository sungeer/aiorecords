from todoist.utils import BaseModel, Common


class UserModel(BaseModel):

    async def get_users(self, params):
        params = params.__dict__
        sql_str = '''
            SELECT id, um, password, name, is_admin, creat_time
            FROM user
        '''
        filter_fields = ['um', 'password', 'name', 'is_admin', 'creat_time']
        where_str, where_values = await Common.parse_where_str(filter_fields, params)
        limit_str = await Common.parse_limit_str(params)
        await self.conn()
        await self.execute((sql_str + where_str + limit_str), where_values)
        data = await self.cursor.fetchall()
        page = int(params.get('page', 1))
        per_page = int(params.get('size', 20))
        page_info = await Common.get_page_info(self.cursor, sql_str + where_str, where_values, truncate=True, page=page, per_page=per_page)
        await self.close()
        page_info.update({'data': data})
        return page_info

    async def get_user_by_id(self, user_id):
        sql_str = '''
            SELECT id, um, password, name, is_admin, creat_time
            FROM user
            WHERE id = %s
        '''
        await self.conn()
        await self.execute(sql_str, (user_id,))
        data = await self.cursor.fetchone()
        await self.close()
        return data

    async def get_user_by_um(self, um):
        sql_str = '''
            SELECT id, um, password, name, is_admin, creat_time
            FROM user
            WHERE um = %s
        '''
        await self.conn()
        await self.execute(sql_str, (um,))
        data = await self.cursor.fetchone()
        await self.close()
        return data

    async def add_user(self, user):
        sql_str = '''
            INSERT INTO user (um, password, name, is_admin, creat_time)
            VALUES (%s, %s, %s, %s, %s)
        '''
        await self.conn()
        await self.execute(sql_str, (user.um, user.password, user.name, user.is_admin, user.creat_time))
        await self.commit()
        user_id = await self.cursor.lastrowid  # 新增数据的ID
        await self.close()
        return user_id

    async def delete_user(self, user_id):
        sql_str = '''
            DELETE FROM user WHERE id = %s
        '''
        await self.conn()
        await self.execute(sql_str, (user_id,))
        await self.commit()
        row = await self.cursor.rowcount  # 执行的数量
        await self.close()
        return row

    async def update_user(self, user):
        sql_str = '''
            UPDATE user SET password = %s, name = %s, is_admin = %s WHERE id = %s
        '''
        await self.conn()
        await self.execute(sql_str, (user.password, user.name, user.is_admin, user.id))
        await self.commit()
        row = await self.cursor.rowcount  # 执行的数量
        await self.close()
        return row
