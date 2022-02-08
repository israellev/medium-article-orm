from utils import Utils
from utils_sql import UtilsSql


class DtoBase:
    TABLE_NAME = ""
    CLASS_DTO = None
    BASE_WHERE = {}

    def __init__(self, db):
        self._db = db

    def _insert_dto_obj(self, dto_obj, except_values=None, **extra_columns):
        """
        It will create:
                'INSERT INTO users (id) VALUES (%s)' with params ['1']
        From parameters of:
                 table_name="users",
                 dto={"id": 1, "name": "x"},
                 except_values=["name"],
         """
        dto_obj = Utils.MergeTwoDicts(dto_obj, extra_columns)
        dto_obj = Utils.MergeTwoDicts(dto_obj, self.BASE_WHERE)
        except_values = except_values or ["id"]
        names_list = [name for name in dto_obj.keys() if name not in except_values]
        names, str_params = UtilsSql.get_dynamic_insert_names_and_string_params(names_list, except_values=except_values)
        sql = """INSERT INTO {} ({})
        VALUES ({})""".format(self.TABLE_NAME, names, str_params)
        params = [dto_obj.get(name) for name in names_list]
        return self._db.insert(sql, safe_params=params)

    def _get_dto_list(self, order_by=None, limit=None, custom_where="", **where_dict):
        dto_names_list = Utils.GetClassListValues(self.CLASS_DTO)
        select_names_list = UtilsSql.get_column_names_from_list(dto_names_list)
        where, params = self.__get_where_and_params(where_dict)
        sql = """SELECT {}
        FROM {} 
        {} {} {} {}""".format(
            select_names_list,
            self.TABLE_NAME,
            where,
            custom_where,
            "" if not order_by else "\nORDER BY {}".format(order_by),
            "" if not limit else "\nLIMIT {}".format(int(limit)),
        )
        dto_list = self.__select_dto(sql, params, dto_names_list) or []
        return dto_list

    def __get_where_and_params(self, where_dict):
        params = []
        where = ""
        where_dict = Utils.MergeTwoDicts(where_dict, self.BASE_WHERE)
        names = where_dict.keys()
        for i, name in enumerate(names):
            where += " WHERE" if not where else " AND"
            param = where_dict.get(name)
            if Utils.IsList(param):
                list_names = UtilsSql.get_string_from_list(param)
                where += " {} IN ({})\n".format(name, list_names)
                continue
            where += " {} = %s\n".format(name)
            if isinstance(param, bool):
                param = 1 if param else 0
            params.append(param)
        return where, params

    def _get_dto(self, order_by="id DESC", **where):
        apps = self._get_dto_list(order_by=order_by, limit=1, **where)
        if Utils.IsList(apps) and len(apps) > 0:
            return apps[0]

    def __select_dto(self, query, safe_params, dto_names_list):
        rows = self._db.select(query, safe_params=safe_params)
        dto_list = UtilsSql.get_dto_list_from_rows(rows, dto_names_list)
        return dto_list

    def _get_dto_list_where(self, where, params):
        dto_names_list = Utils.GetClassListValues(self.CLASS_DTO)
        select_names_list = UtilsSql.get_column_names_from_list(dto_names_list)
        sql = """SELECT {}
        FROM {}
        {}""".format(select_names_list, self.TABLE_NAME, where)
        dto_list = self.__select_dto(sql, params, dto_names_list) or []
        return dto_list

    def _update_dto(self, where_dict, **set_values):
        where, params = self.__get_where_and_params(where_dict)
        set_sql = ""
        set_names = set_values.keys()
        for i, name in enumerate(set_names):
            set_sql += " `{}` = %s".format(name)
            if i != (len(set_names) - 1):
                set_sql += ","
            params.insert(i, set_values.get(name))
        sql = """UPDATE {} SET {} {}""".format(
            self.TABLE_NAME, set_sql, where
        )
        return self._db.update(sql, safe_params=params)
