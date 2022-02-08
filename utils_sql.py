class UtilsSql:

    @staticmethod
    def get_column_names_from_list(list_values, except_values=None):
        """return `a`, `b` from ['a', 'b'] """
        except_values = except_values or []
        return "{}".format(", ".join("`{}`".format(v) for v in list_values if v not in except_values))

    @staticmethod
    def get_string_from_list(list_values):
        """return 'a', 'b' from ['a', 'b'] """
        return "{}".format(", ".join("'{}'".format(v) for v in list_values))

    @staticmethod
    def get_dynamic_update_string(column_names):
        """return "a = %s, b = %s" from ["a", "b"] """
        sql = ""
        for i, name in enumerate(column_names):
            sql += "{} = %s".format(name)
            if i != len(column_names) - 1:
                sql += ", "
        return sql

    @staticmethod
    def get_string_params(list_items):
        """return [%s, %s] from [1, "a"] with any values"""
        return ["%s," if (i != len(list_items) - 1) else "%s" for i, item in enumerate(list_items)]

    @staticmethod
    def get_dynamic_insert_names_and_string_params(column_names, except_values=None):
        """return ["a, b", "%s, %s"]  from ["a", "b"] """
        except_values = except_values or []
        insert_names = ""
        string_params = ""
        for i, name in enumerate(column_names):
            if name in except_values:
                continue
            insert_names += "`{}`".format(name)
            string_params += "%s"
            if i != len(column_names) - 1:
                insert_names += ", "
                string_params += ", "
        return insert_names, string_params

    @staticmethod
    def get_column_in_local_time(hours_from_utc, time_column="event_time"):
        return "DATE_ADD({}, INTERVAL {} HOUR)".format(time_column, int(hours_from_utc or 0))

    @staticmethod
    def get_dto_list_from_rows(rows, names_list):
        result_list = []
        for row in (rows or []):
            dto = {}
            for i, name in enumerate(names_list or []):
                if name.startswith("is_"):
                    dto[name] = True if row[i] else False
                else:
                    dto[name] = row[i]
            result_list.append(dto)
        return result_list
