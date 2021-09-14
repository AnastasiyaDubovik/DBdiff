import psycopg2
import collections
from psycopg2 import sql
from configparser import ConfigParser

"""
Сравнение двух баз данных на уровне данных. 
То что касаеться insert просто через loadData. Что делать с Update и delete ?
"""


class ConnectDB:
    """
    Подключение к базе данных. Выполнение запросов ...
    """

    def __init__(self, name_section: str):
        self.name_section = name_section

    def _get_param_db(self):
        config_parameters = tuple('')
        config_parser = ConfigParser()
        config_parser.read('DBConnection.ini', encoding='UTF8')
        if config_parser.has_section(self.name_section):
            config_parameters = config_parser.items(self.name_section)
        return dict(config_parameters)

    def get_result_query(self, query, type_output=True):
        """
        Подключение к БД и получение данных по запросу(только SELECT)
        :query SQL запрос в базу данных:
        :return возвращает список кортежей :
        """
        param_conn = self._get_param_db()
        conn = psycopg2.connect(**param_conn)
        cur = conn.cursor()
        cur.execute(query)
        if type_output:
            result_query = cur.fetchall()
        else:
            result_query = cur.fetchone()
        conn.commit()
        conn.close()
        return result_query


class AnalyzeDB:
    def __init__(self, source_db: ConnectDB, target__db: ConnectDB):
        self.source_db = source_db
        self.target_db = target__db

    def get_diff_table(self):
        query = "SELECT schemaname,relname, n_tup_ins,n_tup_upd, n_tup_del,n_dead_tup FROM pg_stat_user_tables " \
                "WHERE relname NOT IN ('databasechangelog','databasechangeloglock') "
        target_list = self.target_db.get_result_query(query)
        source_list = self.source_db.get_result_query(query)
        diff_value = list(set(target_list) - set(source_list))
        return [name_obj[:2] for name_obj in diff_value]

    def choose_operation(self, list_table: list):
        """
        Сравнивает количество строк в таблицах source и target.
        В зависимости от сравнения будет выбран опратор INSERT, DELETE или UPDATE.
        :param list_table:
        :return:
        """
        type_diff = collections.defaultdict(list)
        for i in list_table:
            query_count = sql.SQL("SELECT count(*) FROM {}.{}").format(sql.Identifier(i[0]), sql.Identifier(i[1]))
            count_target = self.target_db.get_result_query(query_count, False)
            count_source = self.source_db.get_result_query(query_count, False)
            if int(count_source[0]) == int(count_target[0]):
                type_diff['UPDATE'].append(i)
            elif int(count_target[0]) > int(count_source[0]):
                type_diff['INSERT'].append(i)
            else:
                type_diff['DELETE'].append(i)
        return type_diff

    def get_diff_insert(self, list_obj: list):
        """
        Возвращает данные которые нужно будет записать в source базу данных.
        :param list_obj:
        :return:
        """

        pass

    def get_diff_update(self):
        pass

    def get_diff_delete(self):
        pass


if __name__ == '__main__':
    source__db = ConnectDB('source')
    target_db = ConnectDB('target')
    test_analyze = AnalyzeDB(source__db, target_db)
    res = test_analyze.get_diff_table()
    dict_res = test_analyze.choose_operation(res)
    print(dict_res)
