import psycopg2
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

    def get_result_query(self, query):
        """
        Подключение к БД и получение данных по запросу(только SELECT)
        :query SQL запрос в базу данных:
        :return возвращает список кортежей :
        """
        param_conn = self._get_param_db()
        conn = psycopg2.connect(**param_conn)
        cur = conn.cursor()
        cur.execute(query)
        result_query = cur.fetchall()
        conn.commit()
        conn.close()
        return result_query


class AnalyzeDB:
    def __init__(self, source_db: ConnectDB, target__db: ConnectDB):
        self.source_db = source_db
        self.target_db = target__db

    def get_diff_table(self):
        query = "SELECT schemaname||'.'||relname, n_tup_ins,n_tup_upd, n_tup_del FROM pg_stat_user_tables"
        target_list = self.target_db.get_result_query(query)
        source_list = self.source_db.get_result_query(query)
        diff_value = list(set(target_list) - set(source_list))
        return diff_value

    def get_diff_update(self):
        pass

    def get_diff_delete(self):
        pass


if __name__ == '__main__':
    target_db = ConnectDB('default')
    res = target_db.get_result_query('SELECT version()')
    print(res)
