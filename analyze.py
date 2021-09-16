import psycopg2
import collections
import pandas
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

    def get_dataframe(self, query: str):
        param_conn = self._get_param_db()
        conn = psycopg2.connect(**param_conn)
        cur = conn.cursor()
        cur.execute(query)
        result_query = cur.fetchall()
        print(cur.description)
        df = pandas.DataFrame(result_query, columns=[val[0] for val in cur.description])
        conn.commit()
        conn.close()
        return df


class AnalyzeDB:
    def __init__(self, source_db: ConnectDB, target__db: ConnectDB):
        self.source_db = source_db
        self.target_db = target__db

    def get_diff_table(self):
        query = "SELECT schemaname,relname, n_tup_ins,n_tup_upd, n_tup_del FROM pg_stat_user_tables " \
                "WHERE relname NOT IN ('databasechangelog','databasechangeloglock') "
        target_list = self.target_db.get_result_query(query)
        source_list = self.source_db.get_result_query(query)
        diff_value = list(set(target_list) - set(source_list))
        return [name_obj[:2] for name_obj in diff_value]

    def operator_dml(self):
        """
        Записывает в словарь какие изменения были сделаны с данными в таблице(INSERT, UPDATE, DELETE).
        Сравнение на основе системной таблицы pg_stat_user_tables.
        Замечание: не совсем корректно обрабатываеться оператор DELETE в pg_stat_user_tables
        (Добовляет значения к n_tup_del, даже если транзакция не завершина)
        :return: dict -> {(schema, name table): [INSERT, UPDATE,DELETE]}
        """
        change_parameters = collections.defaultdict(list)
        name_table = self.get_diff_table()

        for i in name_table:
            type_ddm = []
            stmt = sql.SQL("SELECT n_tup_ins,n_tup_upd, n_tup_del FROM pg_stat_user_tables "
                           "WHERE schemaname = {} AND relname = {}").format(sql.Literal(i[0]), sql.Literal(i[1]))
            res_target = self.target_db.get_result_query(stmt, False)
            res_source = self.source_db.get_result_query(stmt, False)
            if res_target[0] != res_source[0]:
                type_ddm.append('INSERT')
            if res_target[1] != res_source[1]:
                type_ddm.append('UPDATE')
            if res_target[2] != res_source[2]:
                type_ddm.append('DELETE')
            change_parameters[i] = type_ddm
        return change_parameters

    def write_insert(self):
        pass

    def distributor_dmm(self):
        dict_obj = self.operator_dml()
        for key, val in dict_obj.items():
            if 'INSERT' in val:
                pass

    def compare_dataframes(self, which=None):
        stmt = 'SELECT * FROM objects.fr_group'
        df_target = self.target_db.get_dataframe(stmt)
        df_source = self.source_db.get_dataframe(stmt)
        compression_df = df_source.merge(df_target, indicator=True, how="outer")
        if which is None:
            diff_df = compression_df[compression_df['_merge'] != 'both']
        else:
            diff_df = compression_df[compression_df['_merge'] == which]
        print(diff_df)


#        compare_df = df_target.compare(df_source[:len(df_target)], align_axis='rows')
#        print(df_source[:len(df_target)])


if __name__ == '__main__':
    source__db = ConnectDB('source')
    target_db = ConnectDB('target')
    test_analyze = AnalyzeDB(source__db, target_db)
    test_analyze.compare_dataframes()
#    test = test_analyze.operator_ddm()
#    print(test.items())
#   res = test_analyze.get_diff_table()
#  dict_res = test_analyze.choose_operation(res)
#  print(dict_res['INSERT'])
