import psycopg2
from psycopg2 import sql
import csv

conn1 = psycopg2.connect(host='127.0.0.1', port='5432', user='postgres', password='root', database='rd222')
conn2 = psycopg2.connect(host='127.0.0.1', port='5432', user='postgres', password='root', database='rd222_target')

cursor1 = conn1.cursor()
cursor2 = conn2.cursor()


def analyze_table(cur_one, cur_two):
    cur_one.execute("SELECT schemaname, relname, n_tup_ins,n_tup_upd, n_tup_del FROM pg_stat_user_tables")
    source = cur_one.fetchall()
    cur_two.execute("SELECT schemaname, relname, n_tup_ins,n_tup_upd, n_tup_del FROM pg_stat_user_tables")
    target = cur_two.fetchall()
    diff_set = list(set(target) - set(source))
    diff_objects = [n[0:2] for n in diff_set]
    return diff_objects


def diff_data(list_objects):
    """
    compare two database
    """
    dict_object = {}
    for i in list_objects:
        stmt = sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(i[0]), sql.Identifier(i[1]))
        cursor1.execute(stmt)
        result_srs = cursor1.fetchall()
        cursor2.execute(stmt)
        result_tar = cursor2.fetchall()
        result = list(set(result_tar) - set(result_srs))
        name_column = [val[0] for val in cursor1.description]
        type_column = [val[1] for val in cursor1.description]
        param_col = zip(name_column, type_column)
        dict_column = dict(param_col)
        dict_object[i] = dict_column  # dictionary - {(schema,name):{name_column:type_column}
        with open(str(i[0] + '_' + i[1] + '.csv'), 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, quotechar='"', quoting=csv.QUOTE_ALL)
            csv_writer.writerow(name_column)
            csv_writer.writerows(result)
    return dict_object


def convert_liquibase_type(db_type):
    copy_dbtype = db_type.copy()
    dict_liq_type = {'NUMERIC': [1700, 20, 21, 23], 'STRING': [19509156, 25, 1043], 'DATA': [1114], 'BLOB': [17],
                     'BOOLEAN': [16]}
    for key_db in copy_dbtype.keys():
        for key in dict_liq_type.keys():
            if copy_dbtype[key_db] in dict_liq_type[key]:
                copy_dbtype[key_db] = key
                break
    return copy_dbtype


list_ob = analyze_table(cursor1, cursor2)
inform_objects = diff_data(list_ob)
convert_inf_obj = {}
for j in inform_objects.keys():
    dict_col = convert_liquibase_type(inform_objects[j])
    convert_inf_obj[j] = dict_col
print(convert_inf_obj)


# print(columns)

def generate_yaml_file():
    pass
