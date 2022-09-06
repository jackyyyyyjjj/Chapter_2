import sqlite3 as sqlite
import json

def make_column_args(json):
    '''takes a json key as a row, and returns a string of the
    column names and datatypes (assumes all text) to use in creating
    a new table'''
    header = json.keys()
    header = ['"' + i + '"' for i in header] + ['"date"']  # quotes allow for columns starting with numbers
    columns = ','.join(header)
    columns = '(' + columns + ' )'
    return columns


def post_row(conn, tablename, rec, columns, date):
    """
    SET THE ID AS THE PRIMARY KET, it can automatic deleate the duplicated data.
    """
    order = '(id CHAR(200) PRIMARY KEY     NOT NULL,date DATE NOT NULL)'
    conn.execute('''CREATE TABLE IF NOT EXISTS %s %s''' % (tablename, order))
    conn.execute("select * from (%s)" % tablename)
    col_name_list = [tuple[0] for tuple in conn.description]
    col_name_list_new = [tuple for tuple in rec.keys()]

    cross_data = set(col_name_list_new).difference(col_name_list)  # col_name_list中有而col_name_list中没有的非常高效！
    if cross_data:
        for i in cross_data:
            conn.execute("ALTER TABLE %s ADD %s" % (tablename, i))
    # keys = ','.join(rec.keys())
    question_marks = ','.join(list(['?'] * (len(rec) + 1)))
    lite = []
    for key, value in rec.items():  # transfer the all the values into string type
        lite.append(str(value))
    lite.append(date)
    values = tuple(lite)
    conn.execute('INSERT OR IGNORE INTO ' + tablename + columns + ' VALUES (' + question_marks + ')', values)
    cur.commit()


# Request Parameters
store = "android"  # Could be either "android" or "itunes".
country_code = "US"  # Two letter country code.
cur = sqlite.connect('appmonsta_app2016.sqlite')  # you can change the databse name here
conn = cur.cursor()

count = 0
# date_list = ['2015-08-17',"2016-08-15",'2017-08-14','2018-08-13','2019-08-12','2020-08-17','2021-08-16']
# name_list = ['AppMonstaPlayStoreDetails20150817','AppMonstaPlayStoreDetails20160815',
#              'AppMonstaPlayStoreDetails20170814',
#              'AppMonstaPlayStoreDetails20180813','AppMonstaPlayStoreDetails20190812',
#              'AppMonstaPlayStoreDetails20200817','AppMonstaPlayStoreDetails20210816']
with open('AppMonstaPlayStoreDetails20160815.json', encoding='utf-8') as f:
    count += 1
    while True:
        line = f.readline()
        count += 1
        if not line: # 到 EOF，返回空字符串，则终止循环
            break
    # Load json object and print it out
        json_record = json.loads(line)
        columns = make_column_args(json_record)
        tablename = 'AppMonstaPlayStoreDetails20160815'
        date = "2016-08-15"
        post_row(conn, tablename, json_record, columns, date)
    # count how many apps have been stored into the database
        if count % 1000 == 0:
            print('Inserting row ' + str(count) + ' into ' + tablename)

