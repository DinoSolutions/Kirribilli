import re
import time

import psycopg2
import psycopg2.sql
import psycopg2.extras

from backend_file_functions import file_selector, file_version, read_config, read_mdf_data


def db_connection(db_cfg):
    conn = psycopg2.connect(
        host=db_cfg['DB_ADDR'],
        port=db_cfg['DB_PORT'],
        dbname=db_cfg['DB_NAME'],
        user=db_cfg['DB_USER'],
        password=db_cfg['DB_PSWD']
    )
    return conn


def db_disconnect(conn):
    try:
        conn.close()
    except Exception as e:
        print('Error occurred when closing connection to database.\n%s' % e)
        return
    return


def db_create_table(conn, table, safe=None):
    cur = conn.cursor()
    if db_exists_table(conn, table):
        print('\nTable [%s] already exists.' % table)
        if not safe:
            print('Existing table will be dropped.')
            db_drop_table(conn, table)
    print('\nStarting to create table [%s]...' % table, end="")
    sql_cre_table = psycopg2.sql.SQL("CREATE TABLE {}();") \
        .format(psycopg2.sql.Identifier(table))
    try:
        cur.execute(sql_cre_table)
        cur.close()
        conn.commit()
        print('Done')
    except Exception as e:
        print('\nError occurred when creating table [%s].\n%s' % (table, e))
    return


def db_drop_table(conn, table):
    cur = conn.cursor()
    print('\nStarting to drop table [%s]...' % table, end="")
    sql_drop_table = psycopg2.sql.SQL("DROP TABLE IF EXISTS {} CASCADE;") \
        .format(psycopg2.sql.Identifier(table))
    try:
        cur.execute(sql_drop_table)
        cur.close()
        conn.commit()
        print('Done')
    except Exception as e:
        print('\nError occurred when dropping table [%s].\n%s' % (table, e))
    return


def db_exists_table(conn, table):
    cur = conn.cursor()
    check_table = psycopg2.sql.SQL("SELECT to_regclass('public.{}');") \
        .format(psycopg2.sql.Identifier(table))
    cur.execute(check_table)
    existence = bool(cur.fetchone()[0])  # True when table exists; False when table doesn't exist
    cur.close()
    return existence


def db_exists_column(conn, table, col):
    cur = conn.cursor()
    sql_check_col = psycopg2.sql.SQL("""
        SELECT EXISTS (SELECT 1 
        FROM information_schema.columns
        WHERE table_name = {} and column_name = {}); """) \
        .format(psycopg2.sql.Literal(table),
                psycopg2.sql.Literal(col))
    cur.execute(sql_check_col)
    existence = cur.fetchone()[0]  # True when column exists; False when column doesn't exist
    cur.close()
    return existence


def db_create_columns(conn, table, columns, types):
    print('\nStarting to add data columns...', end="")
    cur = conn.cursor()
    # Hard code first column for timestamps
    sql_add_column = psycopg2.sql.SQL("""ALTER TABLE {} ADD COLUMN IF NOT EXISTS "TS" NUMERIC(8, 3) PRIMARY KEY;""") \
        .format(psycopg2.sql.Identifier(table))
    cur.execute(sql_add_column)
    # print('Timestamps column [TS] added.')
    # Rest of data columns
    for i in range(1, len(columns)):
        sql_add_column = psycopg2.sql.SQL("""ALTER TABLE {} ADD COLUMN {} {} ;""") \
            .format(psycopg2.sql.Identifier(table),
                    psycopg2.sql.Identifier(columns[i]),
                    psycopg2.sql.Identifier(types[i]))
        try:
            cur.execute(sql_add_column)
            # print('Data column [%s] added.' % columns[i])
        except Exception as e:
            print('Error occurred when adding data column [%s].\n%s' % (columns[i], e))
    cur.close()
    conn.commit()
    print('Done')
    return


def db_save_data(conn, table, columns, data):
    print('\nStarting to save signal data...', end="")
    cur = conn.cursor()
    sql_save_data = psycopg2.sql.SQL("INSERT INTO {} VALUES %s ;") \
        .format(psycopg2.sql.Identifier(table))
    sql_template = '(' + ', '.join(['%s'] * len(columns)) + ')'
    psycopg2.extras.execute_values(cur, sql_save_data, data, sql_template)
    cur.close()
    conn.commit()
    print('Done')
    return


def db_save_data_old(conn, table, columns, data):
    # This function is depreciated due to low performance
    print('\nStarting to save signal data...', end="")
    cur = conn.cursor()
    for i in range(len(data)):
        sql_save_data = psycopg2.sql.SQL("INSERT INTO {} VALUES ({}) ;") \
            .format(psycopg2.sql.Identifier(table),
                    psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(columns)))
        cur.execute(sql_save_data, data[i])
    cur.close()
    conn.commit()
    print('Done')
    return


def db_process_file(pathname, table=None, use_cfg=None):
    t_start = time.time()
    print('\nData importing process started...')
    path = re.search(r"\/*.*\/", pathname).group(0)

    if table:
        table_name = table
    else:
        table_name = re.search(r"\/*.*\/(.*)\.*\.", pathname).group(1)

    config_name = 'config_' + table_name + '.json'
    config_path = path + config_name

    print('Input MDF file name: %s' % pathname)
    print('Input MDF file version: %s' % file_version(pathname))

    print('\nLoading environment configurations...', end="")
    cfg_env = read_config('config_env.json')
    print('Done')

    if use_cfg == 1:
        print('\nLoading signal configurations...', end="")
        cfg_signals = read_config(config_path)
        print('Done')
    else:
        cfg_signals = None

    data_block_titles, data_block, sql_data_type = read_mdf_data(pathname, cfg_signals)

    try:
        conn = db_connection(cfg_env)
        db_create_table(conn, table_name)
        db_create_columns(conn, table_name, data_block_titles, sql_data_type)
        db_save_data(conn, table_name, data_block_titles, data_block)
        db_disconnect(conn)
        print('\nData importing process finished in %.3f seconds.' % (time.time() - t_start))
    except Exception as e:
        print('\nException occurred during data importing process.\n%s' % e)
    return


def main():
    pathnames = file_selector()
    if len(pathnames) == 0:
        print('No input files found.')
        return
    for pathname in pathnames:
        db_process_file(pathname, use_cfg=0)
    return


if __name__ == "__main__":
    main()
