import re
import numpy

import psycopg2
import psycopg2.sql
# import psycopg2.extensions

from parse import file_selector, file_version, read_config, data_prep


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


def db_create_table(conn, table):
    cur = conn.cursor()
    if db_exists_table(conn, table):
        print('\nTable [%s] already exists.' % table)
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
    # Abort if can't find [table]
    if not db_exists_table(conn, table):
        print('Table [%s] doesn\'t exist. Creating of columns aborted.' % table)
        return

    # First column - Timestamps
    print('\nStarting to add primary column [t]...', end="")
    cur = conn.cursor()
    sql_add_timestamps = psycopg2.sql.SQL("""
        ALTER TABLE {} 
        ADD COLUMN IF NOT EXISTS "t" NUMERIC(8,3);""") \
        .format(psycopg2.sql.Identifier(table))  # 86400.000 seconds is 24-hour long measurement
    try:
        cur.execute(sql_add_timestamps)
        cur.close()
        conn.commit()
        print('Done')
    except Exception as e:
        print('\nError occurred when adding primary column [t].\n%s' % e)

    # Rest of data columns
    print('\nStarting to add data columns...')
    for i, type_name in enumerate(types):
        cur = conn.cursor()
        sql_add_column = psycopg2.sql.SQL("""
            ALTER TABLE {}
            ADD COLUMN IF NOT EXISTS {} {} ;""") \
            .format(psycopg2.sql.Identifier(table),
                    psycopg2.sql.Identifier(columns[i]),
                    psycopg2.sql.Identifier(type_name))
        try:
            cur.execute(sql_add_column)
            cur.close()
            conn.commit()
            print('Data column [%s] added or already exists.' % columns[i])
        except Exception as e:
            print('Error occurred when adding data column [%s].\n%s' % (columns[i], e))
    return


def db_save_data(conn, table, columns, timestamps, data):

    # Construct timestamp column
    print('\nStarting to build timestamps...', end="")
    cur = conn.cursor()
    sql_save_timestamps = psycopg2.sql.SQL("""
        INSERT INTO {} ("t")
        SELECT unnest( %s ) ;""") \
        .format(psycopg2.sql.Identifier(table))
    try:
        cur.execute(sql_save_timestamps, (numpy.ndarray.tolist(timestamps),))
        cur.close()
        conn.commit()
        print('Done')
    except Exception as e:
        print('\nError occurred when building timestamps.\n%s' % e)

    # TODO: function to pass signals as columns to the table
    print('\nStarting to save signal values...', end="")
    for i, sig in enumerate(data):
        cur = conn.cursor()
        col_name = columns[i]
        sql_save_data = psycopg2.sql.SQL("""
            INSERT INTO {} ({}) 
            SELECT unnest( %s ) ;""") \
            .format(psycopg2.sql.Identifier(table),
                    psycopg2.sql.Identifier(col_name))
        cur.execute(sql_save_data, (numpy.ndarray.tolist(sig.samples),))
        cur.close()
        conn.commit()
    print('Done')
    return


def db_data_prep(timestamps, signals):
    # TODO: unfinished, don't run
    data_merge = numpy.empty((0, numpy.size(timestamps)), float)
    data_merge = numpy.append(data_merge, [timestamps], axis=0)
    for sig in signals:
        data_merge = numpy.append(data_merge, [sig.samples], axis=0)
    data_transpose = numpy.transpose(data_merge)
    print(data_transpose)
    return data_transpose


def main():
    print('\nData importing process started...')
    filename = file_selector()
    tablename = re.search(r"\/*.*\/(.*)\.*\.", filename).group(1)
    print('Input MDF file version: %s' % file_version(filename))

    print('\nLoading environment configurations...')
    cfg_env = read_config('config_env.json')

    print('\nLoading signal configurations...')
    cfg_signals = read_config('config_'+tablename+'.json')

    print('\nReading MDF file...', end="")
    signals, types, norm, raw, t = data_prep(filename, cfg_signals, freq=0.01)
    print('Done')

    #db_data_prep(t, signals)

    conn = db_connection(cfg_env)
    db_create_table(conn, tablename)
    db_create_columns(conn, tablename, norm, types)
    db_save_data(conn, tablename, norm, t, signals)
    db_disconnect(conn)

    return


if __name__ == "__main__":
    main()
