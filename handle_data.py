import psycopg2
import psycopg2.sql
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
    except:
        return
    return


def sql_test(conn):
    cur = conn.cursor()
    table = 'genesis1'
    check_table = psycopg2.sql.SQL("SELECT to_regclass('public.{}');")\
        .format(psycopg2.sql.Identifier(table))
    print(check_table.as_string(conn))
    cur.execute(check_table)
    print(bool(cur.fetchone()[0]))
    return


def db_create_table(conn, table):
    cur = conn.cursor()
    if not db_val_table_exists(conn, table):
        cre_table = psycopg2.sql.SQL("CREATE TABLE {}();")\
            .format(psycopg2.sql.Identifier(table))
        cur.execute(cre_table)
        conn.commit()
        print('\nTable [%s] created.' % table)
    else:
        print('\nTable [%s] already exists.' % table)
    return


def db_val_table_exists(conn, table):
    cur = conn.cursor()
    check_table = psycopg2.sql.SQL("SELECT to_regclass('public.{}');") \
        .format(psycopg2.sql.Identifier(table))
    cur.execute(check_table)
    return bool(cur.fetchone()[0])  # True when table exists; False when table doesn't exist


def db_create_columns(conn, table, signals, norm):
    # TODO: determine the type of each signal

    cur = conn.cursor()
    if db_val_table_exists(conn, table):
        add_column = psycopg2.sql.SQL("""
            ALTER TABLE {}
            ADD {} {};
            """)
    # TODO: function to pass signals as columns to the table

    return


def main():
    filename = file_selector()
    file_version(filename)
    cfg_signals = read_config('config_signals.json')
    cfg_env = read_config('config_env.json')
    # signals, norm, raw = data_prep(filename, cfg_signals, freq=0.01)
    conn = db_connection(cfg_env)
    # SQL Test function
    # sql_test(conn)
    db_create_table(conn, filename)
    db_create_columns(conn, filename, signals, norm, raw)
    db_disconnect(conn)

    return


if __name__ == "__main__":
    main()
