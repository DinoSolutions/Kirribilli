import psycopg2
import psycopg2.sql
import psycopg2.extensions
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
    print('\nStarting to create table [%s]...' % table)
    if not db_exists_table(conn, table):
        cre_table = psycopg2.sql.SQL("CREATE TABLE {}();")\
            .format(psycopg2.sql.Identifier(table))
        cur.execute(cre_table)
        conn.commit()
        print('Table [%s] created.' % table)
    else:
        print('Table [%s] already exists.' % table)
    return


def db_exists_table(conn, table):
    cur = conn.cursor()
    check_table = psycopg2.sql.SQL("SELECT to_regclass('public.{}');") \
        .format(psycopg2.sql.Identifier(table))
    cur.execute(check_table)
    return bool(cur.fetchone()[0])  # True when table exists; False when table doesn't exist


def db_exists_column(conn, table, col):
    cur = conn.cursor()
    check_col = psycopg2.sql.SQL("""
        SELECT EXISTS (SELECT 1 
        FROM information_schema.columns
        WHERE table_name = {} and column_name = {}); 
    """).format(psycopg2.sql.Literal(table),
                psycopg2.sql.Literal(col))
    # print(check_col.as_string(conn))
    cur.execute(check_col)
    existence = cur.fetchone()[0]  # True when column exists; False when column doesn't exist
    # print(existence)
    return existence


def db_create_columns(conn, table, signals, dtype, norm, timestamps):
    cur = conn.cursor()
    # Abort if can't find [table]
    if not db_exists_table(conn, table):
        print('Table [%s] doesn\'t exist. Creating of columns aborted.' % table)
        return

    # First column - Timestamps
    print('\nStarting to add primary column [Time]...')
    add_timestamps = psycopg2.sql.SQL("""
        ALTER TABLE {}
        ADD COLUMN IF NOT EXISTS "Time" NUMERIC(8,3) NOT NULL PRIMARY KEY;
    """).format(psycopg2.sql.Identifier(table))  # 86400.000 seconds is 24-hour long measurement
    # print(add_timestamps.as_string(conn))
    try:
        cur.execute(add_timestamps)
        print('Primary column [Time] added.')
    except:
        print('Error occurred when adding primary column [Time]. Skipped.')
    conn.commit()

    # TODO: Other columns for data
    # Rest of data columns
    print('\nStarting to add data columns...')
    for i, typ_name in enumerate(dtype):
        # if typ_name == 'int':
        #     add_column = psycopg2.sql.SQL("""
        #         ALTER TABLE {}
        #         ADD COLUMN IF NOT EXISTS {} int ;
        #     """).format(
        #         psycopg2.sql.Identifier(table),
        #         psycopg2.sql.Identifier(norm[i]),
        #     )
        # elif typ_name == 'double':
        #     add_column = psycopg2.sql.SQL("""
        #         ALTER TABLE {}
        #         ADD COLUMN IF NOT EXISTS {} double precision ;
        #                 """).format(
        #         psycopg2.sql.Identifier(table),
        #         psycopg2.sql.Identifier(norm[i]),
        #     )
        # else:
        #     add_column = psycopg2.sql.SQL("""
        #         ALTER TABLE {}
        #         ADD COLUMN IF NOT EXISTS {} text ;
        #                 """).format(
        #         psycopg2.sql.Identifier(table),
        #         psycopg2.sql.Identifier(norm[i]),
        #     )
        add_column = psycopg2.sql.SQL("""
            ALTER TABLE {}
            ADD COLUMN IF NOT EXISTS {} {} ;
        """).format(
            psycopg2.sql.Identifier(table),
            psycopg2.sql.Identifier(norm[i]),
            psycopg2.sql.Identifier(typ_name)
        )
        # print(cur.mogrify(add_column))
        try:
            cur.execute(add_column)
            print('Data column [%s] added.' % norm[i])
        except:
            print('Error occurred when adding data column [%s]. Skipped.' % norm[i])
    conn.commit()

    # TODO: function to pass signals as columns to the table

    return


def main():
    filename = file_selector()
    file_version(filename)
    cfg_signals = read_config('config_signals.json')
    cfg_env = read_config('config_env.json')
    signals, dtype, norm, raw, t = data_prep(filename, cfg_signals, freq=0.01)

    conn = db_connection(cfg_env)
    # sql_test(conn)
    db_create_table(conn, filename)
    # db_exists_column(conn, 'data/Acura_M52_NB_Comfort.MF4', 'Time')
    db_create_columns(conn, filename, signals, dtype, norm, t)
    # db_disconnect(conn)

    return


if __name__ == "__main__":
    main()
