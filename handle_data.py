import psycopg2
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


def db_create_table(conn, filename):
    # TODO: function to create table after checking existance of the table
    cur = conn.cursor()
    return


def db_save_signals(conn, filename, signals_output, signals_normal, signals_raw):
    # TODO: function to pass signals as columns to the table
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE genesis(
        id integer PRIMARY KEY,
        email text,
        name text,
        address text)
    """)
    conn.commit()
    conn.close()

    try:
        # connection operations
        print("Success!")
    finally:
        conn.close()


def main():
    filename = file_selector()
    file_version(filename)
    cfg_signals = read_config('config_signals.json')
    cfg_env = read_config('config_env.json')
    signals, norm, raw = data_prep(filename, cfg_signals, freq=0.01)
    conn = db_connection(cfg_env)
    db_create_table(conn, filename)
    db_save_signals(conn, filename, signals, norm, raw)
    db_disconnect(conn)
    return


if __name__ == "__main__":
    main()
