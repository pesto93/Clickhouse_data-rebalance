import halo
import configargparse
from pssh.clients import ParallelSSHClient
from config import (
    user,
    hosts,
    port,
    proxy_hosts,
    proxy_port,
    db,
    newline,
    version_number,
    cluster,
)
from utils import (
    ch_connection,
    _close_con,
    _log_set,
)

# Globals
args = None
create_with_table = {}
new_table_names = {}


def _drop_new_table(database: str):
    # -------------------------------------------------------------------
    # FOR DROPPING THINGS
    global new_table_names
    con, cur = ch_connection()
    for key, val in new_table_names.items():
        print("""
                drop table if exists {database}.{val} on cluster '{cluster}'
            """.format(database=database, val=val, cluster='{cluster}')
              )
        cur.execute("""
                drop table if exists {database}.{val} on cluster '{cluster}'
                """.format(database=database, val=val, cluster='{cluster}')
                    )
    _close_con(con_cursor=cur, con_connection=con)


def _create_new_table_dic(old_tbl: str):
    global new_table_names
    new_table_names[old_tbl] = old_tbl + version_number


def _add_on_cluster(tbl: str, create_tbl: str):
    if 'mv' in tbl:
        try:
            to_position = create_tbl.index('TO')
            return create_tbl[:to_position] + "ON CLUSTER '{cluster}' " + create_tbl[to_position:]
        except ValueError:
            _log_set.warning(f"substring ['TO'] keyword not found - Please check create table : {newline}{newline}{create_tbl}{newline} ")
            return create_tbl
    else:
        first_bracket_position = create_tbl.index('(')
        return create_tbl[:first_bracket_position] + "ON CLUSTER '{cluster}' " + create_tbl[first_bracket_position:]


def _recreate_old_create_table(tbl: list, create_tbl: list, db_name: str):
    global create_with_table
    for index in range(len(tbl)):
        # FUNCTIONS
        new_create = _add_on_cluster(tbl=tbl[index], create_tbl=create_tbl[index])
        # CONDITIONS
        if '.inner.' in tbl[index]:
            continue
        elif 'local' in tbl[index]:
            create_with_table[tbl[index]] = new_create.replace(
                'CREATE TABLE',
                'CREATE TABLE IF NOT EXISTS'
            ).replace(
                'CREATE MATERIALIZED VIEW',
                'CREATE MATERIALIZED VIEW IF NOT EXISTS'
            ).replace(
                tbl[index],
                tbl[index] + version_number
            )
        elif 'mv' in tbl[index]:
            local_to = tbl[index].replace('_mv', '_local')
            create_with_table[tbl[index]] = new_create.replace(
                'CREATE MATERIALIZED VIEW',
                'CREATE MATERIALIZED VIEW IF NOT EXISTS'
            ).replace(
                tbl[index],
                tbl[index] + version_number
            ).replace(
                local_to,
                local_to + version_number
            )
        else:
            create_with_table[tbl[index]] = new_create.replace(
                'CREATE TABLE',
                'CREATE TABLE IF NOT EXISTS'
            ).replace(
                db_name + '.' + tbl[index],
                db_name + '.' + tbl[index] + version_number
            ).replace(
                tbl[index] + '_local',
                tbl[index] + '_local_old'
            )
        # Make the what the new table will look like and push to global var new_table_names
        _create_new_table_dic(old_tbl=tbl[index])
    # pprint(create_with_table)
    # pprint(new_table_names)


def exec_create_tbl(which_create: int):
    conn, curs = ch_connection()
    if which_create:
        for key, value in create_with_table.items():
            if 'local' in key:
                if 'SELECT' not in value:
                    """
                        Skip creating materialized view table automatically. All MV should be recreated manually
                    """
                    # print("\n Creating local tables", value.replace('\\', ''), '\n')
                    curs.execute(value.replace('\\', ''))
    else:
        for key, value in create_with_table.items():
            if 'local' not in key:
                if 'mv' not in key:
                    """
                        Skip creating materialized view table automatically. All MV should be recreated manually
                    """
                    # print("\n Creating dist tables", value.replace('\\', ''), '\n')
                    curs.execute(value.replace('\\', ''))
    _close_con(con_connection=conn, con_cursor=curs)


@halo.Halo(text='Recreating Create tables from Original tables. Adding {} as Version number to new tables : --------  '.format(version_number),
           spinner='dots')
def update_create_table(tbl: list, create_tbl: list, db_name: str):
    _recreate_old_create_table(tbl=tbl, create_tbl=create_tbl, db_name=db_name)


@halo.Halo(text='Creating new local tables for data re-balance ----------   ', spinner='dots')
def start_local_create_process():
    exec_create_tbl(which_create=1)
    _log_set.info("---------------> CREATE LOCAL TABLE DONE <-----------------------")


@halo.Halo(text='Inserting into new table(s) from dist table(s) ------------  :  ', spinner='dots')
def insert_new_tbl(database: str):
    conn, curs = ch_connection()
    for new_table, old_table_dist in new_table_names.items():
        if 'local' not in new_table:
            """
                Avoid collecting info from local tables, only use distributed tables to collect all info and re-balance into new tables using same
                distributed table too for the new tables. But to Work with distributed tables, I kinda need to make sure the old tables exists on the
                recently added shards -> else I will get an error distributed table does not exist.
            """
            if 'mv' not in new_table:
                """
                    Please skip inserting data into MATERIALIZED VIEW AS WELL
                """
                print(f"INSERT INTO {database}.{new_table} SELECT * FROM {database}.{old_table_dist}")
                curs.execute(f"INSERT INTO {database}.{new_table} SELECT * FROM {database}.{old_table_dist}")
    _close_con(con_connection=conn, con_cursor=curs)


@halo.Halo(text='Creating OLD tables on NEW shards -------- :   ', spinner='dots')
def modify_old_tbl_for_reuse(tbl: list, create_tbl: list):
    conn, curs = ch_connection()
    for index in range(len(tbl)):
        # FUNCTIONS
        old_create = _add_on_cluster(tbl=tbl[index], create_tbl=create_tbl[index])
        # CONDITIONS
        if '.inner.' in tbl[index]:
            continue
        elif 'local' in tbl[index]:
            curs.execute(old_create.replace(
                'CREATE TABLE',
                'CREATE TABLE IF NOT EXISTS'
            ).replace(
                'CREATE MATERIALIZED VIEW',
                'CREATE MATERIALIZED VIEW IF NOT EXISTS'
            ).replace('\\', ''))
        elif 'mv' in tbl[index] and 'MATERIALIZED' in create_tbl[index]:
            curs.execute(old_create.replace(
                'CREATE MATERIALIZED VIEW',
                'CREATE MATERIALIZED VIEW IF NOT EXISTS'
            ).replace('\\', ''))
        else:
            curs.execute(old_create.replace(
                'CREATE TABLE',
                'CREATE TABLE IF NOT EXISTS'
            ).replace('\\', ''))
    _close_con(con_connection=conn, con_cursor=curs)


@halo.Halo(text='Dropping old local and dist table(s)  -------- :   ', spinner='dots')
def _drop_old_tables(database: str):
    conn, curs = ch_connection()
    for key, value in new_table_names.items():
        if any(char.isdigit() for char in value):
            # _log_set.info(
            #     f"""
            #     DROP TABLE IF EXISTS {database}.{value.replace(f'local{version_number}', '_local_old')} ON CLUSTER '{cluster}'
            #     """
            # )
            curs.execute(
                """
                DROP TABLE IF EXISTS {database}.{value} ON CLUSTER '{cluster}'
                """.format(database=database, value=value.replace(f'local{version_number}', 'local_old'), cluster='{cluster}')
            )
    _close_con(con_cursor=curs, con_connection=conn)


@halo.Halo(text=f'Renaming New local table with version {version_number} to Old table  -------- :   ', spinner='dots')
def _rename_new_to_old_tbl(database: str):
    conn, curs = ch_connection()
    for key, value in new_table_names.items():
        curs.execute(f"EXISTS {database}.{value}")
        is_exist = [desc[0] for desc in curs.fetchall()][0]
        if is_exist:
            # print("""
            #     RENAME TABLE {database}.{value} TO {database}.{key} ON CLUSTER '{cluster}'
            #     """.format(database=database, value=value, key=key, cluster='{cluster}')
            #       )
            curs.execute(
                """
                RENAME TABLE {database}.{value} TO {database}.{key} ON CLUSTER '{cluster}'
                """.format(database=database, value=value, key=key, cluster='{cluster}')
            )
    _close_con(con_cursor=curs, con_connection=conn)


@halo.Halo(text=f'Renaming Old local table to make new local table the chief  -------- :   ', spinner='dots')
def _rename_old_local_tbl(database: str):
    conn, curs = ch_connection()
    for key, value in new_table_names.items():
        if 'local' in key:
            curs.execute(f"EXISTS {database}.{key}")
            is_exist = [desc[0] for desc in curs.fetchall()][0]
            if is_exist:
                # print("""
                #         RENAME TABLE {database}.{key} TO {database}.{key}_old ON CLUSTER '{cluster}'
                #         """.format(database=database, key=key, cluster='{cluster}')
                #       )
                curs.execute(
                    """
                    RENAME TABLE {database}.{key} TO {database}.{key}_old ON CLUSTER '{cluster}'
                    """.format(database=database, key=key, cluster='{cluster}')
                )
    _close_con(con_cursor=curs, con_connection=conn)
    pass


@halo.Halo(text=f'Creating New dist table to point to renamed Old local table  -------- :   ', spinner='dots')
def start_old_dist_create_process():
    exec_create_tbl(which_create=0)
    _log_set.info("---------------> CREATE NEW DIST TO OLD TABLE DONE <-----------------------")


def _testing_recreating_mvs():
    # todo : try to recreate mv automatically
    # for key, value in create_with_table.items():
    #     if 'MATERIALIZED' in value:
    #         print(new_table_names.get(key))
    #         b = f"INSERT INTO {args.target_database}.{new_table_names.get(key)} {value[value.index('SELECT'):]}"
    #
    #         pp(f"{key} : {b}")
    pass


if __name__ == "__main__":
    p = configargparse.ArgParser(default_config_files=[])
    p.add_argument('--clickhouse-seed', default=hosts)
    p.add_argument('--target-database', default=db)
    args = p.parse_args()

    with halo.Halo(text='Connecting to {}'.format(args.clickhouse_seed), spinner='dots'):
        arg_hosts = [args.clickhouse_seed]
        client = ParallelSSHClient(
            arg_hosts,
            user=user,
            pkey='~/.ssh/id_rsa',
            port=port,
            timeout=20,
            proxy_host=proxy_hosts,
            proxy_pkey='~/.ssh/id_rsa',
            proxy_port=proxy_port,
            proxy_user=user,
            tunnel_timeout=30
        )
        create_table_cmd = client.run_command(
            """ 
                clickhouse-client -q "select create_table_query from system.tables where database= '{db}'"
            """.format(db=args.target_database)
        )
        table_cmd = client.run_command(
            """ 
                clickhouse-client -q "select name from system.tables where database= '{db}'"
            """.format(db=args.target_database)
        )
        print('\n')
        _log_set.info(f""" Client Create table cmd --->  {create_table_cmd}""")
        _log_set.info(f""" Client Table names cmd --->  {table_cmd}""")

        tbl_list = [x for x in table_cmd[0].stdout]
        create_tbl_list = [x for x in create_table_cmd[0].stdout]

    # # -------------------------------------------------------------------
    # Add all old table to new shard
    modify_old_tbl_for_reuse(
        tbl=tbl_list,
        create_tbl=create_tbl_list,
        )
    # # -------------------------------------------------------------------
    # UPDATE THE CREATE TABLE FROM ORIGINAL TABLES
    update_create_table(
        tbl=tbl_list,
        create_tbl=create_tbl_list,
        db_name=args.target_database
    )
    # # -------------------------------------------------------------------
    # # Start Create table process
    start_local_create_process()
    # # -------------------------------------------------------------------
    # # Start rename old local tables process
    _rename_old_local_tbl(database=args.target_database)
    # # -------------------------------------------------------------------
    # # Start rename new table to the old (default) table names.
    _rename_new_to_old_tbl(database=args.target_database)
    # # -------------------------------------------------------------------
    # # Start Create new dist table for old table
    start_old_dist_create_process()
    # # -------------------------------------------------------------------
    # # # Start Insert process from the new distributed table pointing to the old local tables data.
    insert_new_tbl(database=args.target_database)
    # # # # -------------------------------------------------------------------
    _drop_old_tables(database=args.target_database)
    # # # # -------------------------------------------------------------------
    # todo : DEAL WITH AUTOMATED TABLE MATERIALIZED VIEW CREATION
    # # # -------------------------------------------------------------------

    # A VERY DESTRUCTIVE QUERY, RUN WHEN YOU ARE SURE OF IT
    # # FOR DROPPING THINGS
    # _drop_new_table(database=args.target_database)
