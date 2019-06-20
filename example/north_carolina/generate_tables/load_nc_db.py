import argparse
import logging
from sys import exit
import pandas as pd
from os import path
import ohio.ext.pandas  # noqa
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError



def get_engine(user, password, host, database, port=5432):
    """

    :param configs:
    :return: Returns a SQLAlchemy pg connnection.
    """
    # try:
    #     db_access = configs.db['db_credentials']
    # except KeyError:
    #     logging.error('KeyError in configuration file.')
    #     exit(1)
    try:
        engine = create_engine('postgresql://{user}:{pw}@{host}:{port}/{db}'.format(
            user=user,
            pw=password,
            host=host,
            port=port,
            db=database))
    except SQLAlchemyError:
        logging.error('PG: could not create pg engine!')
        exit(1)
    return engine


def push_todb(engine, output_schema, output_table, output_df, create_tables='append'):
    """

    :param engine:
    :param output_df:
    :return:
    """
    logging.info(f"pushing to db {output_table} table from '{output_df}'...")
    try:
        output_df.pg_copy_to(
            schema=output_schema,
            name=output_table,
            con=engine,
            if_exists=create_tables)
    except SQLAlchemyError as e:
        logging.error(f"push_db_data: Could not push results to the target database: {e}")


def push_tocsv(file_to_write, df, encoding='utf-8', index=False):
    """
    :param file_to_write:
    :param df:
    """
    logging.info('pushing to csv...')
    # ipath, input_name = path.split(input_file)
    # outpath = ipath + input_name[:input_file.find('.')] + '_nc_table_' + datestr + '.csv'

    try:
        output_df.to_csv(file_to_write, encoding=encoding, index=index)
    except IOError:
        logging.info('push_csv_data: Could not push results to a csv file.')



if __name__ == '__main__':
    class Args():
        pass


    a = Args()
    parser = argparse.ArgumentParser(description="Collect arguments for "
                                                 "ohio upload to database")

    parser.add_argument('-infile', help="Specify input file in '.pkl' format.")

    # indicate one of either bruteforce, generate, or find must be called
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--csv', type=str, help="Specify name of csv to write to.")
    group.add_argument('--db', help="Specify name of database to write to.")

    db_usage = parser.add_argument_group('db_usage', 'Commands for db loading')
    db_usage.add_argument('--user', help="Specify user.")
    db_usage.add_argument('--pw', help="Specify user pw.")
    db_usage.add_argument('--table', help="Specify db table to write to")
    db_usage.add_argument('--host', help="Specify database host.")
    db_usage.add_argument('--schema', help="Specify schema to write to.")
    db_usage.add_argument('--create', help="Specify create table instruction.")

    csv_usage = parser.add_argument_group('csv_usage', 'Commands for csv writing')
    csv_usage.add_argument('--encoding', help="Specify encoding to use for output.")
    csv_usage.add_argument('--index', help="Specify whether to write index to file.")


    try:
        args = parser.parse_args(namespace=a)
    except argparse.ArgumentError or argparse.ArgumentTypeError as exc:
        exit("load-db error: Please review arguments passed: {}".format(
            args, exc.message))
    except Exception as e:
        exit("load-db error: Please review arguments passed: {}".format(e))


    final_data = pd.read_pickle(a.infile)
    final_data.columns = [col.lower() for col in final_data.columns]

    if a.csv:
        if not a.index:
            index = False
        if not a.encoding:
            encoding = 'utf-8'

        try:
            push_tocsv(df=final_data, encoding=encoding, index=index)
        except Exception as e:
            print(f"Error encountered when writing to output file '{a.csv}': {e}")


    elif a.db:
        conn = get_engine(user=a.user, password=a.pw, host=a.host, database=a.db)
        print('Connection created, now attempting db write...')

        if a.schema:
            output_schema = a.schema
        else:
            output_schema = "cleaned"

        if a.create:
            create_tables= a.create
        else:
            create_tables="append"
        try:
            push_todb(engine=conn, output_schema=output_schema, output_table=a.table,
                      create_tables=create_tables, output_df=final_data)
            print(f'Successfully wrote to table {a.table}!')
        except Exception as e:
            print(f"Error encountered when writing to table '{a.table}': {e}")
