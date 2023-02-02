import os
import psycopg2
import psycopg2.extras as extras
import re
import shutil
import sys
import warnings
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

warnings.filterwarnings('ignore')
env_path = Path('.', '.env')
load_dotenv(dotenv_path=env_path)

def create_tables(cursor, conn):
    ''' функция для создания таблиц в dwh'''
    try:
        cursor.execute(open('/home/de11an/mrnv/Project/sql_scripts/main_ddl.sql', 'r').read())
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    print('Tables created')


def connect_to_db(db, host, user, password, port):
  '''Создание подключения к PostgreSQL базе'''
  try:
      # connect to the PostgreSQL server
      conn = psycopg2.connect(database = db,
                      host =     host,
                      user =     user,
                      password = password,
                      port =     port)
      # Отключение автокоммита
      conn.autocommit = False
      # Создание курсора
      cursor = conn.cursor()
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)
      sys.exit(1)
  print(f'Connection to "{db}" database was successful')
  return conn, cursor


def clear_stg(cursor, conn):
    ''' функция для чистки стейджинга'''
    try:
        cursor.execute(open('/home/de11an/mrnv/Project/sql_scripts/clean_stg.sql', 'r').read())
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    print('Staging cleared')

def find_date_from_meta(table, cursor):
  ''' функция ля получения даты из меты '''
  cursor.execute("""
      select
          max_update_dt
      from de11an.mrnv_meta_bank
      where schema_name='de11an' and table_name= '""" + table + """'""")
  last_date = cursor.fetchone()[0]
  return last_date


def find_file_to_process(name, last_date):
  '''функция для поиска файла для обработки'''
  file_to_process = None
  file_dt = None
  for i in sorted(os.listdir(os.getenv("FILES_PATH_PREFIX"))):
      if not i.startswith(name):
          continue
      words = re.split('[_,.]', i)
      _, file_date = ','.join(words), words[-2]
      file_dt = datetime.strptime(file_date, '%d%m%Y').date()
      if file_dt > last_date:
          file_to_process = i
          break
  return  file_to_process, file_dt


def execute_values(conn, cursor, df, table):
    '''функция для вставки данных в базу данных'''
    # создание списка tupples из значений датафрейма
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL запрос
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    print(f'Data has been inserted to {table}')


def load_data_to_dwh(conn, cursor, list_of_script):
    ''' функция для переноса данных из стейджинга в таргет'''
    try:
        for s in list_of_script:
            cursor.execute(
                open(f"/home/de11an/mrnv/Project/sql_scripts/{s}.sql", "r").read())
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    print('Data has been loaded to DWH successfully')

def create_report(cursor, conn):
    ''' функция для создания репорта'''
    try:
        cursor.execute(open('/home/de11an/mrnv/Project/sql_scripts/report.sql', 'r').read())
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    print('Report created')

def move_file(file_name):
  '''функция для переноса обработанных файлов в архив'''
  try:
    shutil.move(
              f'{os.getenv("FILES_PATH_PREFIX")}/{file_name}',
              f'{os.getenv("PROCESSED_FILES_DIR")}/{file_name}.backup'
          )
  except OSError as err:
    print(err)
  print(f'Processed file {file_name} have been moved to archive')