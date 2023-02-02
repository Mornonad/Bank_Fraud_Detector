#!/usr/bin/env python3

import os
import pandas as pd
from py_scripts import functions as f



conn_edu, cursor_edu = f.connect_to_db('edu',
                                       os.getenv('HOST'),
                                       os.getenv('USER_EDU'),
                                       os.getenv('PASSWORD_EDU'),
                                       os.getenv('PORT')
                                       )
conn_bank, cursor_bank = f.connect_to_db('bank',
                                         os.getenv('HOST'),
                                         os.getenv('USER_BANK'),
                                         os.getenv('PASSWORD_BANK'),
                                         os.getenv('PORT'))

# найдем актульные файлы для обработки

# terminals

last_terminals_date = f.find_date_from_meta('mrnv_terminals', cursor_edu)
terminals_file, terminals_file_dt = f.find_file_to_process('terminals_', last_terminals_date)

# transactions

last_transactions_date = f.find_date_from_meta('mrnv_trans', cursor_edu)
transactions_file, transactions_file_dt = f.find_file_to_process('transactions_', last_transactions_date)

# passport_blacklists

last_passport_date = f.find_date_from_meta('mrnv_passports', cursor_edu)
passport_blacklists_file, passport_blacklist_file_dt = f.find_file_to_process('passport_blacklist_', last_passport_date)

# Чтение данных из файлов и запись в датафреймы

with open(f'{os.getenv("FILES_PATH_PREFIX")}/{transactions_file}') as k:
    transactions = pd.read_csv(k, sep=';')
    transactions.rename(columns={'transaction_id': 'trans_id', 'transaction_date': 'trans_date', 'amount': 'amt'},
                        inplace=True)

    # Замена типов
    transactions['trans_id'] = transactions['trans_id'].astype('object')
    transactions['amt'] = transactions['amt'].astype(str).str.replace(',', '.').astype('float')
    transactions['update_dt'] = transactions_file_dt.strftime('%Y-%m-%d')
    transactions['trans_date'] = pd.to_datetime(transactions['trans_date'])

with open(f'{os.getenv("FILES_PATH_PREFIX")}/{terminals_file}', mode='rb') as k:
    terminals = pd.read_excel(k)
    terminals['update_dt'] = terminals_file_dt.strftime('%Y-%m-%d')

with open(f'{os.getenv("FILES_PATH_PREFIX")}/{passport_blacklists_file}', mode='rb') as k:
    passports_blacklist = pd.read_excel(k)
    passports_blacklist.rename(columns={'date': 'entry_dt', 'passport': 'passport_num'}, inplace=True)

# Загрузка таблиц из датабазы

sql_clients = f"""SELECT client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        create_dt,
                        to_date('{passport_blacklist_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as update_dt
                  FROM info.clients"""

sql_accounts = f"""SELECT 
                        trim(account) as account_num,
                        valid_to,
                        client,
                        create_dt,
                        to_date('{passport_blacklist_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as update_dt
                  FROM info.accounts"""

sql_cards = f"""SELECT 
                      trim(card_num) as card_num,
	                  trim(account) as account_num,
                      create_dt,
                      to_date('{passport_blacklist_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as update_dt
                  FROM info.cards"""

# Запись данных из базы в датафреймы

clients = pd.read_sql_query(sql_clients, conn_bank)
accounts = pd.read_sql_query(sql_accounts, conn_bank)
cards = pd.read_sql_query(sql_cards, conn_bank)

# Очистка стейджинга

f.clear_stg(cursor_edu, conn_edu)

# # Выполнение загрузки данных в стейджинг

f.execute_values(conn_edu, cursor_edu, clients, 'de11an.mrnv_stg_clients')
f.execute_values(conn_edu, cursor_edu, accounts, 'de11an.mrnv_stg_accounts')
f.execute_values(conn_edu, cursor_edu, cards, 'de11an.mrnv_stg_cards')
f.execute_values(conn_edu, cursor_edu, terminals, 'de11an.mrnv_stg_terminals')
f.execute_values(conn_edu, cursor_edu, transactions, 'de11an.mrnv_stg_transactions')
f.execute_values(conn_edu, cursor_edu, passports_blacklist, 'de11an.mrnv_stg_passport_blacklist')


# Захват в стейджинг ключей из источника полным срезом для вычисления удалений.

f.execute_values(conn_edu, cursor_edu, pd.DataFrame(clients['client_id']), 'de11an.mrnv_stg_clients_del')
f.execute_values(conn_edu, cursor_edu, pd.DataFrame(accounts['account_num']), 'de11an.mrnv_stg_accounts_del')
f.execute_values(conn_edu, cursor_edu, pd.DataFrame(cards['card_num']), 'de11an.mrnv_stg_cards_del')
f.execute_values(conn_edu, cursor_edu, pd.DataFrame(terminals['terminal_id']), 'de11an.mrnv_stg_terminals_del')

# Загрузка данных из стейджинга в таргет

scripts = ['clients', 'cards', 'accounts', 'terminals', 'transactions', 'passports']
f.load_data_to_dwh(conn_edu, cursor_edu, scripts)

# Построение отчета

f.create_report(cursor_edu, conn_edu)

# Закрываем соединение

conn_edu.close()
conn_bank.close()
cursor_edu.close()
cursor_bank.close()

# Перемещаем отработанные файлы в папку archive

f.move_file(transactions_file)
f.move_file(terminals_file)
f.move_file(passport_blacklists_file)

