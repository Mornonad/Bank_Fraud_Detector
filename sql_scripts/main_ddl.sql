--meta

create table de11an.mrnv_meta_bank(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt date
);

-- terminals 

create table de11an.mrnv_stg_terminals( 
	terminal_id varchar(10),
	terminal_type varchar(10),
	terminal_city varchar(30),
	terminal_address varchar(100),
	update_dt date
);

create table de11an.mrnv_dwh_dim_terminals_hist(
	terminal_id varchar(10),
	terminal_type varchar(10),
	terminal_city varchar(30),
	terminal_address varchar(100),
	start_dt date,
	end_dt date,
	deleted_flg varchar(2)
);

create table de11an.mrnv_stg_terminals_del (
    terminal_id char(5)
);


--- transactions

create table de11an.mrnv_stg_transactions( 
	trans_id varchar(20),
	trans_date timestamp(0),
	amt decimal(14,2),
	card_num varchar(25),
	oper_type varchar(20), 
	oper_result varchar(20),
	terminal varchar(20),
	update_dt date
);

create table de11an.mrnv_dwh_fact_transactions( 
	trans_id varchar(20),
	trans_date timestamp(0),
	amt decimal(14,2),
	card_num varchar(25),
	oper_type varchar(20), 
	oper_result varchar(20),
	terminal varchar(20),
	update_dt date
);

-- passport blacklist 

create table de11an.mrnv_stg_passport_blacklist(
	passport_num varchar(15),
	entry_dt date
);

create table de11an.mrnv_dwh_fact_passport_blacklist( 
	passport_num varchar(15),
	entry_dt date
);

--clients

create table de11an.mrnv_stg_clients(
	client_id varchar(20),
	last_name varchar(40),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
	create_dt date,
	update_dt date
);

create table de11an.mrnv_dwh_dim_clients_hist(
	client_id varchar(20),
	last_name varchar(40),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
	start_dt date,
	end_dt date,
	deleted_flg varchar(2)
);

create table de11an.mrnv_stg_clients_del (
    client_id varchar(20)
);

--accounts

create table de11an.mrnv_stg_accounts( 
	account_num varchar(40),
	valid_to date,
	client varchar(20),
	create_dt date,
	update_dt date
);

create table de11an.mrnv_dwh_dim_accounts_hist( 
	account_num varchar(40),
	valid_to date,
	client varchar(20),
	start_dt date,
	end_dt date,
	deleted_flg varchar(2)
);

create table de11an.mrnv_stg_accounts_del (
    account_num varchar(40)
);

--accounts

create table de11an.mrnv_stg_cards( 
	card_num varchar(25),
	account_num varchar(40),
	create_dt date,
	update_dt date
);

create table de11an.mrnv_dwh_dim_cards_hist( 
	card_num varchar(25),
	account_num varchar(40),
	start_dt date,
	end_dt date,
	deleted_flg varchar(2)
);

create table de11an.mrnv_stg_cards_del (
    card_num varchar(25)
);

--- report table 

create table de11an.mrnv_rep_fraud(
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(100),
	phone varchar(20),
	event_type varchar(10),
	report_dt date
);


insert into de11an.mrnv_meta_bank( schema_name, table_name, max_update_dt )
	values( 'de11an','mrnv_clients', to_date('1800-01-01','YYYY-MM-DD') );

insert into de11an.mrnv_meta_bank( schema_name, table_name, max_update_dt )
	values( 'de11an','mrnv_accounts', to_date('1800-01-01','YYYY-MM-DD') );

insert into de11an.mrnv_meta_bank( schema_name, table_name, max_update_dt )
	values( 'de11an','mrnv_cards', to_date('1800-01-01','YYYY-MM-DD') );

insert into de11an.mrnv_meta_bank( schema_name, table_name, max_update_dt )
	values( 'de11an','mrnv_terminals', to_date('1800-01-01','YYYY-MM-DD') );

insert into de11an.mrnv_meta_bank( schema_name, table_name, max_update_dt )
	values( 'de11an','mrnv_trans', to_date('1800-01-01','YYYY-MM-DD') );

insert into de11an.mrnv_meta_bank( schema_name, table_name, max_update_dt )
	values( 'de11an','mrnv_passports', to_date('1800-01-01','YYYY-MM-DD') );
