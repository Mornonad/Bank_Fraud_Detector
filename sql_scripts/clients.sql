
-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).

insert into de11an.mrnv_dwh_dim_clients_hist(client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, start_dt, end_dt, deleted_flg)
select 
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.create_dt as start_dt, 
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	0 as deleted_flg 
from de11an.mrnv_stg_clients stg
left join de11an.mrnv_dwh_dim_clients_hist tgt
on stg.client_id = tgt.client_id
	and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = '0'
where tgt.client_id is null;


-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).

update de11an.mrnv_dwh_dim_clients_hist
set 
	end_dt = tmp.update_dt - interval '1 day'
from (
	select 
		stg.client_id, 
		stg.update_dt
	from de11an.mrnv_stg_clients stg
	inner join de11an.mrnv_dwh_dim_clients_hist tgt
	on stg.client_id = tgt.client_id
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
	where stg.client_id <> tgt.client_id
		or stg.last_name <> tgt.last_name 
		or stg.first_name <> tgt.first_name
		or stg.patronymic <> tgt.patronymic
		or stg.passport_num <> tgt.passport_num
		or stg.passport_valid_to <> tgt.passport_valid_to 
		or stg.phone <> tgt.phone
		or ( stg.client_id is null and tgt.client_id is not null ) 	or ( stg.client_id is not null and tgt.client_id is null )
		or ( stg.last_name is null and tgt.last_name is not null ) 	or ( stg.last_name is not null and tgt.last_name is null )
		or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
		or ( stg.patronymic is null and tgt.patronymic is not null ) 	or ( stg.patronymic is not null and tgt.patronymic is null )
		or ( stg.passport_num is null and tgt.passport_num is not null ) 	or ( stg.passport_num is not null and tgt.passport_num is null )
		or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
		or ( stg.phone is null and tgt.phone is not null ) 	or ( stg.phone is not null and tgt.phone is null )
) tmp
where mrnv_dwh_dim_clients_hist.client_id = tmp.client_id
	and mrnv_dwh_dim_clients_hist.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' ); 


insert into de11an.mrnv_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, start_dt, end_dt, deleted_flg)
select 
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.update_dt as start_dt, 
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	0 as deleted_flg 
from de11an.mrnv_stg_clients stg
inner join de11an.mrnv_dwh_dim_clients_hist tgt
	on stg.client_id = tgt.client_id
	and tgt.end_dt = date(stg.update_dt - interval '1 day') 
	and tgt.deleted_flg = '0'
where stg.client_id <> tgt.client_id
		or stg.last_name <> tgt.last_name 
		or stg.first_name <> tgt.first_name
		or stg.patronymic <> tgt.patronymic
		or stg.passport_num <> tgt.passport_num
		or stg.passport_valid_to <> tgt.passport_valid_to 
		or stg.phone <> tgt.phone
		or ( stg.client_id is null and tgt.client_id is not null ) 	or ( stg.client_id is not null and tgt.client_id is null )
		or ( stg.last_name is null and tgt.last_name is not null ) 	or ( stg.last_name is not null and tgt.last_name is null )
		or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
		or ( stg.patronymic is null and tgt.patronymic is not null ) 	or ( stg.patronymic is not null and tgt.patronymic is null )
		or ( stg.passport_num is null and tgt.passport_num is not null ) 	or ( stg.passport_num is not null and tgt.passport_num is null )
		or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
		or ( stg.phone is null and tgt.phone is not null ) 	or ( stg.phone is not null and tgt.phone is null );
	
	
-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).

insert into de11an.mrnv_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, start_dt, end_dt, deleted_flg)
select 
	tgt.client_id,
	tgt.last_name,
	tgt.first_name,
	tgt.patronymic,
	tgt.date_of_birth,
	tgt.passport_num,
	tgt.passport_valid_to,
	tgt.phone,
	date(now()) as start_dt, 
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	1 as deleted_flg 
from de11an.mrnv_dwh_dim_clients_hist tgt
where tgt.client_id in (
	select tgt.client_id
	from de11an.mrnv_dwh_dim_clients_hist tgt
	left join de11an.mrnv_stg_clients_del stg
		on stg.client_id = tgt.client_id
	where stg.client_id is null
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
)
and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
and tgt.deleted_flg = '0';




update de11an.mrnv_dwh_dim_clients_hist
set 
	end_dt = date(now() - interval '1 day')
where mrnv_dwh_dim_clients_hist.client_id in (
	select tgt.client_id
	from de11an.mrnv_dwh_dim_clients_hist tgt
	left join de11an.mrnv_stg_clients stg
	on stg.client_id = tgt.client_id
	where stg.client_id is null
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
)
and mrnv_dwh_dim_clients_hist.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
and mrnv_dwh_dim_clients_hist.deleted_flg = '0';


-- 4. Обновление метаданных.

update de11an.mrnv_meta_bank
set max_update_dt = coalesce(
    ( select max( update_dt ) from de11an.mrnv_stg_clients ),
    ( select max_update_dt from de11an.mrnv_meta_bank
      where schema_name='de11an' and table_name='mrnv_clients' )
)
where schema_name='de11an' and table_name = 'mrnv_clients';

-- 5. Фиксация транзакции.

commit;




