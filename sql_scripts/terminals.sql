-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).

insert into de11an.mrnv_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, start_dt, end_dt, deleted_flg)
select
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	stg.update_dt as start_dt,
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	'0' as deleted_flg
from de11an.mrnv_stg_terminals stg
left join de11an.mrnv_dwh_dim_terminals_hist tgt
	on stg.terminal_id = tgt.terminal_id
	and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = '0'
where tgt.terminal_id is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).

-- 2.1 Обновление старой записи 

update de11an.mrnv_dwh_dim_terminals_hist
set 
	end_dt = tmp.update_dt- interval '1 day'
from  (
	select 
		stg.terminal_id,
		stg.update_dt
	from de11an.mrnv_stg_terminals stg
	inner join de11an.mrnv_dwh_dim_terminals_hist tgt
		on stg.terminal_id = tgt.terminal_id
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
	where stg.terminal_type <> tgt.terminal_type 
		or stg.terminal_city <> tgt.terminal_city 
		or stg.terminal_address <> tgt.terminal_address 
		or ( stg.terminal_type is null and tgt.terminal_type is not null ) 	or ( stg.terminal_type is not null and tgt.terminal_type is null )
		or ( stg.terminal_city is null and tgt.terminal_city is not null ) 	or ( stg.terminal_city is not null and tgt.terminal_city is null )
		or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
) tmp
where mrnv_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
	and mrnv_dwh_dim_terminals_hist.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' ); 

-- 2.2 Добавление измененной записи

insert into de11an.mrnv_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, start_dt, end_dt, deleted_flg)
select 
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	stg.update_dt as start_dt,
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	'0' as deleted_flg
from de11an.mrnv_stg_terminals stg
inner join de11an.mrnv_dwh_dim_terminals_hist tgt
		on stg.terminal_id = tgt.terminal_id
		and tgt.end_dt = stg.update_dt - interval '1 day'
		and tgt.deleted_flg = '0'
where stg.terminal_type <> tgt.terminal_type 
		or stg.terminal_city <> tgt.terminal_city 
		or stg.terminal_address <> tgt.terminal_address 
		or ( stg.terminal_type is null and tgt.terminal_type is not null ) 	or ( stg.terminal_type is not null and tgt.terminal_type is null )
		or ( stg.terminal_city is null and tgt.terminal_city is not null ) 	or ( stg.terminal_city is not null and tgt.terminal_city is null )
		or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null );


-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).

-- 3.1 Добавление новой удаленной записи
	
insert into de11an.mrnv_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, start_dt, end_dt, deleted_flg)
select 
	tgt.terminal_id,
	tgt.terminal_type,
	tgt.terminal_city,
	tgt.terminal_address,
	now() as start_dt,
	to_date( '2999-12-31', 'YYYY-MM-DD' ) as end_dt,
	'1' as deleted_flg
from de11an.mrnv_dwh_dim_terminals_hist tgt
where tgt.terminal_id in (
	select tgt.terminal_id
	from de11an.mrnv_dwh_dim_terminals_hist tgt
	left join de11an.mrnv_stg_terminals_del stg
	on stg.terminal_id = tgt.terminal_id
	where stg.terminal_id is null
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
)
	and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = '0';

-- 3.2 обновление удаленной старой записи

update de11an.mrnv_dwh_dim_terminals_hist
set 
	end_dt = now() - interval '1 day'
where mrnv_dwh_dim_terminals_hist.terminal_id in (
	select tgt.terminal_id
	from de11an.mrnv_dwh_dim_terminals_hist tgt
	left join de11an.mrnv_stg_terminals_del stg
	on stg.terminal_id = tgt.terminal_id
	where stg.terminal_id is null
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
)
and mrnv_dwh_dim_terminals_hist.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
and mrnv_dwh_dim_terminals_hist.deleted_flg = '0';


-- 4. Обновление метаданных.

update de11an.mrnv_meta_bank
set max_update_dt = coalesce(
    ( select max( update_dt ) from de11an.mrnv_stg_terminals ),
    ( select max_update_dt from de11an.mrnv_meta_bank
      where schema_name='de11an' and table_name='mrnv_terminals' )
)
where schema_name='de11an' and table_name = 'mrnv_terminals';

-- 5. Фиксация транзакции.

commit;


