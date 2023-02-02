-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).


insert into de11an.mrnv_dwh_dim_cards_hist( card_num,account_num,start_dt, end_dt, deleted_flg)
select 
	stg.card_num,
	stg.account_num,
	stg.create_dt as start_dt, 
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	0 as deleted_flg 
from de11an.mrnv_stg_cards stg
left join de11an.mrnv_dwh_dim_cards_hist tgt
on stg.card_num = tgt.card_num
	and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = '0'
where tgt.card_num is null;

--# 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
--# 2.1 Обновление старой записи 

update de11an.mrnv_dwh_dim_cards_hist
set 
	end_dt = tmp.update_dt - interval '1 day'
from (
	select 
		stg.card_num, 
		stg.update_dt
	from de11an.mrnv_stg_cards stg
	inner join de11an.mrnv_dwh_dim_cards_hist tgt
	on stg.card_num = tgt.card_num
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
	where stg.card_num <> tgt.card_num
		or stg.account_num <> tgt.account_num 
		or ( stg.account_num is null and tgt.account_num is not null ) 	or ( stg.account_num is not null and tgt.account_num is null )
		or ( stg.card_num is null and tgt.card_num is not null ) 	or ( stg.card_num is not null and tgt.card_num is null )
) tmp
where mrnv_dwh_dim_cards_hist.card_num = tmp.card_num
	and mrnv_dwh_dim_cards_hist.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' ); 

--# 2.2 Добавление измененной записи

insert into de11an.mrnv_dwh_dim_cards_hist(card_num, account_num, start_dt, end_dt, deleted_flg)
select 
	stg.card_num,
	stg.account_num,
	stg.update_dt as start_dt, 
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	0 as deleted_flg 
from de11an.mrnv_stg_cards stg
inner join de11an.mrnv_dwh_dim_cards_hist tgt
	on stg.card_num = tgt.card_num
	and tgt.end_dt = date(stg.update_dt - interval '1 day') 
	and tgt.deleted_flg = '0'
where stg.card_num <> tgt.card_num
	or stg.account_num <> tgt.account_num 
	or ( stg.account_num is null and tgt.account_num is not null ) 	or ( stg.account_num is not null and tgt.account_num is null )
	or ( stg.card_num is null and tgt.card_num is not null ) 	or ( stg.card_num is not null and tgt.card_num is null );
	
	
--# 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
--#  3.1 Добавление новой удаленной записи

insert into de11an.mrnv_dwh_dim_cards_hist(card_num, account_num, start_dt, end_dt, deleted_flg)
select 
	tgt.card_num,
	tgt.account_num,
	date(now()) as start_dt, 
	to_date('2999-12-31', 'YYYY-MM-DD') as end_dt,
	1 as deleted_flg 
from de11an.mrnv_dwh_dim_cards_hist tgt
where tgt.card_num in (
	select tgt.card_num
	from de11an.mrnv_dwh_dim_cards_hist tgt
	left join de11an.mrnv_stg_cards_del stg
		on stg.card_num = tgt.card_num
	where stg.card_num is null
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
)
	and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = '0';

--# 3.2 обновление удаленной старой записи


update de11an.mrnv_dwh_dim_cards_hist
set 
	end_dt = date(now() - interval '1 day')
where mrnv_dwh_dim_cards_hist.card_num in (
	select tgt.card_num
	from de11an.mrnv_dwh_dim_cards_hist tgt
	left join de11an.mrnv_stg_cards stg
	on stg.card_num = tgt.card_num
	where stg.card_num is null
		and tgt.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = '0'
)
and mrnv_dwh_dim_cards_hist.end_dt = to_date( '2999-12-31', 'YYYY-MM-DD' )
and mrnv_dwh_dim_cards_hist.deleted_flg = '0';


-- 4. Обновление метаданных.

update de11an.mrnv_meta_bank
set max_update_dt = coalesce(
    ( select max( update_dt ) from de11an.mrnv_stg_cards ),
    ( select max_update_dt from de11an.mrnv_meta_bank
      where schema_name='de11an' and table_name='mrnv_cards' )
)
where schema_name='de11an' and table_name = 'mrnv_cards';

-- 5. Фиксация транзакции.

commit;