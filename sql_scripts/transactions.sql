-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).


insert into de11an.mrnv_dwh_fact_transactions(trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal, update_dt)
select 
	stg.trans_id,
	stg.trans_date,
	stg.card_num,
	stg.oper_type, 
	stg.amt,
	stg.oper_result,
	stg.terminal,
	stg.update_dt 
from de11an.mrnv_stg_transactions stg
left join de11an.mrnv_dwh_fact_transactions tgt
	on stg.trans_id = tgt.trans_id
where tgt.trans_id is null;


-- 2. Обновление метаданных.

update de11an.mrnv_meta_bank
set max_update_dt = coalesce(
    ( select max( update_dt ) from de11an.mrnv_stg_transactions ),
    ( select max_update_dt from de11an.mrnv_meta_bank
      where schema_name='de11an' and table_name='mrnv_trans' )
)
where schema_name='de11an' and table_name = 'mrnv_trans';

-- 3. Фиксация транзакции.

commit;



