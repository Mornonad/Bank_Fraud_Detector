--password_blacklist


-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).


insert into de11an.mrnv_dwh_fact_passport_blacklist( passport_num, entry_dt )
select 
	stg.passport_num,
	stg.entry_dt 
from de11an.mrnv_stg_passport_blacklist stg
left join de11an.mrnv_dwh_fact_passport_blacklist tgt
	on stg.passport_num = tgt.passport_num
where tgt.passport_num is null;


-- 2. Обновление метаданных.

update de11an.mrnv_meta_bank
set max_update_dt = coalesce(
    ( select max( entry_dt ) from de11an.mrnv_stg_passport_blacklist ),
    ( select max_update_dt from de11an.mrnv_meta_bank
      where schema_name='de11an' and table_name='mrnv_passports' )
)
where schema_name='de11an' and table_name = 'mrnv_passports';

-- 3. Фиксация транзакции.

commit;

