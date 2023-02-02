--1. Creating a view with all required information

create or replace view de11an.mrnv_rep_fraud_view as
select
 	t.trans_date,
	t.amt,
 	t.oper_result, 
 	a.valid_to,
 	c.card_num,
 	cl.passport_num,
 	cl.passport_valid_to,
	cl.phone,
	cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic  as fio,
	ter.terminal_city,
	coalesce(lead(t.trans_date) over w, t.trans_date) as next_trans_date,
	coalesce(lead(ter.terminal_city) over w, ter.terminal_city) as next_city,
	
	coalesce(lag(t.amt, 3) over w, t.amt)  as lag_amt_3,
	coalesce(lag(t.amt, 2) over w, t.amt) as lag_amt_2,
	coalesce(lag(t.amt) over w, t.amt) as lag_amt_1,
	
	coalesce(lag(t.trans_date, 3) over w, t.trans_date) as lag_time_3,
	coalesce(lag(t.trans_date, 2) over w, t.trans_date) as lag_time_2,
	coalesce(lag(t.trans_date) over w, t.trans_date) as lag_time_1,
	
	coalesce(lag(t.oper_result, 3) over w, t.oper_result) as lag_oper_res_3,
	coalesce(lag(t.oper_result, 2) over w, t.oper_result) as lag_oper_res_2,
	coalesce(lag(t.oper_result) over w, t.oper_result) as lag_oper_res_1
from de11an.mrnv_dwh_fact_transactions t 
left join de11an.mrnv_dwh_dim_terminals_hist ter
	on t.terminal = ter.terminal_id
left join de11an.mrnv_dwh_dim_cards_hist c 
	on t.card_num = c.card_num
left join de11an.mrnv_dwh_dim_accounts_hist a 
	on c.account_num = a.account_num 
left join de11an.mrnv_dwh_dim_clients_hist cl
	on a.client = cl.client_id 
window w as (partition by c.card_num order by t.trans_date);

--2.	insert to report table

insert into de11an.mrnv_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
with event_table as (
		select 
		trans_date as event_dt,
		passport_num,
		fio,
		phone,
		case 
			when (passport_valid_to is not null 
					and trans_date > passport_valid_to)
					or passport_num in (select passport_num 
										from de11an.mrnv_dwh_fact_passport_blacklist)
				then '1'
			when valid_to < date(trans_date)
				then '2'
			when terminal_city <> next_city
	    			and extract(epoch from next_trans_date - trans_date) / 60 < 60
	    		then '3'
	    	when extract(epoch from lag_time_1 - lag_time_3) / 60 <= 20
					and lag_amt_3 > lag_amt_2
					and  lag_amt_2 > lag_amt_1 
					and lag_amt_1 > amt
					and lag_oper_res_3 = 'REJECT' 
					and lag_oper_res_2 = 'REJECT' 
					and lag_oper_res_1 = 'REJECT' 	
					and oper_result = 'SUCCESS'
				then '4'
			else Null
		end	as event_type,
	 	( select max_update_dt from de11an.mrnv_meta_bank where schema_name='de11an' and table_name='mrnv_trans' ) as report_dt
	 from de11an.mrnv_rep_fraud_view
	 where date(trans_date) = ( 
								select max_update_dt 
								from de11an.mrnv_meta_bank 
								where schema_name='de11an' and table_name='mrnv_trans' )
)
select  *
from event_table
where event_type is not Null;
									
									
							
			