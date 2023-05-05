--set ORIG_LB = ;
--set ORIG_UB = ;
--set ASOF_UB = ;
--set MOB_UB = ;

with loans as (
select r.lt_payoffuid
     , r.originationdate

     , /* CP6.6 */
       case
            /* Can't divide by zero for credit limit at the bureau */
            when b.V71_RE28S < 0.01 and r.PRICINGTIER != 'TH' then 0
            /* Incremental credit requested > 2 */
            when r.requested_loan_amount > (2 * b.V71_RE28S)  and r.PRICINGTIER != 'TH' then 0
            when r.fico_score < 640  and r.PRICINGTIER != 'TH' then 0
            when r.at20s_mo_since_oldest_trade_opened < 48  and r.PRICINGTIER != 'TH' then 0
            when r.loantier='T6' then 0
            when r.loan_intent not in ('core') and r.loantier in ('T4','T5')  and r.PRICINGTIER != 'TH' then 0
            else 1 end as cp65_66_flag
            
     , /* CP6.7 */
       case
            /* Can't divide by zero for credit limit and summary balance */
            when b.V71_RE28S < 0.01 and r.PRICINGTIER != 'TH' then 0
            when r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance < 0.01 and r.PRICINGTIER != 'TH' then 0
            /* Tier 6 */
            when r.loantier='T6' then 0
            /* Incremental credit requested > 2 */
            when r.requested_loan_amount > (2 * b.V71_RE28S)  and r.PRICINGTIER != 'TH' then 0
            /* fico < 640 */
            when r.fico_score < 640  and r.PRICINGTIER != 'TH' then 0
            /* credit history < 4yrs */
            when r.at20s_mo_since_oldest_trade_opened < 48  and r.PRICINGTIER != 'TH' then 0
            /* LPE */
            when r.loan_intent not in ('core') and r.PRICINGTIER != 'TH' then 0
            /* requested loan amount / summary balance > 1.8*/
            when r.requested_loan_amount> 1.8*(r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance)
            and r.PRICINGTIER != 'TH' then 0
            /* summary balance < 10k*/
            when r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance < 10000
            and r.PRICINGTIER != 'TH' then 0
            else 1 end as cp67_flag
      
      , /* CP6.8 */
       case
            /* Can't divide by zero for credit limit and summary balance */
            when b.V71_RE28S < 0.01 and r.PRICINGTIER != 'TH' then 0
            when r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance < 0.01 and r.PRICINGTIER != 'TH' then 0
            /* Tier 6 */
            when r.loantier='T6' then 0
            /* Incremental credit requested > 1.3 */
            when r.requested_loan_amount > (1.3 * b.V71_RE28S)  and r.PRICINGTIER != 'TH' then 0
            /* fico < 640 */
            when r.fico_score < 640  and r.PRICINGTIER != 'TH' then 0
            /* credit history < 6yrs */
            when r.at20s_mo_since_oldest_trade_opened < 72  and r.PRICINGTIER != 'TH' then 0
            /* LPE */
            when r.loan_intent not in ('core') and r.PRICINGTIER != 'TH' then 0
            /* requested loan amount / summary balance > 1.3 */
            when r.requested_loan_amount> 1.3*(r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance)
            and r.PRICINGTIER != 'TH' then 0
            /* summary balance < 10k*/
            when r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance < 10000
            and r.PRICINGTIER != 'TH' then 0
            else 1 end as cp68_flag
     
     , case when f.hk_h_appl is not null then 1 else 0 end as fraud_flag
     , r.pricingtier
     , r.loan_intent
     , r.fico_score
     , r.monthlydti
     , r.tradelinendi
     , r.loanamount
     , r.term

from cron_store.dsh_credit_policy_monitoring_loan_retro r

left join data_store.vw_credit_policy_decision_parsed c 
  on r.lt_payoffuid = c.payoffuid
  and c.source = 'prod' and c.active_record_flag = true

left join data_store.vw_bureau_variables_00v71 b
  on c.payoffuid = b.payoff_uid
  and c.request_guid = b.request_guid
  
left join data_store.vw_application a 
  on a.payoff_uid = r.lt_payoffuid
  
left join (select distinct hk_h_appl 
           from data_store.vw_appl_tags
           where application_tag in ('Potential Fraud', 'Confirmed Fraud')
           and softdelete = 'False') f
  on f.hk_h_appl = a.hk_h_appl
  
where r.originationdate between $ORIG_LB and $ORIG_UB
)

select date_trunc('month', l.originationdate) as orig_mth
     , e.asofdate
     , e.monthonbook
     , case when l.pricingtier = 'TH' then 'Turndown'
            when l.loan_intent != 'core' then 'LPE'
            when l.loan_intent = 'core' and 
                 (l.fico_score < 640 or l.monthlydti > 0.55 or l.tradelinendi < 1000 or l.pricingtier = 't6') then 'Experimental'
            when e.model_v6_score <= 0.04707866 then 'Tier 1'
            when e.model_v6_score > 0.04707866 and e.model_v6_score <= 0.14240034 then 'Tier 2'
            when e.model_v6_score > 0.14240034 and e.model_v6_score <= 0.25136217 then 'Tier 3'
            when e.model_v6_score > 0.25136217 and e.model_v6_score <= 0.33030704 then 'Tier 4'
            when e.model_v6_score > 0.33030704 and e.model_v6_score <= 0.36716518 then 'Tier 5'
            when l.pricingtier is null then 'Turndown'
            else 'Null'
            end as segment
     , l.term
     , l.cp65_66_flag
     , l.cp67_flag
     , l.cp68_flag
     , l.fraud_flag
     , case when segment = 'Tier 3' and l.term = 60 then 0
            when segment = 'Tier 4' then 0
            when segment = 'Tier 5' then 0
            else 1 end as price_flag
     
     --, count(distinct l.lt_payoffuid) as loan_cnt
     , sum(l.loanamount) as loan_amt
     , sum(e.expectedmonthlydollarchargeoff) as exp_mth_co
     , sum(e.actualmonthlychargeoffamount) as act_mth_co
     , sum(e.remainingprincipal) as remain_prin
     , sum(case when e.status in ('Delinquent 1 Payment',
                                  'Seriously Delinquent 2 Payments',
                                  'Seriously Delinquent 3+ Payments')
                then e.remainingprincipal else 0 end) as bal_30
     
from loans l

left join cron_store.dsh_loan_portfolio_expectations e
  on l.lt_payoffuid = e.payoffuid
  and e.sim_name = 'v1.0 Base Case'
  and (e.asofdate <= $ASOF_UB or e.monthonbook <= $MOB_UB)
  
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
;
