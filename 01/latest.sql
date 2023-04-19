--set LAST_MTH = ;

with loans as (
select r.lt_payoffuid
     , r.originationdate
     
     , r.pricingtier
     , r.loan_intent
     , r.fico_score
     , r.monthlydti
     , r.tradelinendi
     , r.loanamount
     , r.term

from cron_store.dsh_credit_policy_monitoring_loan_retro r
where r.originationdate between date_trunc('month', $LAST_MTH::date) and $LAST_MTH
)

select case when l.pricingtier = 'TH' then 'Turndown'
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
            
     , sum(l.loanamount) as loan_amt
     , sum(e.expectedcumdollarchargeoff) as exp_cum_co
     , sum(e.expectedcummonthlyoutstanding) as exp_cum_outstanding
     
from loans l

left join cron_store.dsh_loan_portfolio_expectations e
  on l.lt_payoffuid = e.payoffuid
  and e.sim_name = 'v1.0 Base Case'
  and e.monthonbook = e.term
  
group by 1
order by 1
;
