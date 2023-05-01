--set ORIG_LB = ;
--set ORIG_UB = ;
--set ASOF_UB = ;
--set MOB_UB = ;

WITH cp6_consolidated AS
(
SELECT
cpd.payoffuid,
IFNULL(cp6_retro.MODEL_V6_SCORE, CASE WHEN cpd.CREDITPOLICYVERSION IN ('v3','v4','v5','v5_5') THEN NULL ELSE cpd.LOGISTICALPREDICTIONOFDEFAULT END) as MODEL_V6_SCORE_Retro

FROM
"BUSINESS_INTELLIGENCE"."DATA_STORE"."VW_CREDIT_POLICY_DECISION_PARSED" as cpd LEFT JOIN
"BUSINESS_INTELLIGENCE"."CRON_STORE"."RPT_CP6_DECISION_RETRO" as cp6_retro on cpd.PayoffUID = cp6_retro.PayoffUID

WHERE cpd.source = 'prod'
AND cpd.ACTIVE_RECORD_FLAG = true
)


select
year(r.originationdate) * 100 + quarter(r.originationdate) as orig_ccyyqq_dt,
r.loantier,
r.term,
case when MODEL_V6_SCORE_Retro <= 0.04707866 then 'T1'
when MODEL_V6_SCORE_Retro > 0.04707866 and MODEL_V6_SCORE_Retro <= 0.14240034 then 'T2'
when MODEL_V6_SCORE_Retro > 0.14240034 and MODEL_V6_SCORE_Retro <= 0.25136217 then 'T3'
when MODEL_V6_SCORE_Retro > 0.25136217 and MODEL_V6_SCORE_Retro <= 0.33030704 then 'T4'
when MODEL_V6_SCORE_Retro > 0.33030704 and MODEL_V6_SCORE_Retro <= 0.36716518 then 'T5'
when MODEL_V6_SCORE_Retro > 0.36716518 and MODEL_V6_SCORE_Retro <= 0.42476870 then 'T6'
when MODEL_V6_SCORE_Retro > 0.42476870 then 'T7'
else null
end as Score_bucket,
e.monthonbook,
case when r.pricingtier = 'TH' then 0
when r.pricingtier is null then 0
when r.loan_intent != 'core' then 0
when r.loan_intent = 'core' and (fico_score < 640 or r.monthlydti > 0.55 or r.tradelinendi < 1000 or r.pricingtier = 't6') then 0
else 1 end as core_cu_flag,
/* Current Credit Policy Post CP6.7 */
case
when r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance < 0.01 and r.PRICINGTIER != 'TH' then 0
/* Tier 6 */
when r.loantier='T6' then 0
/* Incremental credit requested > 2 */
when r.requested_loan_amount > (1.3 * b.V71_RE28S)  and r.PRICINGTIER != 'TH' then 0
/* fico < 640 */
when r.fico_score < 640  and r.PRICINGTIER != 'TH' then 0
/* credit history < 4yrs */
when r.at20s_mo_since_oldest_trade_opened < 72  and r.PRICINGTIER != 'TH' then 0
/* LPE */
when r.loan_intent not in ('core') and r.PRICINGTIER != 'TH' then 0
/* requested loan amount / summary balance > 1.8*/
when r.requested_loan_amount> 1.3*(r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance) and r.PRICINGTIER != 'TH' then 0
/* summary balance < 10k*/
when r.tradelineunsecuredinstallmentloansbalance + r.tradelinerevolvingtradesbalance < 10000 and r.PRICINGTIER != 'TH' then 0
else 1 end as current_policy_post68_flag,
case when m.MODEL_V6_SCORE_retro >= 0.33030704 then 0
when r.term = 48 and m.MODEL_V6_SCORE_retro >= 0.25136217 then 0
when r.term = 60 and m.MODEL_V6_SCORE_retro >= 0.14240034 then 0
else 1 end as current_pricing_flag,
sum(e.ACTUALMONTHLYCHARGEOFFAMOUNT) as act_mth_co,
sum(e.EXPECTEDMONTHLYDOLLARCHARGEOFF) as exp_mth_co,
sum(r.loanamount) as loanamount,
sum(CUMCHARGEOFFAMOUNT) as act_cum_co,
sum(EXPECTEDCUMDOLLARCHARGEOFF) as exp_cum_co

from

"BUSINESS_INTELLIGENCE"."CRON_STORE"."DSH_CREDIT_POLICY_MONITORING_LOAN_RETRO" as r left join
"BUSINESS_INTELLIGENCE"."DATA_STORE"."VW_CREDIT_POLICY_DECISION_PARSED" as c on r.lt_payoffuid = c.payoffuid and c.SOURCE = 'prod' and c.ACTIVE_RECORD_FLAG = true left join
"BUSINESS_INTELLIGENCE"."DATA_STORE"."VW_BUREAU_VARIABLES_00V71" as b on c.payoffuid = b.payoff_uid and b.request_guid = c.request_guid left join
"BUSINESS_INTELLIGENCE"."CRON_STORE"."DSH_LOAN_PORTFOLIO_EXPECTATIONS" as e on r.lt_payoffuid = e.payoffuid and e.SIM_NAME = 'v1.0 Base Case' left join
cp6_consolidated as m on r.lt_payoffuid = m.payoffuid

where
r.originationdate >= $ORIG_LB
and r.originationdate <= $ORIG_UB
and (e.asofdate <= $ASOF_UB or e.monthonbook <= $MOB_UB)

group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5,6,7,8