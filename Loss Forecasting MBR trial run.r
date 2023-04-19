# Databricks notebook source
sessionInfo()

# COMMAND ----------

install.packages('snakecase')

# COMMAND ----------

packageVersion("snakecase")

# COMMAND ----------

getwd()

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # Install ODBC drivers for Snowflake
# MAGIC curl https://sfc-repo.snowflakecomputing.com/odbc/linux/2.25.7/snowflake-odbc-2.25.7.x86_64.deb -o snowflake-odbc-2.25.7.x86_64.deb
# MAGIC sudo dpkg -i snowflake-odbc-2.25.7.x86_64.deb
# MAGIC sed -i 's/SF_ACCOUNT/happymoney.us-east-1/' /etc/odbc.ini

# COMMAND ----------

# DBTITLE 1,Snowflake.R
library(odbc)
library(DBI)

# For PII user, swap "snowflake_cron" with "snowflake_cron_pii"
CONN <- DBI::dbConnect(odbc::odbc(), "snowflake",
                          uid = dbutils.secrets.get("snowflake_cron", "username"), 
                          pwd = dbutils.secrets.get("snowflake_cron", "password"),
                          server = "happymoney.us-east-1.snowflakecomputing.com")

dbGetQuery(CONN, "use role \"BUSINESS_INTELLIGENCE_TOOLS\";")
dbGetQuery(CONN, "use warehouse \"BUSINESS_INTELLIGENCE_TOOLS\";")
dbGetQuery(CONN, "use database \"BUSINESS_INTELLIGENCE\";")

# COMMAND ----------

set_param = function(p, is_num = F) {
  query = glue::glue("set {deparse(substitute(p))} = '{p}';")
  if (is_num) query = glue::glue('set {deparse(substitute(p))} = {p};')
  dbGetQuery(CONN, query)
}

# COMMAND ----------

# DBTITLE 1,01_pull_data
root = getwd()
s3root = '/dbfs/mnt/science/HanjunL/LossForecasting/MBR'
CUR_STEP = '01'

# COMMAND ----------

library(data.table)

# COMMAND ----------

### Date parameters

library(lubridate)
DP = list(
  ## Dates defining the current period
  # Currently observed period of elevated risk began in June 2022
  Period_LB = as.Date('2022-05-31'),
  Period_Actual_UB = floor_date(Sys.Date(), 'month') - 1,
  Period_Implied_UB = floor_date(Sys.Date() %m+% months(3), 'month') - 1,
  
  ## Dates defining the period used for expectation calculation
  Expectation_Range = "21Q4-22Q3",
  Expectation_LB = as.Date('2021-10-01')
)
DP$Expectation_UB = DP$Expectation_LB %m+% months(12) - 1
detach(package:lubridate, unload=T)

# saveRDS(DP, file.path(root, CUR_STEP, 'date_params.rds'))
saveRDS(DP, file.path(root, CUR_STEP, 'date_params.rds'))

# COMMAND ----------

### Pull performance data

## Query
# This will be used to pull both actual performance data up to latest month
# and data in defined period to build expectation curve.
# For these two uses, we just need to set different parameters.
query = readr::read_file(file.path(root, CUR_STEP, 'perf_or_exp.sql'))


## SQL Parameters
ORIG_LB = as.Date('2000-01-01') # date in distant past, essentially no lower bound
ORIG_UB = DP$Period_Actual_UB
ASOF_UB = DP$Period_Implied_UB
MOB_UB = -1 # set MoB condition to false, as we filter by asofdate for actual performance

set_param(ORIG_LB)
set_param(ORIG_UB)
set_param(ASOF_UB)
set_param(MOB_UB)

# COMMAND ----------

## Pull data
df = dbGetQuery(CONN, query)

setDT(df)
setnames(df, snakecase::to_snake_case)

# COMMAND ----------

## Data cleaning function
clean_df = function(df) {
  # remove NA asofdate
  df = df[!is.na(asofdate)]
  
  # replace NA with 0
  df[is.na(act_mth_co), act_mth_co := 0]
  
  # term to numeric
  df[, term := as.numeric(term)]
  
  df
}

# COMMAND ----------

df = clean_df(df)

# save cleaned data
saveRDS(df, file.path(root, CUR_STEP, 'perf.rds'))

# COMMAND ----------

### Pull data for expectation curve

## Query
# Shared query with performance data pull above

## SQL Parameters
ORIG_LB = DP$Expectation_LB
ORIG_UB = DP$Expectation_UB
ASOF_UB = as.Date('2000-01-01') # distant past -> asofdate condition = false, as we filter by MoB for expectation
MOB_UB = 60 # longest term we offer

set_param(ORIG_LB)
set_param(ORIG_UB)
set_param(ASOF_UB)
set_param(MOB_UB)

## Pull data
df = dbGetQuery(CONN, query)

setDT(df)
setnames(df, snakecase::to_snake_case)

df = clean_df(df)

# save cleaned data
saveRDS(df, file.path(root, CUR_STEP, 'exp.rds'))

# COMMAND ----------

### Pull data about originations in most recent month

## Query
# As this requires expectation only at end of loan and segmenting by only tier
# instead of various flags like the two data pulls above, we use a separate query
query = readr::read_file(file.path(root, CUR_STEP, 'latest.sql'))

## SQL Parameters
LAST_MTH = DP$Period_Actual_UB
set_param(LAST_MTH)

## Pull data
df = dbGetQuery(CONN, query)

setDT(df)
setnames(df, snakecase::to_snake_case)

# save raw data
saveRDS(df, file.path(root, CUR_STEP, 'latest_orig.rds'))

# COMMAND ----------

# DBTITLE 1,02_build_datasets
# library(here)
library(data.table)

root = getwd()
PRE_STEP = '01'
CUR_STEP = '02'

df = readRDS(file.path(root, PRE_STEP, 'perf.rds'))
exp = readRDS(file.path(root, PRE_STEP, 'exp.rds'))
orig = readRDS(file.path(root, PRE_STEP, 'latest_orig.rds'))
DP = readRDS(file.path(root, PRE_STEP, 'date_params.rds'))
setDT(df)

# source(file.path(root, CUR_STEP, 'build_fns.R'))
source('build_fns.R')

# COMMAND ----------

# NOTE: dsh_loan_portfolio_expectations creates wrong end of month sometimes
# This step makes sure they match
DP$Period_Implied_UB = max(df$asofdate)

# COMMAND ----------

### Parameters
PORT_LBS = as.Date(c('2020-01-01', '2021-07-01'))
ASOFS = c(as.Date('2021-12-31'), DP$Period_LB, DP$Period_Actual_UB)
saveRDS(PORT_LBS, file.path(root, CUR_STEP, 'PORT_LBS.rds'))
saveRDS(ASOFS, file.path(root, CUR_STEP, 'ASOFS.rds'))

# parameters for slide 2 table
S1 = c(as.Date(c('2020-12-31', '2021-12-31')), DP$Period_LB, DP$Period_Implied_UB)
S2 = c(DP$Period_LB, DP$Period_Actual_UB, DP$Period_Implied_UB)
ROW_ORDER = c(paste('Tier' , 1:5), 
              'Experimental', 'Turndown', 'LPE', 'Total')

# COMMAND ----------

### Build datasets for Core - CP applied
DF = df[segment %in% c('Tier 1', 'Tier 2','Tier 3', 'Tier 4', 'Tier 5') &
          cp_67_flag == 1 & price_flag == 1]
EXP = exp[segment %in% c('Tier 1', 'Tier 2','Tier 3', 'Tier 4', 'Tier 5') &
            cp_67_flag == 1 & price_flag == 1]

# COMMAND ----------

## Cum Act CO by Quarter/Year
RV = agg_vintages(DF[asofdate <= DP$Period_Actual_UB], 'monthonbook')

ERV = agg_df(EXP, 'monthonbook')
ERV[, ':='(act_cum_co_rate = exp_cum_co_rate, orig_grp = 'Expectation', orig_fn = 'exp')]

RV = rbindlist(list(ERV, RV), use.names = T)
saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_M.rds'))

# Cum Act CO by Quarter/Year - By Tier
RV = agg_vintages(DF[asofdate <= DP$Period_Actual_UB], c('segment', 'monthonbook'))
saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_M_tier.rds'))

# Cum Act CO by Quarter/Year - By Term
RV = agg_vintages(DF[asofdate <= DP$Period_Actual_UB], c('term', 'monthonbook'))
saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_M_term.rds'))


## Act vs Exp by Portfolio
RV = agg_portfolios(DF, PORT_LBS, 'asofdate')
aRV = agg_intervals(RV, ASOFS, c('por', 'asofdate'))

saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_A_por.rds'))
saveRDS(aRV, file.path(root, CUR_STEP, 'aC_Y_A_por.rds'))

# Act vs Exp by Portfolio - By Tier
RV = agg_portfolios(DF, PORT_LBS, c('segment', 'asofdate'))
aRV = agg_intervals(RV, ASOFS, c('segment', 'por', 'asofdate'))
bRV = agg_intervals(RV, c(DP$Period_LB, DP$Period_Actual_UB), c('segment', 'por', 'asofdate'))
names(bRV)[3] = 'mul'

saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_A_por_tier.rds'))
saveRDS(aRV, file.path(root, CUR_STEP, 'aC_Y_A_por_tier.rds'))
saveRDS(bRV, file.path(root, CUR_STEP, 'bC_Y_A_por_tier.rds'))

# Act vs Exp by Portfolio - By Term
RV = agg_portfolios(DF, PORT_LBS, c('term', 'asofdate'))
aRV = agg_intervals(RV, ASOFS, c('term', 'por', 'asofdate'))

saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_A_por_term.rds'))
saveRDS(aRV, file.path(root, CUR_STEP, 'aC_Y_A_por_term.rds'))


## Act vs Exp
RV = prep_df_AvE_plot(DF, 'asofdate')
saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_A.rds'))

# Act vs Exp - By Tier
RV = prep_df_AvE_plot(DF, c('segment', 'asofdate'))
saveRDS(RV, file.path(root, CUR_STEP, 'C_Y_A_tier.rds'))

# COMMAND ----------

### Build datasets for Core - No CP applied
DF = df[segment %in% c('Tier 1', 'Tier 2','Tier 3', 'Tier 4', 'Tier 5')]
EXP = exp[segment %in% c('Tier 1', 'Tier 2','Tier 3', 'Tier 4', 'Tier 5')]

# COMMAND ----------

## Cum Act CO by Quarter/Year
RV = agg_vintages(DF[asofdate <= DP$Period_Actual_UB], 'monthonbook')

ERV = agg_df(EXP, 'monthonbook')
ERV[, ':='(act_cum_co_rate = exp_cum_co_rate, orig_grp = 'Expectation', orig_fn = 'exp')]

RV = rbindlist(list(ERV, RV), use.names = T)
saveRDS(RV, file.path(root, CUR_STEP, 'C_N_M.rds'))


## Act vs Exp
RV = prep_df_AvE_plot(DF, 'asofdate')
saveRDS(RV, file.path(root, CUR_STEP, 'C_N_A.rds'))

# Act vs Exp - By Tier
RV = prep_df_AvE_plot(DF, c('segment', 'asofdate'))
saveRDS(RV, file.path(root, CUR_STEP, 'C_N_A_tier.rds'))

# COMMAND ----------

### Build datasets for Total Portfolio - CP applied
DF = df[cp_67_flag == 1]
EXP = exp[cp_67_flag == 1]

# COMMAND ----------

## Cum Act CO by Quarter/Year
RV = agg_vintages(DF[asofdate <= DP$Period_Actual_UB], 'monthonbook')

ERV = agg_df(EXP, 'monthonbook')
ERV[, ':='(act_cum_co_rate = exp_cum_co_rate, orig_grp = 'Expectation', orig_fn = 'exp')]

RV = rbindlist(list(ERV, RV), use.names = T)
saveRDS(RV, file.path(root, CUR_STEP, 'T_Y_M.rds'))


## Act vs Exp
RV = prep_df_AvE_plot(DF, 'asofdate')
saveRDS(RV, file.path(root, CUR_STEP, 'T_Y_A.rds'))


## For summary table on slide 2
c1RV = agg_intervals(DF, S2, c('segment', 'asofdate'))
c2RV = agg_intervals(DF, S2, 'asofdate')

names(c2RV)[1] = 'segment'
c2RV[, segment := 'Total']
cRV = rbindlist(list(c1RV, c2RV))
saveRDS(cRV, file.path(root, CUR_STEP, 'cT_Y_A.rds'))

# COMMAND ----------

### Build datasets for Total Portfolio - No CP applied
DF = df
EXP = exp

# COMMAND ----------

## Cum Act CO by Quarter/Year
RV = agg_vintages(DF[asofdate <= DP$Period_Actual_UB], 'monthonbook')

ERV = agg_df(EXP, 'monthonbook')
ERV[, ':='(act_cum_co_rate = exp_cum_co_rate, orig_grp = 'Expectation', orig_fn = 'exp')]

RV = rbindlist(list(ERV, RV), use.names = T)
saveRDS(RV, file.path(root, CUR_STEP, 'T_N_M.rds'))


## Act vs Exp
RV = prep_df_AvE_plot(DF, 'asofdate')
saveRDS(RV, file.path(root, CUR_STEP, 'T_N_A.rds'))


## For summary table on slide 2
c1RV = agg_intervals(DF, S1, c('segment', 'asofdate'))
c2RV = agg_intervals(DF, S1, 'asofdate')

names(c2RV)[1] = 'segment'
c2RV[, segment := 'Total']
cRV = rbindlist(list(c1RV, c2RV))
saveRDS(cRV, file.path(root, CUR_STEP, 'cT_N_A.rds'))


# COMMAND ----------

### Build datasets for reject populations for CP 6.5-6.6
DF = df[cp_65_66_flag == 0]

## Act vs Exp
RV = prep_df_AvE_plot(DF, 'asofdate')
saveRDS(RV, file.path(root, CUR_STEP, 'CP65_N_A.rds'))

# COMMAND ----------

### Build datasets for reject populations for CP 6.7
DF = df[cp_65_66_flag == 1 & cp_67_flag == 0]

## Act vs Exp
RV = prep_df_AvE_plot(DF, 'asofdate')
saveRDS(RV, file.path(root, CUR_STEP, 'CP67_N_A.rds'))

# COMMAND ----------

### Build summary table for slide 2
t1 = readRDS(file.path(root, CUR_STEP, 'cT_N_A.rds'))
t2 = readRDS(file.path(root, CUR_STEP, 'cT_Y_A.rds'))

# compute origination % and expected annualized default
orig[, orig_pct := loan_amt / sum(loan_amt)]
tot = orig[, lapply(.SD, sum), .SDcols = 2:5][, segment := 'Total']
orig = rbindlist(list(orig, tot), use.names = T)
orig[, aed := exp_cum_co / exp_cum_outstanding * 12]
orig = orig[, .(segment, loan_amt, orig_pct, aed)]

# join the data frames
rv = data.table(segment = Reduce(union, list(orig$segment, t1$segment, t2$segment)))
rv = Reduce(function(...) merge(..., by = 'segment', all = T), 
            list(rv, orig, t1, t2))

rv = rv[segment != 'Null'] # remove Null Tier

# rearrange rows
rv[, segment := factor(segment, levels = ROW_ORDER)]
setorder(rv, segment)

saveRDS(rv, file.path(root, CUR_STEP, 'summary.rds'))

# COMMAND ----------


