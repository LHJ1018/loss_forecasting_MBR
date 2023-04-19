### Starting function
agg_df = function(df, by_cols,
                  SD_cols = c('loan_amt', 'exp_mth_co', 'act_mth_co',
                              'remain_prin', 'bal_30'),
                  cnt_cols = setdiff(c('orig_mth', 'asofdate'), by_cols)) {
  
  rv = df[, lapply(.SD, sum), keyby = by_cols, .SDcols = SD_cols]
  
  rv[, ':='(mul = act_mth_co / exp_mth_co,
            dlq_30 = bal_30 / remain_prin)]
  
  cum_by_cols = if (length(by_cols) > 1) head(by_cols, -1) else NULL
  
  rv[, ':='(exp_cum_co = cumsum(exp_mth_co),
            act_cum_co = cumsum(act_mth_co),
            exp_cum_co_rate = cumsum(exp_mth_co) / loan_amt,
            act_cum_co_rate = cumsum(act_mth_co) / loan_amt)
     , by = cum_by_cols]
  
  cnt = df[, lapply(.SD, dplyr::n_distinct), keyby = by_cols, .SDcols = cnt_cols]
  cbind(rv, cnt[, ..cnt_cols])
}

# Aggregate a list of portfolios defined by origination date lower bounds
agg_portfolios = function(df, port_lbs, by_cols,
                          SD_cols = c('loan_amt', 'exp_mth_co', 'act_mth_co',
                                      'remain_prin', 'bal_30')) {
  
  rbindlist(lapply(port_lbs, function(lb) {
    rv = agg_df(df[orig_mth >= lb], by_cols, SD_cols)
    rv[, por := lb]
  }))
}

# Aggregate by list of asofdate
interval_name = function(x) {
  ub = strftime(x, '%b%y')
  lb = strftime(shift(x + 1), '%b%y')
  fifelse(is.na(lb), as.character(NA), paste0(lb, '_', ub))
}

diff_vs_prev = function(x) x - shift(x)

agg_intervals = function(df, asofs, by_cols) {
  # last element of by_cols must be "asofdate"
  rv = agg_df(df, by_cols)
  
  itv_by_cols = if (length(by_cols) > 1) head(by_cols, -1) else NULL
  rv = rv[asofdate %in% asofs
          , .(interval = interval_name(asofdate),
              mul = diff_vs_prev(act_cum_co) / diff_vs_prev(exp_cum_co))
          , keyby = itv_by_cols]
  
  rv = rv[, interval := factor(interval, levels = interval_name(asofs)[-1])]
  
  LHS = fifelse(length(itv_by_cols) > 0, paste(itv_by_cols, collapse = '+'), '.')
  
  dcast(rv[!is.na(mul)]
        , as.formula(paste0(LHS, '~', 'interval'))
        , value.var = 'mul')
}

# Actual vs Expected CO (delinquency implied forecast included)
prep_df_AvE_plot = function(df, by_cols, cutoff = DP$Period_Actual_UB) {
  df = agg_df(df, by_cols)
  df[, mul := mul - 1]
  
  rv = melt(df, id.vars = c(by_cols, 'mul'), measure.vars = c('act_mth_co', 'exp_mth_co'))
  rv[variable == 'act_mth_co' & asofdate > cutoff, variable := 'dlq_mth_co']
  
  d = rv[variable == 'act_mth_co', tail(.SD, 1), keyby = by_cols]
  d[, variable := 'dlq_mth_co']
  
  rbind(rv, d)
}

# Aggregate different levels of granularity on origination date
qtr_yr_fns = list(qtr = function(x) paste0(year(x), '_Q', quarter(x)),
                  yr = year)

agg_orig_grps = function(df, by_cols,
                         orig_fns = qtr_yr_fns,
                         SD_cols = c('loan_amt', 'exp_mth_co', 'act_mth_co',
                                     'remain_prin', 'bal_30')) {
  
  rbindlist(lapply(seq_along(orig_fns), function(i) {
    df = copy(df)
    df[, orig_grp := orig_fns[[i]](orig_mth)]
    rv = agg_df(df, c('orig_grp', by_cols), SD_cols)
    rv[, orig_fn := names(orig_fns)[i]]
  }))
}

agg_vintages = function(df, by_cols) {
  rv = agg_orig_grps(df, by_cols)
  # for quarter, only keep data points with the full 3 vintage months
  # for year, only keep data points with the full 12 vintage months
  rv[orig_mth == fcase(orig_fn == 'qtr', 3, orig_fn == 'yr', 12)]
}
