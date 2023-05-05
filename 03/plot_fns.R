library(RColorBrewer)
library(patchwork)
library(grid)
library(gridExtra)
library(gtable)

round_2 = function(x, suffix = '', num_d = 2) {
  paste0(formatC(x, digits = num_d, format = 'f'), suffix)
}

# Names used for naming scales
A_vs_E_sc_names = c('act_mth_co', 'dlq_mth_co', 'exp_mth_co')

### Parameters - Scales used in plots
# Scale for Y axis
SC_Y = lapply(list(dollar = scales::label_dollar(scale = 10e-7, suffix = 'M', accuracy = .01),
                   pct = scales::label_percent(accuracy = .01),
                   num = scales::label_number(accuracy = .01)),
              function(f) scale_y_continuous(labels = f))

# Scale for X axis
SC_X = list(every3mo = scale_x_date(date_breaks = '3 months', date_labels = '%b\n%Y'))

# Scale for legend
SC_L = list(por = function(x) strftime(x, '%b%Y+ Originations'),
            A_vs_E = setNames(c('Actual', 'DLQ Implied', 'Expected'), A_vs_E_sc_names))

# Scale for line color
SC_COL = list(vin = setNames(c(brewer.pal(9, 'Greens')[c(7, 6)],
                               brewer.pal(9, 'Blues')[c(9, 8)]),
                             QTRS),
              por = setNames(brewer.pal(9, 'Set1')[c(2, 3)], PORT_LBS),
              A_vs_E = setNames(brewer.pal(9, 'GnBu')[c(9, 9, 5)], A_vs_E_sc_names),
              dlq = setNames(c(brewer.pal(9, 'Greys')[6:4],
                               brewer.pal(9, 'Greens')[9:6],
                               brewer.pal(9, 'Blues')[9:6]),
                             YRS_QTRS))

SC_COL$loss = setNames(c(brewer.pal(9, 'Greys')[9], SC_COL$dlq),
                       c('Expectation', YRS_QTRS))

# Scale for line type
SC_TYPE = list(A_vs_E = setNames(c('solid', 'dotted', 'solid'), A_vs_E_sc_names),
               loss = setNames(c('dotted', rep('solid', length(SC_COL$dlq))),
                               c('Expectation', YRS_QTRS)))



### Basic line plot function
line_plot = function(df, x, y, line_col, line_type = line_col,
                     scale_col = rep('black', dplyr::n_distinct(df[[line_col]])), 
                     scale_type = rep('solid', length(scale_col)), 
                     scales_lab = function(x) x,
                     draw_box = T,
                     scale_x = NULL, scale_y = NULL,
                     plot_title = NULL) {
  
   p = ggplot(df) +
    aes(x = .data[[x]], y = .data[[y]],
        col = as.factor(.data[[line_col]]), linetype = as.factor(.data[[line_type]])) +
    geom_line() +
    scale_color_manual(values = scale_col, labels = scales_lab) +
    scale_linetype_manual(values = scale_type, labels = scales_lab) +
    labs(title = plot_title, col = '', linetype = '')
   
   if (length(scale_x) > 0) p = p + scale_x
   if (length(scale_y) > 0) p = p + scale_y
   if (draw_box) p = p + theme(plot.background = element_rect(color = "black", size = .6))
   
   p
}

# Cumulative loss rates plot
cum_loss_rates_plot = function(df) {
  line_plot(df,
            x = 'monthonbook', y = 'act_cum_co_rate', line_col = 'orig_grp',
            scale_col = SC_COL$loss, scale_type = SC_TYPE$loss, scale_y = SC_Y$pct) +
    guides(col = guide_legend(nrow=2))
}

# Delinquency 30 rate
dlq_30_rates_plot = function(df) {
  line_plot(df,
            x = 'monthonbook', y = 'dlq_30', line_col = 'orig_grp',
            scale_col = SC_COL$dlq, scale_y = SC_Y$pct) +
    guides(col = guide_legend(nrow=2))
}

# Go forward vintage plot
forward_vin_plot = function(df, draw_box = T) {
  # labels to show multipliers
  lab_df = df[, tail(.SD, 1), keyby = .(orig_grp)]
  
  line_plot(df,
            x = 'monthonbook', y = 'act_cum_co_rate', line_col = 'orig_grp',
            scale_col = SC_COL$vin, scale_y = SC_Y$pct,
            plot_title = 'Loss Rate by Vintage',
            draw_box = draw_box) +
    geom_text(aes(label = round_2(act_cum_co / exp_cum_co, 'x')), 
               col = 'black', vjust = 0, size = 2.3, fontface = 'bold',
               data = lab_df)
}

# Go forward portfolio plot
adf_to_tablegrob = function(df) {
  df = copy(df)
  
  n_int = length(ASOFS) - 1
  ix = seq(to = ncol(df), length.out = n_int)
  
  df[, (ix) := lapply(.SD, round_2, suffix = 'x'), .SDcols = ix]
  
  cols = matrix(rep(SC_COL$por[as.character(df$por)], each = n_int), byrow = T, ncol = n_int)
  
  g = tableGrob(
    df[, ..ix],
    theme = ttheme_minimal(core=list(fg_params = list(fontface = 'bold', fontsize = 7,
                                                      col = cols),
                                     bg_params = list(fill = 'white')),
                           colhead=list(fg_params = list(fontface = 'plain', fontsize = 7,
                                                         col = matrix('gray40', ncol = n_int)),
                                        bg_params = list(fill = 'white'))),
    rows = NULL
  )
  
  g <- gtable_add_grob(g,
                       grobs = rectGrob(gp = gpar(fill = NA, lwd = 2, col = 'gray70')),
                       t = 2, b = nrow(g), l = 1, r = ncol(g))
  
  g
}

forward_por_plot = function(df, adf, draw_box = T) {
  (line_plot(df,
            x = 'asofdate', y = 'mul', line_col = 'por',
            scale_col = SC_COL$por, scales_lab = SC_L$por,
            scale_x = SC_X$every3mo, scale_y = SC_Y$num,
            plot_title = 'Act vs Exp Multiple by Portfolio',
            draw_box = draw_box) +
    geom_vline(xintercept = ASOFS, linetype = 'dashed', col = 'gray40')) +
    inset_element(adf_to_tablegrob(adf), 
                  left = .08, bottom = .85, right = .28, top = .95)
}

# Combined go forward plot
forward_vin_por_plot = function(df_v, df_p, adf) {
  v = forward_vin_plot(df_v, F)
  p = forward_por_plot(df_p, adf, F)
  (v + p + plot_layout(widths = c(3,4))) +
    plot_annotation(theme = theme(plot.background = element_rect(color = "black", size = .6)))
}

# Actual vs Expected CO plot (delinquency implied forecast included)
act_vs_exp_w_dlq_plot = function(df) {
  # labels to show multipliers
  lab_df = df[variable %in% c('act_mth_co', 'dlq_mth_co') & asofdate >= A_vs_E_Lab_LB,
              head(.SD, 1), keyby = .(asofdate)]
  
  line_plot(df,
            x = 'asofdate', y = 'value', line_col = 'variable',
            scale_col = SC_COL$A_vs_E, scale_type = SC_TYPE$A_vs_E, scales_lab = SC_L$A_vs_E,
            scale_y = SC_Y$dollar) +
    geom_text(aes(label = scales::percent(mul, accuracy = 1)),
              col = 'black', vjust = -1, size = 2.3,
              data = lab_df)
}



# Bar plot for multiples by Tier
mul_by_tier_plot = function(df) {
  ggplot(df) +
    aes(x = segment, y = mul, fill = as.factor(por)) +
    geom_bar(stat="identity", position=position_dodge()) +
    geom_text(aes(label = round_2(mul)), 
              vjust = 1.6, color = "white",
              position = position_dodge(0.9), size = 3.5) +
    scale_fill_manual(values = SC_COL$por, labels = SC_L$por) +
    labs(fill = '') + 
    theme(plot.background = element_rect(color = "black", size = .6))
}


# Summary Table for Slide 2
summary_table = function(df) {
  # prepare data frame for table formatting
  df = copy(df)
  df[, ':='(loan_amt = loan_amt / 1e6,
            orig_pct = orig_pct * 100,
            aed = aed * 100)]
  
  # create table object
  ft = flextable(df)
  
  # format columns in $, %, and multiplier formats
  ft = colformat_double(ft, j = 2, digits = 0, prefix = '$', suffix = 'M')
  ft = colformat_double(ft, j = 3, digits = 0, suffix = '%')
  ft = colformat_double(ft, j = 4, digits = 1, suffix = '%')
  ft = colformat_double(ft, j = 5:9, digits = 1, suffix = 'x')
  
  # rename table header
  cn = c('Segment', 
         'Orig $', 
         '% of Orig $',
         'Annualized Default Exp',
         paste0(names(df)[7], '*'),
         paste0(names(df)[9], '*'))
  names(cn) = names(df)[c(1:4, 7, 9)]
  
  ft = set_header_labels(ft, values = cn)
  
  # add another header for latest month, Without CP and CP Applied
  ft = add_header_row(ft, values = c(strftime(DP$Period_Actual_UB, '%B %Y'), 
                                     'Without CP', 'CP Applied'), 
                      colwidths = c(4, 3, 2))
  
  # add borders
  ft = border_outer(ft)
  ft = border_inner(ft)
  
  # justify all texts
  ft = align(ft, align = 'center', part = 'all')
  
  # add footnote about delinquency implied numbers
  ft = add_footer_lines(ft, values = strftime(DP$Period_Actual_UB,
                                              '* Numbers beyond %b %Y are delinquency implied'))
  
  # fill in colors
  ft = bg(ft, i = 1:2, j = 1:4, bg = '#fff5a7', part = 'header')
  ft = bg(ft, i = 1:2, j = 5:7, bg = '#f0d4dc', part = 'header')
  ft = bg(ft, i = 1:2, j = 8:9, bg = '#b8d4ac', part = 'header')
  ft = bg(ft, j = 1, bg = '#c9c8c6')
  
  # bold header and last row
  ft = bold(ft, part = 'header')
  ft = bold(ft, i = nrow(df))

  ft
}
