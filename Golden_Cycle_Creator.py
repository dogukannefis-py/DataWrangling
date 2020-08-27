#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def golden_cycle_creator(df,golden_baslangic,golden_bitis):
    
    """
        Create confidence interval for all features and one golden cycle
    """
    
    df2=df[golden_baslangic:golden_bitis].copy()
    df_grouped=df2
    df_grouped["toreduce"]="golden"
    df_grouped_2=df_grouped.groupby("toreduce").mean()
    
    
    ## %95 Confidince Interval to find upper and lower bound for each feature
    df_stats=df2.describe().T
    stats=df_stats[['count','mean','std']]

    ci95_hi = []
    ci95_lo = []

    for i in stats.index:
        c, m, s = stats.loc[i] 
        ci95_hi.append(m + 1.96*s/math.sqrt(c))
        ci95_lo.append(m - 1.96*s/math.sqrt(c))

    stats['ci95_hi'] = ci95_hi
    stats['ci95_lo'] = ci95_lo
    
    return df_grouped_2,stats[['ci95_lo']].T ,stats[['ci95_hi']].T 

