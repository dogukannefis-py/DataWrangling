#!/usr/bin/env python
# coding: utf-8

# In[1]:


def drop_by_countcycle(df,thresholdmin,thresholdmax,param):
    
    """
            Drop row respect to anomaly feature
    """
    countframe=pd.DataFrame(df[param].value_counts())
    drop=countframe.loc[(countframe[param]> thresholdmax) | (countframe[param]< thresholdmin)].index.values
    print("Previous Shape:",df.shape)
    for i in range(len(drop)):
        df = df.drop(df[df[param] == drop[i]].index)
    print("New Shape:",df.shape)
    print("\nDropped Cycles:\n",drop)

    
    return df


# In[2]:


def golden_cycle_creator(df,golden_baslangic,golden_bitis):
    
    """
        Reducing Whole row to one row
    """
    
    df2=df[golden_baslangic:golden_bitis].copy()
    df_grouped=df2
    df_grouped["toreduce"]="golden"
    df_grouped_2=df_grouped.groupby("toreduce").mean()

    return df_grouped_2 


# In[3]:


def confidence_interval(df,param,coeff=2.42):
    
    """
        Select dataframe respect to confidence interval 
    """
    
    df2=df.copy()

    df_stats=df2[param].describe().T
    stats=df_stats[['count','mean','std']]

    stats
    ci95_hi=stats['mean'] + coeff*stats['std']/math.sqrt(stats['count'])
    ci95_lo=stats['mean'] - coeff*stats['std']/math.sqrt(stats['count'])
    df6=df2.loc[(df2[param]>=ci95_lo)&(df2[param]<=ci95_hi)]
    return df6
    


# In[4]:


def drop_constant_columns(df):
    
    """
        Drop Constant and Almost Constant Columns

    """

    cols=df.columns
    counts=[[],[]]
    for c in cols:
        typ = df[c].dtypes
        uniq = len(df[c].unique())
        if uniq == 2 and typ == np.float64:
            counts[1].append(c)
        elif uniq == 1:
            counts[0].append(c)
    print('Constant Column Count: {} \nBinary Column Count: {} \n'.format(*[len(c) for c in counts]))
    print('Dropped Constant columns: ')
    print(*counts[0],sep = ", ")
    print('\nDropped Binary columns: ')  # Binary olmadigi icin silinebilir
    print(*counts[1],sep = ", ")

    df=df.drop(columns=counts[0])
    df=df.drop(columns=counts[1])
    print("\nShape: ",df.shape)
    
    return(df)


# In[5]:


def drop_corr(dataframe, corr_val,dont_drop):

    """
        Correlation Function (Drops Columns Corraleted with others More Than Indicated Threshold)
    """
    
    np.warnings.filterwarnings('ignore')
    # Creates Correlation Matrix and Instantiates
    corr_matrix = dataframe.corr()
    iters = range(len(corr_matrix.columns) - 1)
    drop_cols = []

    df2 = pd.DataFrame(columns=['Pair1', 'Pair2', 'Correlation'])
    # Iterates through Correlation Matrix Table to find correlated columns
    for i in iters:
        for j in range(i):
            item = corr_matrix.iloc[j:(j+1), (i+1):(i+2)]
            col = item.columns
            row = item.index
            val = item.values
            if abs(val) > corr_val:
                # Prints the correlated feature set and the corr val
                #print(col.values[0], "|", row.values[0], "|", round(val[0][0], 2))
                df2.loc[(i*100)+j] = [col.values[0]] + [row.values[0]] + [str(round(val[0][0], 2))]
    
    #print("Correlation Table:\n")
    #print(df2)
                
    #Create Index From DATE and TIME and create output array to not drop
    duplicates=df2.groupby(by=["Pair1"]).count().sort_values(["Pair2"],ascending=False)["Pair2"].index.values
    duplicates=np.setdiff1d(duplicates, dont_drop)
    print("\nDropped Columns:\n{}".format(str(duplicates)))
    
    #Drop one of columns more than %80 correlated
    dropped_df=dataframe.drop(columns=duplicates)

    return dropped_df


# In[6]:


def shift(df):
    
    """
        Drop some features to another dataframe to shift
        And join the shifted dataframe with main dataframe 
    """
    
    df.reset_index(level=0, inplace=True)
    cols = [c for c in df.columns if (c[:6] == param1) or (c==param2)]
    cols2 = [c for c in df.columns if (c[:6] == param1)]

    df2=df.copy()
    df_tim=df2.loc[:,cols]
    df_ara=df.drop(cols2,axis=1)
    
    df_tim[param2]=df_tim.apply(lambda row:row.ActCntCyc-1,axis=1)
    
    df_new=df_ara.merge(df_tim, on=param2, how='left')
    
    return df_new


# In[7]:


def groupby_func(df,param):
    
    df=df.groupby(param).mean()
    print("Shape of groupped dataframe: ",df.shape)
    return df


# In[8]:


def drop_time(df):
    """Select columns"""
    cols = [c for c in df.columns if (c[:6] != param1)|(c==param2)]
    df2=df[cols]
    return df2

