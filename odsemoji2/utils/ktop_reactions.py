import pandas as pd


def get_ktop_reactions(df, k=20, reactions_col="reactions_", COL_PREFIX="reaction_"):
    df[reactions_col] = df[reactions_col].apply(lambda x : dict(x) )
    df2 = df[reactions_col].apply(pd.Series).add_prefix(COL_PREFIX)
    df3 = pd.concat([df, df2], axis=1).drop(reactions_col, axis=1)
    filter_col = [col for col in df3 if col.startswith(COL_PREFIX)]
    react_stat = df3[filter_col].describe().transpose().sort_values(["count"], ascending=False)
    #df_[df_["count"] >= 100] #get by threshold
    cols_to_del = react_stat[k:].index.values
    ktop_cols = react_stat[:k].index.values
    #was_rows = df3.shape[0]
    #df3 = df3.drop(cols_to_del, axis=1).dropna(axis=0, subset=ktop_cols, how='all')
    #print("{} rows droped.".format(was_rows-df3.shape[0]))
    return df3, ktop_cols, react_stat



#Usage example
#reacts, ktop_cols, react_stat = get_ktop_reactions(df.dropna(subset=["reactions_"]))
