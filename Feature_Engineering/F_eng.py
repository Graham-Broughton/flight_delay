def split_num_cat(df):
    """
    return two dataframes, one containing the numerical columns, the other the categorical columns
    """
    num_cols = df[df.dtypes[df.dtypes != 'object'].index]
    cat_cols = df[df.dtypes[df.dtypes == 'object'].index]
    return num_cols, cat_cols