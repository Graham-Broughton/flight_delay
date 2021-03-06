def split_num_cat(df):
    """
    return two dataframes, one containing the numerical columns, the other the categorical columns
    """
    num_cols = df[df.dtypes[df.dtypes != 'object'].index]
    cat_cols = df[df.dtypes[df.dtypes == 'object'].index]
    return num_cols, cat_cols

def process_taxi_times_median(df):
    """
    returns new columns for avg taxi in and out based on airports, and total taxi on tail num
    """
    df['per_airport_taxi_total'] = df['airport_taxi_in_avg'] + df['airport_taxi_out_avg']
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    tailtaxi = df.groupby('tail_num', as_index=False).agg({'taxi_total': 'median'})
    tailtaxi = tailtaxi.rename(columns={'taxi_total': 'median_tail_taxi_total'})
    df = df.merge(tailtaxi, on='tail_num')
    return df

def process_carrier_id(df):
    
    carriers = df.groupby('op_unique_carrier', as_index=False).agg(carrier_dep_mean=('dep_delay', 'mean'), carrier_arr_mean=('arr_delay', 'mean'), carrier_dep_median=('dep_delay', 'median'), carrier_dep_std=('dep_delay', 'std'), carrier_arr_median=('arr_delay', 'median'), carrier_arr_std=('arr_delay', 'std'))
    df = df.merge(carriers, on='op_unique_carrier')
    return df

def tail_num_median(df):
    """
    returns df with new column: arrival average based on tail number
    """
    tail = df.groupby(['tail_num'], as_index=False).agg(median_tail_dep_delay=('dep_delay', 'median'), median_tail_arr_delay=('arr_delay', 'median'), std_tail_dep_delay=('dep_delay', 'std'), std_tail_arr_delay=('arr_delay', 'std'))
    tail['std_tail_arr_delay'] = tail['std_tail_arr_delay'].fillna(tail['std_tail_arr_delay'].mean())
    tail['std_tail_dep_delay'] = tail['std_tail_dep_delay'].fillna(tail['std_tail_dep_delay'].mean())
    df = df.merge(tail, on='tail_num')
    return df

def process_airports(df):
    """
    returns df with hourly arrival/departure averages per airport per weekday
    """
    hourly_arr_delay = df.groupby(['dest_airport_id', 'weekday', 'arr_hour'], as_index=False).agg(hourly_median_arr_delay=('arr_delay', 'median'), hourly_std_arr_delay=('arr_delay', 'std'))
    hourly_arr_delay['hourly_std_arr_delay'] = hourly_arr_delay['hourly_std_arr_delay'].fillna(hourly_arr_delay['hourly_std_arr_delay'].mean())
    hourly_dep_delay = df.groupby(['origin_airport_id', 'weekday', 'dep_hour'], as_index=False).agg(hourly_median_dep_delay=('dep_delay', 'median'), hourly_std_dep_delay=('dep_delay', 'std'))
    hourly_dep_delay['hourly_std_dep_delay'] = hourly_dep_delay['hourly_std_dep_delay'].fillna(hourly_dep_delay['hourly_std_dep_delay'].mean())
    hourly_departures = df.groupby(['origin_airport_id', 'dep_hour'], as_index=False).size()
    hourly_arrivals = df.groupby(['dest_airport_id', 'arr_hour'], as_index=False).size()
    hourly_departures = hourly_departures.rename(columns={'size': 'hourly_departures'})
    hourly_arrivals = hourly_arrivals.rename(columns={'size': 'hourly_arrivals'})
    weekly_arr_delay = df.groupby(['dest_airport_id', 'weekday'], as_index=False).agg(weekly_median_arr_delay=('arr_delay', 'median'), weekly_std_arr_delay=('arr_delay', 'std'), weekly_mean_arr_delay=('arr_delay', 'mean'))
    weekly_arr_delay['weekly_std_arr_delay'] = weekly_arr_delay['weekly_std_arr_delay'].fillna(weekly_arr_delay['weekly_std_arr_delay'].mean())
    weekly_dep_delay = df.groupby(['origin_airport_id', 'weekday'], as_index=False).agg(weekly_median_dep_delay=('dep_delay', 'median'), weekly_std_dep_delay=('dep_delay', 'std'), weekly_mean_dep_delay=('dep_delay', 'mean'))
    weekly_dep_delay['weekly_std_dep_delay'] = weekly_dep_delay['weekly_std_dep_delay'].fillna(weekly_dep_delay['weekly_std_dep_delay'].mean())
    df = df.merge(weekly_arr_delay, on=['dest_airport_id', 'weekday'])
    df = df.merge(weekly_dep_delay, on=['weekday', 'origin_airport_id'])
    df = df.merge(hourly_departures, on=['origin_airport_id', 'dep_hour'])
    df = df.merge(hourly_arrivals, on=['dest_airport_id', 'arr_hour'])
    df = df.merge(hourly_arr_delay, on=['dest_airport_id', 'weekday', 'arr_hour'])
    df = df.merge(hourly_dep_delay, on=['origin_airport_id', 'weekday', 'dep_hour'])
    return df

def model_processing(df):
    import pandas as pd
    from sklearn.preprocessing import OrdinalEncoder
    dist_dummies = pd.get_dummies(df['dist_cats'])
    df = pd.concat([dist_dummies, df], axis=1)
    df['taxi_total_long'] = df['taxi_total'] * df['long']
    
    df['hourly_arr_std_dep_delay'] = df['hourly_std_arr_delay'] * df['dep_delay']
    df['carrier_arr_median_dep_delay'] = df['dep_delay'] / df['carrier_arr_median']
    df['weekly_median_dep^2'] = df['dep_delay'] / df['weekly_median_dep_delay']
    
    df['hourly_std_arr_delay_dep^2'] = df['hourly_std_arr_delay'] * df['dep_delay']^2
    df['hourly_std_dep_dep^2'] = df['dep_delay']^2 * df['hourly_std_dep_delay']
    df['hourly_dep_dep^2'] = df['dep_delay']^2 * df['hourly_dep_delay']
    
    daydums = pd.get_dummies(df['arr_hour'])
    df = pd.concat([daydums, df], axis=1)
    df['evening_dep_delay'] = df['evening'] * df['dep_delay']
    df['evening_taxi_total'] = df['taxi_total'] * df['evening']
    
    df = df.drop(columns=['taxi_total', 'dep_delay', 'wheels_on', 'wheels_off', 'evening', 'long'])
    return df
