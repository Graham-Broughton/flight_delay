


def process_distance(df):
    """
    groups distance into 3 even cats based on distance: long medium and short
    also creates new columns, hour of the day, for arrival and departure
    """
    import pandas as pd
    df['dist_cats'] = pd.qcut(df['distance'], 3, labels=['short', 'medium', 'long'])
    return df

def airport_hour_avgs(df):
    """
    returns df with hourly arrival/departure averages per airport per weekday
    """
    hourly_arr_delay = df.groupby(['dest_airport_id', 'weekday', 'arr_hour'], as_index=False).agg({'arr_delay': 'mean'})
    hourly_dep_delay = df.groupby(['origin_airport_id', 'weekday', 'dep_hour'], as_index=False).agg({'dep_delay': 'mean'})
    hourly_arr_delay = hourly_arr_delay.rename(columns={'arr_delay': 'hourly_arr_delay'})
    hourly_dep_delay = hourly_dep_delay.rename(columns={'dep_delay': 'hourly_dep_delay'})
    hourly_departures = df.groupby(['origin_airport_id', 'dep_hour'], as_index=False).size()
    hourly_arrivals = df.groupby(['dest_airport_id', 'arr_hour'], as_index=False).size()
    hourly_departures = hourly_departures.rename(columns={'size': 'hourly_departures'})
    hourly_arrivals = hourly_arrivals.rename(columns={'size': 'hourly_arrivals'})
    df = df.merge(hourly_departures, on=['origin_airport_id', 'dep_hour'])
    df = df.merge(hourly_arrivals, on=['dest_airport_id', 'arr_hour'])
    df = df.merge(hourly_arr_delay, on=['dest_airport_id', 'weekday', 'arr_hour'])
    df = df.merge(hourly_dep_delay, on=['origin_airport_id', 'weekday', 'dep_hour'])
    return df

def airport_hour_medians(df):
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

def delay_early(df):
    """
    returns df with new column distance / flight time
    returns two new dfs for delayed and early flights calculated by departure delay above or below or equal to 0
    """
    df['miles_per_min'] = df['distance'] / df['actual_elapsed_time']
    delayed = df[df['dep_delay'] > 0]
    early = df[df['dep_delay'] <= 0]
    return df, delayed, early

def get_top_states(df):
    """
    creates new columns origin and dest states
    creates new df with sorted states based on amount of flights 
    filtered for states responsible for half of total, using cummulative summation
    """
    df['origin_states'] = df['origin_city_name'].str[-2:]
    df['dest_states'] = df['dest_city_name'].str[-2:]
    state_size = df.groupby(['origin_states', 'dest_states']).size().reset_index().groupby('origin_states').sum().reset_index().sort_values(by=0, ascending=False)
    state_size['cumsum'] = state_size[0].cumsum()
    state_size = state_size[state_size['cumsum'] < (len(sample2) / 2)]
    return df, state_size

def rename_brand_code(df):
    df['branded_code_share'].replace({'AA_CODESHARE': 'AA', 'AS_CODESHARE': 'AS', 'DL_CODESHARE': 'DL', 'HA_CODESHARE': 'HA', 'UA_CODESHARE': 'UA'}, inplace=True)
    return df

def remove_outliers(df):
    """
    filters for arrival delay NaN's and for outliers based on histogram
    """
    df['arr_delay'].dropna()
    df = df[df['arr_delay'] < 200]
    df = df[df['arr_delay'] > -100]
    df = df[df['crs_elapsed_time'] < 500]
    return df


def process_times(df):
    df['dep_hour'] = pd.to_datetime(df['crs_dep_time'], format='%H%M', errors='coerce').round('60min').dt.hour
    df['arr_hour'] = pd.to_datetime(df['crs_arr_time'], format='%H%M', errors='coerce').round('60min').dt.hour
    df['weekday'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.weekday
    df['month_day'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.day
    return df

def tail_num_avg(df):
    """
    returns df with new column: arrival average based on tail number
    """
    tail1 = df.groupby(['tail_num'], as_index=False).agg({'arr_delay': 'mean'})
    tail2 = df.groupby(['tail_num'], as_index=False).agg({'dep_delay': 'mean'})
    tail2 = tail2.rename(columns={'dep_delay': 'tail_dep_delay'})
    tail1 = tail1.rename(columns={'arr_delay': 'tail_arr_delay'})
    df = df.merge(tail1, on='tail_num')
    df = df.merge(tail2, on='tail_num')
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

def process_taxi_times(df):
    """
    returns new columns for avg taxi in and out based on airports, and total taxi on tail num
    """
    taxiin = df.groupby('dest_airport_id', as_index=False).agg(airport_taxi_in_mean=('taxi_in', 'mean'), airport_taxi_in_median=('taxi_in', 'median'), airport_taxi_in_std=('taxi_in', 'std'))
    taxiout = df.groupby('origin_airport_id', as_index=False).agg(airport_taxi_out_mean=('taxi_out', 'mean'), airport_taxi_out_median=('taxi_out', 'median'), airport_taxi_out_std=('taxi_out', 'std'))
    df = df.merge(taxiin, on='dest_airport_id')
    df = df.merge(taxiout, on='origin_airport_id')
    df['airport_taxi_total'] = df['airport_taxi_in_avg'] + df['airport_taxi_out_avg']
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    tailtaxi = df.groupby('tail_num', as_index=False).agg({'taxi_total': 'mean'})
    tailtaxi = tailtaxi.rename(columns={'taxi_total': 'tail_taxi_total'})
    df = df.merge(tailtaxi, on='tail_num')
    return df

def process_taxi_times_median(df):
    """
    returns new columns for avg taxi in and out based on airports, and total taxi on tail num
    """
    taxiin = df.groupby('dest_airport_id', as_index=False).agg(median_airport_taxi_in=('taxi_in', 'median'), std_airport_taxi_in=('taxi_in', 'std'))
    taxiout = df.groupby('origin_airport_id', as_index=False).agg(median_airport_taxi_out=('taxi_out', 'median'), std_airport_taxi_out=('taxi_out', 'std'))
    df = df.merge(taxiin, on='dest_airport_id')
    df = df.merge(taxiout, on='origin_airport_id')
    df['per_airport_taxi_total'] = df['airport_taxi_in_avg'] + df['airport_taxi_out_avg']
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    tailtaxi = df.groupby('tail_num', as_index=False).agg({'taxi_total': 'median'})
    tailtaxi = tailtaxi.rename(columns={'taxi_total': 'median_tail_taxi_total'})
    df = df.merge(tailtaxi, on='tail_num')
    return df

def sin_cos_time(df):
    """
    transforms hours and weekday number to sin + cos columns
    """
    df['dep_hour_sin'] = np.sin(df['dep_hour']*(2*np.pi/24))
    df['dep_hour_cos'] = np.cos(df['dep_hour']*(2*np.pi/24))
    df['arr_hour_sin'] = np.sin(df['arr_hour']*(2*np.pi/24))
    df['arr_hour_cos'] = np.cos(df['arr_hour']*(2*np.pi/24))
    df['weekday_sin'] = np.sin(df['weekday']*(2*np.pi/7))
    df['weekday_cos'] = np.cos(df['weekday']*(2*np.pi/7))
    return df

def process_carrier_id(df):
    
    carriers = df.groupby('op_unique_carrier', as_index=False).agg(carrier_dep_mean=('dep_delay', 'mean'), carrier_arr_mean=('arr_delay', 'mean'), carrier_dep_median=('dep_delay', 'median'), carrier_dep_std=('dep_delay', 'std'), carrier_arr_median=('arr_delay', 'median'), carrier_arr_std=('arr_delay', 'std'))
    df = df.merge(carriers, on='op_unique_carrier')
    return df