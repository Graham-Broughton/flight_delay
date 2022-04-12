


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



def load_data():
    """
    loads processed data and returns it in X, y format
    """
    import pandas as pd
    df = pd.read_csv('G_PSQL_data/JAN_sample_processing.csv')
    return df

def process_times(df):
    df['dep_hour'] = pd.to_datetime(df['crs_dep_time'], format='%H%M', errors='coerce').round('60min').dt.hour
    df['arr_hour'] = pd.to_datetime(df['crs_arr_time'], format='%H%M', errors='coerce').round('60min').dt.hour
    df['weekday'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.weekday
    df['month_day'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.day
    return df

def tail_num_arr_avg(df):
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

def process_taxi_times(df):
    """
    returns new columns for avg taxi in and out based on airports, and total taxi on tail num
    """
    taxiin = df.groupby('dest_airport_id', as_index=False).agg({'taxi_in': 'mean'})
    taxiout = df.groupby('origin_airport_id', as_index=False).agg({'taxi_out': 'mean'})
    taxiout = taxiout.rename(columns={'taxi_out': 'airport_taxi_out_avg'})
    taxiin = taxiin.rename(columns={'taxi_in': 'airport_taxi_in_avg'})
    df = df.merge(taxiin, on='dest_airport_id')
    df = df.merge(taxiout, on='origin_airport_id')
    df['per_airport_taxi_total'] = df['airport_taxi_in_avg'] + df['airport_taxi_out_avg']
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    tailtaxi = df.groupby('tail_num', as_index=False).agg({'taxi_total': 'mean'})
    tailtaxi = tailtaxi.rename(columns={'taxi_total': 'tail_taxi_total'})
    df.merge(tailtaxi, on='tail_num')
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