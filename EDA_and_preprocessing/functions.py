


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
    airports = df.groupby('dest_airport_id', as_index=False).agg(late=('late_binary', 'sum'), total=('late_binary', 'size'))
    airports['total_percent_late'] = (airports['late'] / airports['total']) * 100
    airports.drop(columns=['total', 'late'], inplace=True)
    airport_weekly_hourly_arr = df.groupby(['dest_airport_id', 'weekday', 'arr_hour'], as_index=False).agg(airport_weekly_hourly_arr=('arr_delay', 'mean'))
    airport_weekly_hourly_dep = df.groupby(['origin_airport_id', 'weekday', 'dep_hour'], as_index=False).agg(airport_weekly_hourly_dep=('dep_delay', 'mean'))
    airport_hourly_late_arr = df.groupby(['dest_airport_id', 'arr_hour'], as_index=False).agg(late=('late_binary', 'sum'), total=('late_binary', 'size'))
    airport_hourly_late_arr['airport_arr_hourly_late_percent'] = airport_hourly_late_arr['late'] / airport_hourly_late_arr['total'] * 100
    airport_hourly_late_dep = df.groupby(['origin_airport_id', 'dep_hour'], as_index=False).agg(late=('late_binary', 'sum'), total=('late_binary', 'size'))
    airport_hourly_late_dep['airport_dep_hourly_late_percent'] = airport_hourly_late_dep['late'] /airport_hourly_late_dep['total'] * 100
    airport_taxi_arr = df.groupby('dest_airport_id', as_index=False).agg(arr_taxi=('taxi_in', 'mean'))
    airport_taxi_dep = df.groupby('origin_airport_id', as_index=False).agg(dep_taxi=('taxi_out', 'mean'))
    airport_hourly_late_arr.drop(columns=['total', 'late'], inplace=True)
    airport_hourly_late_dep.drop(columns=['total', 'late'], inplace=True)
    df = df.merge(airport_weekly_hourly_arr, on=['dest_airport_id', 'weekday', 'arr_hour'])
    df = df.merge(airport_weekly_hourly_dep, on=['origin_airport_id', 'weekday', 'dep_hour'])
    df = df.merge(airport_hourly_late_arr, on=['dest_airport_id', 'arr_hour'])
    df = df.merge(airport_hourly_late_dep, on=['origin_airport_id', 'dep_hour'])
    df = df.merge(airport_taxi_arr, on=['dest_airport_id'])
    df = df.merge(airport_taxi_dep, on=['origin_airport_id'])
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
    df = df[df['arr_delay'] < 100]
    df = df[df['arr_delay'] > -50]
    df = df[df['crs_elapsed_time'] < 500]
    return df


def process_times(df):
    import pandas as pd
    df['dep_hour'] = pd.to_datetime(df['crs_dep_time'], format='%H%M', errors='coerce').round('60min').dt.hour
    df['arr_hour'] = pd.to_datetime(df['crs_arr_time'], format='%H%M', errors='coerce').round('60min').dt.hour
    df['weekday'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.weekday
    df['month_day'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.day
    return df

def tail_num_avg(df):
    """
    returns df with new column: arrival average based on tail number
    """
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    tail = df.groupby(['tail_num'], as_index=False).agg(tail_arr=('arr_delay', 'mean'), tail_dep=('dep_delay', 'mean'), tail_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), total=('late_binary', 'size'), reduce=('reduced_delay_flight', 'sum'), reduced_total=('reduced_delay_flight', 'size'))
    tail['tail_precent_late'] = tail['late'] / tail['total'] * 100
    tail['tail_percent_reduce_delay'] = tail['reduce'] / tail['reduced_total'] * 100
    tail = tail.drop(columns=['late', 'total', 'reduce', 'reduced_total'])
    df = df.merge(tail, on='tail_num')
    return df

def process_carriers(df):
    """
    returns new columns for avg taxi in and out based on airports, and total taxi on tail num
    """
    carriers = df.groupby('op_unique_carrier', as_index=False).agg(carrier_arr=('arr_delay', 'mean'), carrier_dep=('dep_delay', 'mean'), carrier_taxiin=('taxi_in', 'mean'), carrier_taxiout=('taxi_out', 'mean'), carrier_late=('late_binary', 'sum'), carrier_total=('late_binary', 'size'), carrier_reduce_delay=('reduced_delay_flight', 'sum'), reduced_total=('reduced_delay_flight', 'size'))
    carriers['carrier_percent_late'] = carriers['carrier_late'] / carriers['carrier_total'] * 100
    carriers['carrier_taxi_total'] = carriers['carrier_taxiin'] + carriers['carrier_taxiout']
    carriers['percent_carrier_delay_reduced'] = carriers['carrier_reduce_delay'] / carriers['reduced_total'] * 100
    carriers = carriers.drop(columns=['carrier_reduce_delay', 'reduced_total', 'carrier_taxiout', 'carrier_taxiin', 'carrier_total', 'carrier_late'])
    df = df.merge(carriers, on='op_unique_carrier')
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

def process_binary(df):
    """
    returns df with new binary columns late or not and able to reduce delay or not
    """
    df['late_binary'] = 0
    late = df['arr_delay'] > 0
    df.loc[late, 'late_binary'] = 1
    reduced_delay_flight = df['arr_delay'] < df['dep_delay']
    df['reduced_delay_flight'] = 0
    df.loc[reduced_delay_flight, 'reduced_delay_flight'] = 1
    return df

def process_weekly(df):
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    weekday = df.groupby('weekday', as_index=False).agg(weekday_arr=('arr_delay', 'mean'), weekday_dep=('dep_delay', 'mean'), weekday_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), tot=('late_binary', 'size'), red=('reduced_delay_flight', 'sum'), rtot=('reduced_delay_flight', 'size'))
    weekday['weekly_percent_late'] = weekday['late'] / weekday['tot'] * 100
    weekday['weekly_reduced_delay'] = weekday['red'] / weekday['rtot'] * 100
    weekday = weekday.drop(columns=['red', 'late', 'rtot', 'tot'])

    df = df.merge(weekday, on='weekday')
    return df

def process_hourly(df):
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    arr_hourly = df.groupby('arr_hour', as_index=False).agg(arr_hourly_arr=('arr_delay', 'mean'), arr_hourly_dep=('dep_delay', 'mean'), arr_hour_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), ltot=('late_binary', 'size'), reduce=('reduced_delay_flight', 'sum'), rtot=('reduced_delay_flight', 'size'))
    arr_hourly['arr_hourly_percent_late'] = arr_hourly['late'] / arr_hourly['ltot'] * 100
    arr_hourly['arr_hourly_percent_reduced_delay'] = arr_hourly['reduce'] / arr_hourly['rtot'] * 100
    arr_hourly = arr_hourly.drop(columns=['late', 'ltot', 'reduce', 'rtot'])
    
    dep_hourly = df.groupby('dep_hour', as_index=False).agg(dep_hourly_arr=('arr_delay', 'mean'), dep_hourly_dep=('dep_delay', 'mean'), dep_hourly_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), tot=('late_binary', 'size'), red=('reduced_delay_flight', 'sum'), rtot=('reduced_delay_flight', 'size'))
    dep_hourly['dep_hourly_percent_late'] = dep_hourly['late'] / dep_hourly['tot'] * 100
    dep_hourly['dep_hourly_percent_reduced'] = dep_hourly['red'] / dep_hourly['rtot'] * 100
    dep_hourly = dep_hourly.drop(columns=['late', 'red', 'tot', 'rtot'])
    
    df.merge(arr_hourly, on='arr_hour')
    df.merge(dep_hourly, on='dep_hour')
    return df