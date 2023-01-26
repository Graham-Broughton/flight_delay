def airport_combine_test(df, df2):
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
    airport_taxi_arr = df.groupby('dest_airport_id', as_index=False).agg(airport_taxi_in=('taxi_in', 'mean'))
    airport_taxi_dep = df.groupby('origin_airport_id', as_index=False).agg(airport_taxi_out=('taxi_out', 'mean'))
    airport_hourly_late_arr.drop(columns=['total', 'late'], inplace=True)
    airport_hourly_late_dep.drop(columns=['total', 'late'], inplace=True)
    df2 = df2.merge(airport_weekly_hourly_arr, on=['dest_airport_id', 'weekday', 'arr_hour'], how='left')
    df2 = df2.merge(airport_weekly_hourly_dep, on=['origin_airport_id', 'weekday', 'dep_hour'], how='left')
    df2 = df2.merge(airport_hourly_late_arr, on=['dest_airport_id', 'arr_hour'], how='left')
    df2 = df2.merge(airport_hourly_late_dep, on=['origin_airport_id', 'dep_hour'], how='left')
    df2 = df2.merge(airport_taxi_arr, on=['dest_airport_id'], how='left')
    df2 = df2.merge(airport_taxi_dep, on=['origin_airport_id'], how='left')
    return df2

def combine_routes(df, df2):
    tail_route_times = df.groupby(['tail_num', 'origin_airport_id', 'dest_airport_id'], as_index=False).agg(tail_route_arr_mean=('arr_delay', 'mean'), tail_route_dep_mean=('dep_delay', 'mean'), tail_route_actual=('actual_elapsed_time', 'mean'), tail_route_pred=('crs_elapsed_time', 'mean'))
    tail_route_times['tail_route_air_ratio'] = tail_route_times['tail_route_actual'] / tail_route_times['tail_route_pred'] * 100
    tail_route_times = tail_route_times.drop(columns=['tail_route_actual', 'tail_route_pred'])
    carrier_route_times = df.groupby(['op_unique_carrier', 'origin_airport_id', 'dest_airport_id'], as_index=False).agg(carrier_route_arr_mean=('arr_delay', 'mean'), carrier_route_dep_mean=('dep_delay', 'mean'), carrier_route_air_actual=('actual_elapsed_time', 'mean'), carrier_route_air_pred=('crs_elapsed_time', 'mean'))
    carrier_route_times['carrier_route_air_percent'] = carrier_route_times['carrier_route_air_actual'] / carrier_route_times['carrier_route_air_pred'] * 100
    carrier_route_times = carrier_route_times.drop(columns=['carrier_route_air_pred', 'carrier_route_air_actual'])
    routes = df.groupby(['origin_airport_id', 'dest_airport_id'], as_index=False).agg(route_arr_mean=('arr_delay', 'mean'), route_dep_mean=('dep_delay', 'mean'))
    df2 = df2.merge(routes, on=['origin_airport_id', 'dest_airport_id'], how='left')
    df2 = df2.merge(carrier_route_times, on=['op_unique_carrier', 'origin_airport_id', 'dest_airport_id'], how='left')
    df2 = df2.merge(tail_route_times, on=['tail_num', 'origin_airport_id', 'dest_airport_id'], how='left')
    return df2

def combine_hourly(df, df2):
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    arr_hourly = df.groupby('arr_hour', as_index=False).agg(arr_hourly_arr=('arr_delay', 'mean'), arr_hourly_dep=('dep_delay', 'mean'), arr_hour_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), ltot=('late_binary', 'size'), reduce=('reduced_delay_flight', 'sum'), rtot=('reduced_delay_flight', 'size'))
    arr_hourly['arr_hourly_percent_late'] = arr_hourly['late'] / arr_hourly['ltot'] * 100
    arr_hourly['arr_hourly_percent_reduced_delay'] = arr_hourly['reduce'] / arr_hourly['rtot'] * 100
    arr_hourly = arr_hourly.drop(columns=['late', 'ltot', 'reduce', 'rtot'])
    dep_hourly = df.groupby('dep_hour', as_index=False).agg(dep_hourly_arr=('arr_delay', 'mean'), dep_hourly_dep=('dep_delay', 'mean'), dep_hourly_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), tot=('late_binary', 'size'), red=('reduced_delay_flight', 'sum'), rtot=('reduced_delay_flight', 'size'))
    dep_hourly['dep_hourly_percent_late'] = dep_hourly['late'] / dep_hourly['tot'] * 100
    dep_hourly['dep_hourly_percent_reduced'] = dep_hourly['red'] / dep_hourly['rtot'] * 100
    dep_hourly = dep_hourly.drop(columns=['late', 'red', 'tot', 'rtot'])
    df2 = df2.merge(arr_hourly, on='arr_hour', how='left')
    df2 = df2.merge(dep_hourly, on='dep_hour', how='left')
    return df2

def combine_weekly(df, df2):
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    weekday = df.groupby('weekday', as_index=False).agg(weekday_arr=('arr_delay', 'mean'), weekday_dep=('dep_delay', 'mean'), weekday_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), tot=('late_binary', 'size'), red=('reduced_delay_flight', 'sum'), rtot=('reduced_delay_flight', 'size'))
    weekday['weekly_percent_late'] = weekday['late'] / weekday['tot'] * 100
    weekday['weekly_reduced_delay'] = weekday['red'] / weekday['rtot'] * 100
    weekday = weekday.drop(columns=['red', 'late', 'rtot', 'tot'])
    df2 = df2.merge(weekday, on='weekday', how='left')
    return df2

def combine_carriers(df, df2):
    """
    returns new columns for avg taxi in and out based on airports, and total taxi on tail num
    """
    carriers = df.groupby('op_unique_carrier', as_index=False).agg(carrier_arr=('arr_delay', 'mean'), carrier_dep=('dep_delay', 'mean'), carrier_taxiin=('taxi_in', 'mean'), carrier_taxiout=('taxi_out', 'mean'), carrier_late=('late_binary', 'sum'), carrier_total=('late_binary', 'size'), carrier_reduce_delay=('reduced_delay_flight', 'sum'), reduced_total=('reduced_delay_flight', 'size'))
    carriers['carrier_percent_late'] = carriers['carrier_late'] / carriers['carrier_total'] * 100
    carriers['carrier_taxi_total'] = carriers['carrier_taxiin'] + carriers['carrier_taxiout']
    carriers['percent_carrier_delay_reduced'] = carriers['carrier_reduce_delay'] / carriers['reduced_total'] * 100
    carriers = carriers.drop(columns=['carrier_reduce_delay', 'reduced_total', 'carrier_taxiout', 'carrier_taxiin', 'carrier_total', 'carrier_late'])
    df2 = df2.merge(carriers, on='op_unique_carrier', how='left')
    return df2

def combine_tail_num_avg(df, df2):
    """
    returns df with new column: arrival average based on tail number
    """
    df['taxi_total'] = df['taxi_in'] + df['taxi_out']
    tail = df.groupby(['tail_num'], as_index=False).agg(tail_arr=('arr_delay', 'mean'), tail_dep=('dep_delay', 'mean'), tail_taxi=('taxi_total', 'mean'), late=('late_binary', 'sum'), total=('late_binary', 'size'), reduce=('reduced_delay_flight', 'sum'), reduced_total=('reduced_delay_flight', 'size'))
    tail['tail_precent_late'] = tail['late'] / tail['total'] * 100
    tail['tail_percent_reduce_delay'] = tail['reduce'] / tail['reduced_total'] * 100
    tail = tail.drop(columns=['late', 'total', 'reduce', 'reduced_total'])
    df2 = df2.merge(tail, on='tail_num', how='left')
    return df2

def process_times2(df):
    import pandas as pd
    try:
        df['weekday'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.weekday
        df['month_day'] = pd.to_datetime(df['fl_date'], format='%Y-%m-%d', errors='coerce').dt.day
        df['dep_hour'] = pd.to_datetime(df['crs_dep_time'], format='%H%M', errors='coerce').round('60min').dt.hour
        df['arr_hour'] = pd.to_datetime(df['crs_arr_time'], format='%H%M', errors='coerce').round('60min').dt.hour
    except KeyError:
        pass
    finally:
        return df