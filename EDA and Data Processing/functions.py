


def process_distance(df):
    """
    groups distance into 3 even cats based on distance: long medium and short
    also creates a new column - hour of the day - and groups on that 
    """
    df['dep_time'] = pd.to_datetime(df['dep_time'], format='%H%M', errors='coerce') 
    df['dep_time'] = df['dep_time'].round('60min')
    df['dist_cats'] = pd.qcut(df['distance'], 3, labels=['short', 'medium', 'long'])
    df['hour'] = df['dep_time'].dt.hour
    new_df = sample2.groupby(['hour', 'dist_cats']).size().reset_index().sort_values(0, ascending=False).groupby('dist_cats')
    return new_df

def group_airports(df1, df2, column):
    """
    groups airports based on flights
    joins passengers from passenger df
    returns df with airports, passengers and flights sorted on 'passengers' or 'flights' and flights/passenger percent
    """
    df1 = df1.groupby(['origin_airport_id', 'dest_airport_id'], as_index=False).size().groupby('origin_airport_id', as_index=False).agg({'size': 'sum'}).sort_values(by='size', ascending=False)
    df1['flights'] = df1['size']
    df.drop(columns='size', inplace=True)
    df2 = df2.groupby(['origin_airport_id', 'dest_airport_id'], as_index=False).sum().groupby('origin_airport_id', as_index=False).agg({'passengers': 'sum'})
    df3 = df1.merge(df2, on='origin_airport_id', how='inner')
    df3.sort_values(by=column, ascending=False)
    df3['flights/passengers'] = df3['flights'] / df3['passengers'] * 100
    return df3

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
    state_size = df.groupby(['origin_states', 'dest_states']).size().reset_index().groupby('origin_states').sum().reset_index(as_index=False).sort_values(by=0, ascending=False)
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
    return df