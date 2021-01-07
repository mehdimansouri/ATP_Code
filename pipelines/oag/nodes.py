import logging
import numpy as np
import os
import pandas as pd

from typing import Any, Dict, Tuple

# Create logger
log = logging.getLogger(__name__)


def load_latest_date_file(oag_data_folder_path: str) -> pd.DataFrame:
    # Load latest file
    dfs = []
    for filename in os.listdir(oag_data_folder_path):
        if not filename.endswith('.csv'):
            continue

        file_path = oag_data_folder_path + '/' + filename
        dfs.append(pd.read_csv(file_path))

    data = pd.concat(dfs)
    return data


def previous_year_capacity(data: pd.DataFrame) -> pd.DataFrame:
    # Process dates
    data['DepLocalDate'] = pd.to_datetime(data['DepLocalDate'])
    data['futureDate'] = data['DepLocalDate'] + pd.DateOffset(years=1)

    # Get previous year figures
    data = data.merge(
        data.copy(),
        how='outer',
        left_on=['DepCountryCode', 'ArrCountryCode', 'DepLocalDate'],
        right_on=['DepCountryCode', 'ArrCountryCode', 'futureDate'],
        suffixes=('', '_PrevYear'))
    data.loc[pd.isnull(data['DepLocalDate']), 'DepLocalDate'] = data['futureDate_PrevYear']

    # Fill nulls with zeros for dates for which we have previous data for
    prev_cols = ['SchedFlightCount_PrevYear', 'CancellationCount_PrevYear']

    # mask = data.previousDate_PrevYear >= data.DepLocalDate.min()
    # data.loc[mask, prev_cols] = data.loc[mask, prev_cols].fillna(0)

    data[prev_cols] = data[prev_cols].fillna(0)

    # Keep and rename necessary columns
    data = data[['DepLocalDate', 'DepCountryCode', 'ArrCountryCode', 'SchedFlightCount',
                 'CancellationCount', 'SchedFlightCount_PrevYear', 'CancellationCount_PrevYear']]
    data.columns = ['date', 'country_code_destination', 'country_code_origin', 'sched_flight_count',
                    'cancellation_count', 'sched_flight_count_prev_year',
                    'cancellation_count_prev_year']

    return data


# def merge_innovata_data(
# 	data: pd.DataFrame,
# 	innovata_data: pd.DataFrame,
# 	innovata_country_mappings: pd.DataFrame) -> pd.DataFrame:

# 	# Add country code mappings
# 	innovata_data = innovata_data \
# 		.merge(
# 			innovata_country_mappings,
# 			how='left',
# 			left_on='Origin Country',
# 			right_on='country_name') \
# 		.merge(
# 			innovata_country_mappings,
# 			how='left',
# 			left_on='Destination Country',
# 			right_on='country_name',
# 			suffixes=('_origin', '_destination'))


# 	# Aggregate Innovata data
# 	innovata_data[['Flights', 'Seats']] = innovata_data[['Flights', 'Seats']].astype(float)
# 	innovata_data['Date'] = pd.to_datetime(innovata_data['Date'])

# 	innovata_agg = innovata_data \
# 		.groupby(['Date', 'country_code_origin', 'country_code_destination']) \
# 		.agg({
# 			'Flights': np.sum,
# 			'Seats': np.sum,
# 		}) \
# 		.rename(columns={
# 			'Flights': 'Flights_Innovata',
# 			'Seats': 'Seats_Innovata',
# 		}) \
# 		.reset_index()
# 	innovata_agg.columns = [col.lower() for col in innovata_agg.columns]

# 	# TODO: Remove once the data has actual dates rather than a monthly view
# 	# Adds zeroes for foward filling
# 	dates = innovata_agg[['date']].drop_duplicates().dropna()
# 	dates['dummy'] = 1
# 	ond = innovata_agg[['country_code_origin', 'country_code_destination']].drop_duplicates(
# 	).dropna()
# 	ond['dummy'] = 1

# 	idx_df = dates \
# 		.merge(ond) \
# 		.drop('dummy', axis=1)

# 	innovata_agg = innovata_agg.merge(
# 		idx_df,
# 		how='outer',
# 		on=['date', 'country_code_origin', 'country_code_destination']) \
# 		.fillna(0)

# 	# Merge datasets
# 	data = data.merge(
# 		innovata_agg,
# 		how='outer',
# 		on=['date', 'country_code_origin', 'country_code_destination'])

# 	# TODO: Remove once the data has actual dates rather than a monthly view
# 	dates = data[['date']].drop_duplicates().dropna()
# 	dates['dummy'] = 1

# 	idx_df = dates \
# 		.merge(ond) \
# 		.drop('dummy', axis=1)

# 	idx = ['country_code_origin', 'country_code_destination', 'date']
# 	cols = ['flights_innovata', 'seats_innovata']

# 	filled = idx_df.merge(
# 		data[idx + cols],
# 		how='left',
# 		on=idx)
# 	filled = filled.sort_values(idx)
# 	filled[cols] = filled[cols].fillna(method='ffill')

# 	data = data \
# 		.drop(cols, axis=1) \
# 		.merge(
# 			filled,
# 			how='outer',
# 			on=idx)

# 	# Return data
# 	return data


def merge_schedule_data(
        data: pd.DataFrame,
        schedule_data: pd.DataFrame) -> pd.DataFrame:
    # Change column names to match actual flight OAG data
    schedule_data = schedule_data.rename(columns={
        'Dep IATA Country Code': 'country_code_destination',
        'Arr IATA Country Code': 'country_code_origin',
        'Frequency': 'frequency',
        'Seats (Total)': 'seats',
        'Time series': 'date'
    })
    schedule_data.date = pd.to_datetime(schedule_data.date)

    # Merge datasets
    data = data.merge(
        schedule_data,
        how='outer',
        on=['date', 'country_code_origin', 'country_code_destination'])

    # Return data
    return data


def add_geographical_mappings(data: pd.DataFrame, country_mappings: pd.DataFrame) -> pd.DataFrame:
    # .rename(columns={
    # 	'DepCountryCode': 'country_code_destination',
    # 	'ArrCountryCode': 'country_code_origin',
    # 	'DepLocalDate': 'date',
    # 	'SchedFlightCount': 'sched_flight_count',
    # 	'CancellationCount': 'cancellation_count'}) \

    # Add mappings
    data = data \
        .merge(
        country_mappings[['code_2', 'country', 'region']],
        how='left',
        left_on='country_code_origin',
        right_on='code_2') \
        .drop(['code_2'], axis=1) \
        .merge(
        country_mappings[['code_2', 'country', 'region']],
        how='left',
        left_on='country_code_destination',
        right_on='code_2',
        suffixes=('_origin', '_destination')) \
        .drop(['code_2'], axis=1) \
 \
    # Domestic v. international
    data['travel_type'] = np.where(
        data['country_code_origin'] == data['country_code_destination'],
        'Domestic',
        np.where(
            data['region_origin'] == data['region_destination'],
            'Continental',
            'Intercontinental'
        ))

    # Capitalize columns
    data.columns = ['_'.join([word.capitalize() for word in col.split('_')]) for col in
                    data.columns]

    return data


def aggregate_data_by_orig_dest(data: pd.DataFrame) -> pd.DataFrame:
    # Aggregate data
    orig = data.groupby(['Country_Code_Origin', 'Region_Origin', 'Date', 'Travel_Type']) \
        .agg({
        'Sched_Flight_Count': np.sum,
        'Cancellation_Count': np.sum
    }) \
        .reset_index()

    dest = data.groupby(['Country_Code_Destination', 'Region_Destination', 'Date', 'Travel_Type']) \
        .agg({
        'Sched_Flight_Count': np.sum,
        'Cancellation_Count': np.sum
    }) \
        .reset_index()

    # Join data
    joined = orig \
        .merge(
        dest,
        how='outer',
        left_on=['Country_Code_Origin', 'Region_Origin', 'Date', 'Travel_Type'],
        right_on=['Country_Code_Destination', 'Region_Destination', 'Date', 'Travel_Type'],
        suffixes=('_(Origin)', '_(Destination)')) \
        .rename(columns={
        'Country_Code_Origin': 'Country_Code',
        'Region_Origin': 'Region'}) \
        .drop(['Country_Code_Destination', 'Region_Destination'], axis=1)

    # Compute totals
    joined['Sched_Flight_Count'] = joined['Sched_Flight_Count_(Origin)'] + joined[
        'Sched_Flight_Count_(Destination)']
    joined['Cancellation_Count'] = joined['Cancellation_Count_(Origin)'] + joined[
        'Cancellation_Count_(Destination)']

    # Pivot data
    pivot = pd.pivot_table(
        joined,
        index=['Country_Code', 'Region', 'Date'],
        columns=['Travel_Type'],
        values=['Sched_Flight_Count_(Origin)', 'Cancellation_Count_(Origin)'],
        aggfunc='sum'
    )

    pivot.columns = ['%s - %s' % (a, b) for a, b in pivot.columns]

    # Compute totals
    pivot['Sched_Flight_Count_(Origin)'] = pivot[
        [col for col in pivot.columns if 'Sched_Flight_Count_(Origin)' in col]].sum(axis=1)
    pivot['Cancellation_Count_(Origin)'] = pivot[
        [col for col in pivot.columns if 'Cancellation_Count_(Origin)' in col]].sum(axis=1)
    result = pivot.reset_index()

    return result
