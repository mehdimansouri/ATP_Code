import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
import time

from rpy2 import robjects
from rpy2.robjects import packages
from typing import Any, Dict, List

# Import project utils
from src.iata_covid import utils


# Create logger
log = logging.getLogger(__name__)


### Helper nodes ###

def before_after_helper(data, before_range, after_range, columns, index_cols):
	dtypes = data.dtypes
	def _agg_helper(colname):
		if dtypes[colname] == np.dtype('O'):
			return np.max
		else:
			return np.mean

	before = data[data.Date.between(*before_range)] \
		.fillna(0) \
		.groupby(index_cols) \
		.agg({col: _agg_helper(col) for col in columns})

	after = data[data.Date.between(*after_range)] \
		.fillna(0) \
		.groupby(index_cols) \
		.agg({col: _agg_helper(col) for col in columns})

	# Align dataframes
	after = after.join(before[[]], how='outer').sort_index()
	before = before.join(after[[]], how='outer').sort_index()
	
	# Before and after numerical columns
	after_numeric = after[[col for col in after.columns if dtypes[col] != np.dtype('O')]]
	before_numeric = before[[col for col in before.columns if dtypes[col] != np.dtype('O')]]

	# Pct change
	indexes = after_numeric.divide(before_numeric).abs() * np.where((after_numeric < 0) & ((after_numeric - before_numeric) < 0), -1, 1)
	indexes = 100 * (indexes - 1.0)
	indexes = indexes.rename(columns={col: col + '_Pct_Change' for col in columns if dtypes[col] != np.dtype('O')})

	# Deltas
	deltas = after_numeric - before_numeric
	deltas = deltas.rename(columns={col: col + '_Delta' for col in columns if dtypes[col] != np.dtype('O')})

	# Merged
	result = indexes.reset_index() \
		.merge(after.reset_index(), how='outer', on=index_cols) \
		.merge(before.reset_index(), how='outer', on=index_cols, suffixes=('', '_Prev')) \
		.merge(deltas.reset_index(), how='outer', on=index_cols)

	return result


### Main nodes ###

def identify_restrictions_changes(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
	data = data[pd.to_datetime(data.date).between(pd.to_datetime('2020-01-01'), dt.datetime.today())]
	changes_rows = []

	for country_code in data.country_code.unique():
		country_df = data[data.country_code == country_code] \
			.sort_values('date')

		previous_values = {}
		for i, row in country_df.iterrows():
			for col in columns:
				if pd.isnull(row[col]) or pd.isnull(row[col + '_Label']):
					continue
				
				if col not in previous_values:
					previous_values[col] = row[col]
					previous_values[col + '_Label'] = row[col + '_Label']
				elif row[col] != previous_values[col]:
					change_type = 'More restrictive'
					if row[col] < previous_values[col]:
						change_type = 'Less restrictive'

					changes_rows.append([country_code, row['date'], col, previous_values[col + '_Label'], row[col + '_Label'], change_type, row[col] - previous_values[col]])
					previous_values[col] = row[col]
					previous_values[col + '_Label'] = row[col + '_Label']

	result = pd.DataFrame(data=changes_rows, columns=['country_code', 'date', 'Name', 'Previous_Value', 'Current_Value', 'Change_Type', 'Change_Ordinal_Magnitude'])
	return result


def compute_market_time_series(data: pd.DataFrame) -> pd.DataFrame:
	# Capitalize columns
	data.columns = ['_'.join([word.capitalize() for word in col.split('_')]) for col in data.columns]

	# Get value columns
	potential_value_columns = [
		'Pax', 'Pax_Prev_Year', 'Number_Of_Requests',
		'Sched_Flight_Count', 'Cancellation_Count', 'Sched_Flight_Count_Prev_Year', 'Cancellation_Count_Prev_Year']
	value_columns = [col for col in potential_value_columns if col in data.columns]

	# Compute markets
	data['Market'] = utils.rt_market(data.Country_Code_Origin, data.Country_Code_Destination)
	data['Market_Name'] = np.where(
			data.Country_Code_Origin < data.Country_Code_Destination,
			data.Country_Origin + '-' + data.Country_Destination,
			data.Country_Destination + '-' + data.Country_Origin)

	# Assign order to geographical fields based on market logic
	for base_col in ('Country_Code', 'Region'):
		for i, suffix in enumerate(('_Origin', '_Destination')):
			origin = data[base_col + '_Origin']
			destination = data[base_col + '_Destination']

			data[base_col + '_1'] = np.where(data['Country_Code_Origin'] < data['Country_Code_Destination'], origin, destination)
			data[base_col + '_2'] = np.where(data['Country_Code_Origin'] >= data['Country_Code_Destination'], origin, destination)

	result = data \
		.groupby(['Date', 'Market', 'Market_Name', 'Travel_Type', 'Country_Code_1', 'Region_1', 'Country_Code_2', 'Region_2']) \
		.agg({col: np.sum for col in value_columns}) \
		.reset_index()

	return result


def consolidate_market_time_series(
	route_demand_time_series: pd.DataFrame,
	route_trips_time_series: pd.DataFrame,
	route_oag_time_series: pd.DataFrame,
	covid_time_series: pd.DataFrame,
	google_trends: pd.DataFrame,
	government_response_time_series: pd.DataFrame) -> pd.DataFrame:

	# Prepare datasets
	index_cols = ['Date', 'Market', 'Market_Name', 'Travel_Type', 'Country_Code_1', 'Region_1', 'Country_Code_2', 'Region_2']
	
	# Route market time series
	route_demand_time_series.Date = pd.to_datetime(route_demand_time_series.Date)
	route_demand_time_series = route_demand_time_series[route_demand_time_series.Date >= pd.to_datetime('2020-01-01')]
	min_date = route_demand_time_series.Date.min()
	max_date = route_demand_time_series.Date.max()

	# Covid data
	covid_time_series = covid_time_series.drop(['Country/Region', 'covid_cases', 'covid_recoveries'], axis=1)
	covid_time_series.date = pd.to_datetime(covid_time_series.date)
	covid_time_series = covid_time_series.rename(columns={'country_code': 'Country_Code', 'date': 'Date'})

	# Route trip time series
	route_trips_time_series.Date = pd.to_datetime(route_trips_time_series.Date)
	route_trips_time_series = route_trips_time_series[route_trips_time_series.Date.between(min_date, max_date)]
	route_trips_time_series = route_trips_time_series[index_cols + ['Pax', 'Pax_Prev_Year']] \
		.rename(columns={'Pax': 'Trips', 'Pax_Prev_Year': 'Trips_Prev_Year'})

	# Google trends
	google_trends.date = pd.to_datetime(google_trends.date)
	google_trends = google_trends.rename(columns={'country_code': 'Country_Code', 'date': 'Date'})

	# Government response time series
	# Fill forward null values
	restriction_cols = [
		'Containment_Health_Index_For_Display',
		'C7_Restrictions on internal movement', 'C7_Restrictions on internal movement_Label',
		'C8_International travel controls', 'C8_International travel controls_Label']

	government_response_time_series = government_response_time_series.sort_values(['Country_Name', 'Date'])
	government_response_time_series[restriction_cols] = government_response_time_series \
		.groupby('Country_Name')[restriction_cols] \
		.ffill()

	government_response_time_series = government_response_time_series[['Country_Code', 'Date'] + restriction_cols] \
		.rename(columns={'Containment_Health_Index_For_Display': 'Containment_Health_Index'})
	government_response_time_series.Date = pd.to_datetime(government_response_time_series.Date)

	# Route OAG time series
	route_oag_time_series.Date = pd.to_datetime(route_oag_time_series.Date)
	route_oag_time_series = route_oag_time_series[route_oag_time_series.Date.between(min_date, max_date)]
	route_oag_time_series = route_oag_time_series[index_cols
		+ ['Sched_Flight_Count', 'Cancellation_Count', 'Sched_Flight_Count_Prev_Year', 'Cancellation_Count_Prev_Year']]

	# Merge datasets
	data = route_demand_time_series \
		.merge(route_trips_time_series, how='outer', on=index_cols) \
		.merge(route_oag_time_series, how='outer', on=index_cols) \
		.merge(covid_time_series.rename(columns={'Country_Code': 'Country_Code_1'}), how='left', on=['Country_Code_1', 'Date']) \
		.merge(covid_time_series.rename(columns={'Country_Code': 'Country_Code_2'}), how='left', on=['Country_Code_2', 'Date'], suffixes=('_1', '_2')) \
		.merge(google_trends.rename(columns={'Country_Code': 'Country_Code_1'}), how='left', on=['Country_Code_1', 'Date']) \
		.merge(google_trends.rename(columns={'Country_Code': 'Country_Code_2'}), how='left', on=['Country_Code_2', 'Date'], suffixes=('_1', '_2')) \
		.merge(government_response_time_series.rename(columns={'Country_Code': 'Country_Code_1'}), how='left', on=['Country_Code_1', 'Date']) \
		.merge(government_response_time_series.rename(columns={'Country_Code': 'Country_Code_2'}), how='left', on=['Country_Code_2', 'Date'], suffixes=('_1', '_2'))

	# Capitalize columns
	data.columns = ['_'.join([word.capitalize() for word in col.split('_')]) for col in data.columns]

	return data


def compute_market_scorecard(data: pd.DataFrame) -> pd.DataFrame:
	# Process date
	data.Date = pd.to_datetime(data.Date)

	# Index columns
	index_cols = ['Market', 'Market_Name', 'Travel_Type', 'Country_Code_1', 'Region_1', 'Country_Code_2', 'Region_2']

	# Compute date ranges
	# cols = data.columns.values.tolist()
	# columns = data.columns[cols.index('covid_cases'): cols.index('country')]
	# data[columns] = data[columns].replace('', np.nan).astype(float)

	latest_date = pd.to_datetime(data.loc[(~pd.isnull(data['Google_Coronavirus_Interest_1'])) & (~pd.isnull(data['Pax'])), 'Date'].max())
	latest_range = [latest_date - pd.Timedelta(days=6), latest_date]

	# Restriction columns
	restriction_cols = [
		'Containment_Health_Index_1',
		'C7_Restrictions on internal movement_1', 'C7_Restrictions on internal movement_Label_1',
		'C8_International travel controls_1', 'C8_International travel controls_Label_1',
		'Containment_Health_Index_2', 'C7_Restrictions on internal movement_2',
		'C7_Restrictions on internal movement_Label_2',
		'C8_International travel controls_2', 'C8_International travel controls_Label_2',]

	# Before and after comparison (pre-crisis)
	restriction_scorecard = before_after_helper(
		data,
		[dt.datetime.strptime('2020-01-19', '%Y-%m-%d'), dt.datetime.strptime('2020-01-19', '%Y-%m-%d')],
		[latest_date, latest_date],
		restriction_cols,
		index_cols)
	restriction_scorecard['Indexed_on'] = 'Pre-crisis (Third week of 2020)'

	# Before and after comparison (WoW)
	restriction_wow_scorecard = before_after_helper(
		data,
		[latest_date - pd.Timedelta(days=7), latest_date - pd.Timedelta(days=7)],
		[latest_date, latest_date],
		restriction_cols,
		index_cols)
	restriction_wow_scorecard['Indexed_on'] = 'Previous week'

	# Combine indexes
	restriction_scorecard = pd.concat([restriction_scorecard, restriction_wow_scorecard], ignore_index=True)

	# Non-restriction columns
	before_and_after_cols = [
		'Pax',
		'Pax_Prev_Year',
		'Number_Of_Requests',
		'Trips',
		'Trips_Prev_Year',
		'Sched_Flight_Count',
		'Cancellation_Count',
		'Sched_Flight_Count_Prev_Year',
		'Cancellation_Count_Prev_Year',
		'Covid_New_Cases_1',
		'Covid_New_Cases_Per_100k_People_1',
		'Covid_New_Deaths_1',
		'Covid_New_Deaths_Per_1mm_People_1',
		'Covid_New_Cases_2',
		'Covid_New_Cases_Per_100k_People_2',
		'Covid_New_Deaths_2',
		'Covid_New_Deaths_Per_1mm_People_2',
		'Google_Coronavirus_Interest_1',
		# 'Google_Coronavirus_Travel_Interest_1',
		'Google_Maps_Interest_1',
		'Google_Travel_Interest_1',
		# 'Google_Lodging_Interest_1',
		'Google_Flight_Interest_1',
		'Google_Coronavirus_Interest_2',
		# 'Google_Coronavirus_Travel_Interest_2',
		'Google_Maps_Interest_2',
		'Google_Travel_Interest_2',
		# 'Google_Lodging_Interest_2',
		'Google_Flight_Interest_2',
	]

	# Before and after comparison (pre-crisis)
	scorecard = before_after_helper(
		data,
		[dt.datetime.strptime('2020-01-13', '%Y-%m-%d'), dt.datetime.strptime('2020-01-19', '%Y-%m-%d')],
		latest_range,
		before_and_after_cols,
		index_cols)
	scorecard['Indexed_on'] = 'Pre-crisis (Third week of 2020)'

	# Before and after comparison (WoW)
	wow_scorecard = before_after_helper(
		data,
		[(d - pd.Timedelta(days=7)) for d in latest_range],
		latest_range,
		before_and_after_cols,
		index_cols)
	wow_scorecard['Indexed_on'] = 'Previous week'

	# Combine indexes
	scorecard = pd.concat([scorecard, wow_scorecard], ignore_index=True)

	# Merge datasets
	result = scorecard \
		.merge(restriction_scorecard, on=index_cols + ['Indexed_on'])

	# Set infinity values to null
	result = result.replace([np.inf, -np.inf], np.nan)

	# Replace underscores with spaces
	result.columns = [col.replace('_', ' ') for col in result.columns]

	return result

