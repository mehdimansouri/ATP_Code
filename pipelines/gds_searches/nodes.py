import logging
import numpy as np
import os
import pandas as pd

from typing import Any, Dict

# Create logger
log = logging.getLogger(__name__)


def rt_market(orig_array, dest_array):
    '''Returns an array of half alpha markets from an orig and dest array set'''
   
    market_array = np.where(orig_array < dest_array, orig_array + '-' + dest_array, dest_array + '-' + orig_array)
    return market_array


def combine_data(
	gds_search_folder_path: str,
	historical_data: pd.DataFrame,
	airport_mappings: pd.DataFrame,
	multiple_airport_cities: pd.DataFrame) -> pd.DataFrame:

	airport_mappings = airport_mappings[~pd.isnull(airport_mappings['iata_code'])]
	def _process_file(filepath):
		# Each chunk is in df format
		df_list = []

		chunksize = 5000000
		df_chunk = pd.read_csv(filepath, chunksize=chunksize)
		i = 0
		j = 0
		print(filepath)
		for chunk in df_chunk:
			chunk.dropna(inplace=True)

			# Apply country mappings
			chunk = map_countries(chunk, airport_mappings)
			chunk = handle_airport_cities(chunk, multiple_airport_cities)
			chunk['travel_month'] = pd.to_datetime(chunk['request_outbound_date'].astype(int).astype(str)).dt.strftime('%b %Y')
			print("France rows")
			print(chunk[chunk.country_code_origin=="FR"].shape)
			if 'number_of_request' in chunk:
				chunk.rename(columns={'number_of_request': 'number_of_requests'}, inplace=True)

			chunk_agg = chunk \
				.groupby(['pos', 'date_request', 'travel_month', 'country_code_origin', 'country_code_destination']) \
				.agg({'number_of_requests': np.sum}) \
				.reset_index()
			print(chunk[chunk.country_code_origin == "FR"].shape)
			i += len(chunk_agg)
			j += len(chunk)
			print(i, j)

			del chunk
			df_list.append(chunk_agg)
		
		return pd.concat(df_list, ignore_index=True)

	# Load winglet data files
	# dfs = [historical_data]
	#Starting from scratch as all historical winglet files have been revived
	dfs = []
	print(gds_search_folder_path)
	for file_name in os.listdir(gds_search_folder_path):
		if file_name.endswith('.csv') and file_name != 'winglet_historical.csv':
			file_path = gds_search_folder_path + '/' + file_name
			print(file_name,file_path)
			dfs.append(_process_file(file_path))

	# Combine and aggregate data
	data = pd.concat(dfs) \
		.groupby(['pos', 'date_request', 'travel_month', 'country_code_origin', 'country_code_destination']) \
		.agg({'number_of_requests': np.sum}) \
		.reset_index()

	# Filter out searches more than one year out
	data['date_request'] = pd.to_datetime(data.date_request.astype(str))
	data = data[(pd.to_datetime(data['travel_month']) >= (data['date_request'] - pd.to_timedelta(data['date_request'].dt.day - 1, unit='day')))
		& (pd.to_datetime(data['travel_month']) <= (data['date_request'] + pd.offsets.DateOffset(years=1)))]

	return data


def map_countries(data: pd.DataFrame, airports: pd.DataFrame) -> pd.DataFrame:
	data = data.merge(
		airports[['iata_code', 'iso_country']].rename(columns={'iso_country': 'country_code'}),
		how='left',
		left_on='request_origin',
		right_on='iata_code')

	data = data.merge(
		airports[['iata_code', 'iso_country']].rename(columns={'iso_country': 'country_code'}),
		how='left',
		left_on='request_destination',
		right_on='iata_code',
		suffixes=('_origin', '_destination'))	

	return data


def handle_airport_cities(data: pd.DataFrame, multiple_airport_cities: pd.DataFrame) -> pd.DataFrame:
	multiple_airport_cities = multiple_airport_cities.fillna('NA')[['city', 'country']]
	multiple_airport_cities.columns = ['city_temp', 'country_temp']
	
	# Update origin countries and continents accordingly
	data = data.merge(multiple_airport_cities, left_on='request_origin', right_on='city_temp', how='left')
	data['country_code_origin'] = data['country_code_origin'].fillna(data['country_temp'])
	data = data.drop([col for col in data.columns if col.endswith('_temp')], axis=1)

	# Update destination countries and continents accordingly
	data = data.merge(multiple_airport_cities, left_on='request_destination', right_on='city_temp', how='left')
	data['country_code_destination'] = data['country_code_destination'].fillna(data['country_temp'])
	data = data.drop([col for col in data.columns if col.endswith('_temp')], axis=1)

	return data


def add_features(data: pd.DataFrame, country_mappings: pd.DataFrame) -> pd.DataFrame:
	# Date processing
	print("At the stage of adding features")
	print(data[data.country_code_origin=="FR"].shape)
	data['request_date'] = pd.to_datetime(data.date_request.astype(str))

	# data['request_outbound_date'] = pd.to_datetime(data.request_outbound_date.astype(str))
	# data['travel_month'] = data['request_outbound_date'].dt.strftime('%b %Y')

	# # Days till departure
	# data['days_till_departure'] = (data['request_outbound_date'] - data['request_date']).dt.days
	# data['weeks_till_departure'] = (data['days_till_departure'] / 7).astype(int) + 1

	# # Week & month
	# data['request_outbound_week'] = data.request_outbound_date - pd.to_timedelta(data.request_outbound_date.dt.dayofweek, unit='d')
	# data['month_request'] = (data['date_request'] / 100).astype(int)

	data = data \
		.merge(
			country_mappings[['code_2', 'country', 'continent', 'region']],
			how='left',
			left_on='country_code_origin',
			right_on='code_2') \
		.drop('code_2', axis=1) \
		.merge(
			country_mappings[['code_2', 'country', 'continent', 'region']],
			how='left',
			left_on='country_code_destination',
			right_on='code_2',
			suffixes=('_origin', '_destination')) \
		.drop('code_2', axis=1)

	# Domestic v. international
	data['travel_type'] = np.where(
		data['country_code_origin'] == data['country_code_destination'],
		'Domestic',
		np.where(
			data['region_origin'] == data['region_destination'],
			'Continental',
			'Intercontinental'
		))

	return data


def aggregate_country_searches(data: pd.DataFrame, date_field: str) -> pd.DataFrame:
	aggregated = data \
		.rename(columns={
			date_field: 'date',
			'pos': 'country_of_sale'}) \
		.groupby(['date', 'country_of_sale', 'country_code_origin', 'country_origin', 'region_origin',
				'country_code_destination', 'country_destination', 'region_destination', 'travel_type', 'travel_month']) \
		.agg({'number_of_requests': 'sum'}) \
		.fillna(0)

	return aggregated.reset_index()
