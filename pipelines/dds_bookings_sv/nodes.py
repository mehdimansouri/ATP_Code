import logging
import numpy as np
import os
import pandas as pd

from typing import Any, Dict

# Create logger
log = logging.getLogger(__name__)


def load_data(
	historical_data: pd.DataFrame,
	deltas_folder_paths: str,
	date_field: str) -> pd.DataFrame:

	# Load and aggregate delta files
	deltas = []
	temp = None
	for filename in os.listdir(deltas_folder_paths):
		if not filename.startswith('Tickets_purchased'):
			continue

		if temp is not None:
			del temp

		file_path = deltas_folder_paths + '/' + filename
		temp = pd.read_csv(file_path, sep='\t')
		temp.columns = [col.replace('_', ' ') for col in temp.columns]
		temp = temp[(temp['Travel Date'] < '2100-01-01') & (~pd.isnull(temp['Travel Date']))]
		temp = temp[(pd.to_datetime(temp['Travel Date']) >= pd.to_datetime(temp['Purchase Date']))
			& (pd.to_datetime(temp['Travel Date']) <= (pd.to_datetime(temp['Purchase Date']) + pd.offsets.DateOffset(years=1)))]
		temp['Travel Month'] = pd.to_datetime(temp['Travel Date']).dt.strftime('%b %Y')
		aggregated = temp \
			.groupby(['Purchase Date', 'Country of Sale', 'Orig Country', 'Dest Country', 'Travel Month']) \
			.agg({'Pax': np.sum}) \
			.reset_index()

		deltas.append(aggregated)
	
	# Concatenate, aggregate and return results
	data = pd.concat([historical_data] + deltas, sort=False) \
			.groupby(['Purchase Date', 'Country of Sale', 'Orig Country', 'Dest Country', 'Travel Month']) \
			.agg({'Pax': np.sum}) \
			.reset_index()
	del historical_data

	# Concatenate and return data
	return data


def load_travel_date_data(
	historical_data: pd.DataFrame,
	deltas_folder_paths: str,
	date_field: str) -> pd.DataFrame:

	# Load and aggregate delta files
	deltas = []
	temp = None
	for filename in os.listdir(deltas_folder_paths):
		if not filename.startswith('Tickets_purchased'):
			continue

		if temp is not None:
			del temp

		file_path = deltas_folder_paths + '/' + filename
		temp = pd.read_csv(file_path, sep='\t')

		temp.columns = [col.replace('_', ' ') for col in temp.columns]
		temp = temp[(temp['Travel Date'] < '2100-01-01') & (~pd.isnull(temp['Travel Date']))]
		temp = temp[(pd.to_datetime(temp['Travel Date']) >= pd.to_datetime(temp['Purchase Date']))
			& (pd.to_datetime(temp['Travel Date']) <= (pd.to_datetime(temp['Purchase Date']) + pd.offsets.DateOffset(years=1)))]

		aggregated = temp \
			.groupby(['Travel Date', 'Country of Sale', 'Orig Country', 'Dest Country']) \
			.agg({'Pax': np.sum}) \
			.reset_index()

		deltas.append(aggregated)
	
	# Concatenate, aggregate and return results
	data = pd.concat([historical_data] + deltas, sort=False) \
			.groupby(['Travel Date', 'Country of Sale', 'Orig Country', 'Dest Country']) \
			.agg({'Pax': np.sum}) \
			.reset_index()
	del historical_data

	# Concatenate and return data
	return data


def previous_year_benchmarks(data: pd.DataFrame, data_2019: pd.DataFrame) -> pd.DataFrame:
	# Prepare dataframes
	data = data[(data['Orig Country'] != '0') & (data['Dest Country'] != '0')
		& (data['Orig Country'] != 0) & (data['Dest Country'] != 0)
		& (data['Country of Sale'] != 0) & (data['Country of Sale'] != '0')]

	data_2019 = data_2019[
		(data_2019['Orig Country'] != '0') & (data_2019['Dest Country'] != '0')
		& (data_2019['Orig Country'] != 0) & (data_2019['Dest Country'] != 0)
		& (data_2019['Country of Sale'] != 0) & (data_2019['Country of Sale'] != '0')]
	
	prev = pd.concat(
		[data_2019, data[data['Purchase Date'] > data_2019['Purchase Date'].max()]],
		sort=False,
		ignore_index=True)
	del data_2019

	# Process dates
	data['Purchase Date'] = pd.to_datetime(data['Purchase Date'])

	prev['Purchase Date'] = pd.to_datetime(prev['Purchase Date']) + pd.offsets.DateOffset(years=1)
	prev['Travel Month'] = pd.to_datetime(prev['Travel Month']) + pd.offsets.DateOffset(years=1)
	prev['Travel Month'] = prev['Travel Month'].dt.strftime('%b %Y')

	prev = prev[prev['Purchase Date'] <= data['Purchase Date'].max()]

	# Map previous year numbers
	prev.rename(columns={'Pax': 'Pax_Prev_Year'}, inplace=True)
	merged = data.merge(
		prev,
		on=['Purchase Date', 'Country of Sale', 'Orig Country', 'Dest Country', 'Travel Month'],
		how='outer')

	merged[['Pax', 'Pax_Prev_Year']] = merged[['Pax', 'Pax_Prev_Year']].fillna(0)
	merged = merged[(merged['Pax'] != 0) | (merged['Pax_Prev_Year'] != 0)]

	return merged


def previous_travel_date_year_benchmarks(data: pd.DataFrame, data_2019: pd.DataFrame) -> pd.DataFrame:
	# Prepare dataframes
	data = data[(data['Orig Country'] != '0') & (data['Dest Country'] != '0')
		& (data['Orig Country'] != 0) & (data['Dest Country'] != 0)
		& (data['Country of Sale'] != 0) & (data['Country of Sale'] != '0')]

	data_2019 = data_2019[
		(data_2019['Orig Country'] != '0') & (data_2019['Dest Country'] != '0')
		& (data_2019['Orig Country'] != 0) & (data_2019['Dest Country'] != 0)
		& (data_2019['Country of Sale'] != 0) & (data_2019['Country of Sale'] != '0')]
	
	# Generate travel date dataframe
	data_2019['Travel Month Date'] = pd.to_datetime(data_2019['Travel Month'])
	data_2019 = data_2019 \
		.groupby(['Country of Sale', 'Orig Country', 'Dest Country', 'Travel Month Date']) \
		.agg({'Pax': np.mean}) \
		.reset_index()

	date_range =pd.DataFrame({'Travel Date': pd.date_range(start='2019-01-01', end='2019-12-31').values}) 
	date_range['Travel Month Date'] = pd.to_datetime(date_range['Travel Date'].dt.strftime('%Y-%m'))
	data_2019 = data_2019.merge(date_range, on=['Travel Month Date'], how='inner') \
		.drop(['Travel Month Date'], axis=1)

	# Concatenate dataframes accordingly
	data['Travel Date'] = pd.to_datetime(data['Travel Date'])
	prev = pd.concat(
		[data_2019[data_2019['Travel Date'] < data['Travel Date'].min()], data],
		sort=False,
		ignore_index=True)
	del data_2019

	# Process dates
	data['Travel Date'] = pd.to_datetime(data['Travel Date'])
	prev['Travel Date'] = pd.to_datetime(prev['Travel Date']) + pd.offsets.DateOffset(years=1)
	prev = prev[prev['Travel Date'] <= data['Travel Date'].max()]

	# Map previous year numbers
	prev.rename(columns={'Pax': 'Pax_Prev_Year'}, inplace=True)
	merged = data.merge(
		prev,
		on=['Travel Date', 'Country of Sale', 'Orig Country', 'Dest Country'],
		how='outer')

	merged[['Pax', 'Pax_Prev_Year']] = merged[['Pax', 'Pax_Prev_Year']].fillna(0)
	merged = merged[(merged['Pax'] != 0) | (merged['Pax_Prev_Year'] != 0)]

	return merged


def add_geographical_mappings(
	data: pd.DataFrame,
	country_name_mappings: pd.DataFrame,
	country_mappings: pd.DataFrame) -> pd.DataFrame:

	country_name_mappings = country_name_mappings[['country_code', 'country_name']]
	country_mappings = country_name_mappings \
		.merge(
			country_mappings[['code_2', 'country', 'region']],
			how='left',
			left_on='country_code',
			right_on='code_2') \
		.drop(['code_2'], axis=1)

	# Origin country codes
	data = data.merge(country_mappings, how='left', left_on='Orig Country', right_on='country_name') \
		.rename(columns={
			'country_code': 'country_code_origin',
			'country': 'country_origin',
			'region': 'region_origin'
		}) \
		.drop('country_name', axis=1)

	# Destination country codes
	data = data.merge(country_mappings, how='left', left_on='Dest Country', right_on='country_name') \
		.rename(columns={
			'country_code': 'country_code_destination',
			'country': 'country_destination',
			'region': 'region_destination'
		}) \
		.drop('country_name', axis=1)
	# data['dest_country_code'] = joined.country_code

	return data


def add_features(data: pd.DataFrame) -> pd.DataFrame:
	# # Date processing
	# data[date_field] = pd.to_datetime(data[date_field])

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


def aggregate_country_bookings(data: pd.DataFrame) -> pd.DataFrame:
	data \
		.rename(columns={
			'Purchase Date': 'date',
			'Country of Sale': 'country_of_sale',
			'Travel Month': 'travel_month'},
			inplace=True) \

	result = data[['date', 'country_of_sale', 'country_code_origin', 'country_origin', 'region_origin',
		'country_code_destination', 'country_destination', 'region_destination', 'travel_type', 'travel_month',
		'Pax', 'Pax_Prev_Year']]

	return result


def aggregate_travel_date_country_bookings(data: pd.DataFrame) -> pd.DataFrame:
	data \
		.rename(columns={
			'Travel Date': 'date',
			'Country of Sale': 'country_of_sale'},
			inplace=True) \

	result = data[['date', 'country_of_sale', 'country_code_origin', 'country_origin', 'region_origin',
		'country_code_destination', 'country_destination', 'region_destination', 'travel_type',
		'Pax', 'Pax_Prev_Year']]

	return result


def merge_dds_gds_datasets(
	dds_country_bookings: pd.DataFrame,
	gds_country_searches: pd.DataFrame,
	country_mappings: pd.DataFrame) -> pd.DataFrame:

	join_cols = ['date', 'country_of_sale', 'country_code_origin', 'country_origin', 'region_origin',
		'country_code_destination', 'country_destination', 'region_destination', 'travel_type', 'travel_month']

	# if 'travel_month' in gds_country_searches:
	# 	dds_country_bookings['travel_month'] = 'August 2020'
	# 	join_cols.append('travel_month')

	data = dds_country_bookings \
		.merge(
			gds_country_searches,
			how='outer',
			on=join_cols)

	# Filter out unkown origin/ destinations
	data = data[(~pd.isnull(data.country_code_origin)) & (~pd.isnull(data.country_code_destination))]

	# Map country name for point of sale
	country_mappings = country_mappings[['code_2', 'country']].rename(columns={
		'code_2': 'country_of_sale',
		'country': 'point_of_sale'
	})
	data = data.merge(country_mappings, how='left', on=['country_of_sale'])
	data = data.rename(columns={'country_of_sale': 'country_code_of_sale'})

	# Capitalize columns
	data.columns = ['_'.join([word.capitalize() for word in col.split('_')]) for col in data.columns]

	return data
