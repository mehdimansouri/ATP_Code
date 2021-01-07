import logging
import numpy as np
import os
import pandas as pd
import ssl

from typing import Any, Dict

# Create logger
log = logging.getLogger(__name__)

#create an ssl context
ssl._create_default_https_context = ssl._create_unverified_context

def load_and_merge_data(
	covid_cases_github_file_path: str,
	covid_deaths_github_file_path: str,
	covid_recovered_github_file_path: str) -> pd.DataFrame:

	def _time_series_helper(file_path, value_name):
		raw = pd.read_csv(file_path)
		aggregated = raw \
			.drop(['Province/State', 'Lat', 'Long'], axis=1) \
			.groupby('Country/Region') \
			.sum()

		unpivoted = aggregated \
			.reset_index() \
			.melt(id_vars=('Country/Region')) \
			.rename(columns={
				'variable': 'date',
				'value': value_name})

		unpivoted['date'] = pd.to_datetime(unpivoted['date'])
		return unpivoted

	# Load and transform COVID time series data
	cases = _time_series_helper(covid_cases_github_file_path, 'covid_cases')	
	deaths = _time_series_helper(covid_deaths_github_file_path, 'covid_deaths')
	recoveries = _time_series_helper(covid_recovered_github_file_path, 'covid_recoveries')

	# New cases
	new_dfs = {}
	for name, df in [('covid_cases', cases), ('covid_deaths', deaths)]:
		colname = name.replace('covid_', 'covid_new_')

		# Pivot data
		pivot = pd.pivot(df, values=name, index='date', columns='Country/Region') \
			.sort_index()
		
		# Subtract minus previous
		new_pivot = pivot - pivot.shift()
		new = new_pivot \
			.reset_index() \
			.melt(id_vars=('date')) \
			.rename(columns={
				'variable': 'Country/Region',
				'value': colname})

		# Fill negative numbers with Nulls
		new.loc[new[colname].fillna(0) < 0, colname] = np.nan

		new_dfs[name] = new

	# Merge data
	data = cases \
		.merge(new_dfs['covid_cases'], how='outer', on=['Country/Region', 'date']) \
		.merge(deaths, how='outer', on=['Country/Region', 'date']) \
		.merge(new_dfs['covid_deaths'], how='outer', on=['Country/Region', 'date']) \
		.merge(recoveries, how='outer', on=['Country/Region', 'date'])

	# Process dates
	data['date'] = pd.to_datetime(data['date'])

	return data


def add_geographical_mappings(
	data: pd.DataFrame,
	country_name_mappings: pd.DataFrame,
	world_demographics: pd.DataFrame) -> pd.DataFrame:
	columns = list(data.columns)

	# Add country codes
	joined = data.merge(country_name_mappings, how='left', left_on='Country/Region', right_on='COVID_country')
	data['country_code'] = joined.country_code
	data = data[['country_code'] + columns]

	# World demographics
	data = data.merge(world_demographics[['Country_Code', 'Population (2020)']], left_on=['country_code'], right_on=['Country_Code'])

	data['covid_cases_per_10k_people'] = 10000 * data['covid_cases'] / data['Population (2020)']
	data['covid_new_cases_per_100k_people'] = 100000 * data['covid_new_cases'] / data['Population (2020)']
	data['covid_new_deaths_per_1mm_people'] = 1000000 * data['covid_new_deaths'] / data['Population (2020)']

	data = data.drop(['Country_Code', 'Population (2020)'], axis=1)
	return data

