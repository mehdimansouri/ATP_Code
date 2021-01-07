import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
import time
import ssl

from pytrends.request import TrendReq
from typing import Any, Dict

# Create logger
log = logging.getLogger(__name__)

#create an ssl context
ssl._create_default_https_context = ssl._create_unverified_context


def fetch_trends(geographies: pd.DataFrame, search_term: str, search_name: str, search_category:int=0) -> pd.DataFrame:
	timeframe = '2020-01-01 %s' % dt.datetime.now().strftime('%Y-%m-%d')
	country_dfs = []
	print(timeframe)
	# Pull country + overall worldwide numbers
	request = TrendReq(hl='en-US', tz=360, timeout=(10,50),retries=4) #,requests_args={'verify':False})
	for i, row in geographies.fillna('').iterrows():
		time.sleep(5)
		# log.info((row['geoCode'], row['geoName']))
		# print(row['geo_code'],[search_term])
		request.build_payload(kw_list=[search_term], cat=search_category, timeframe=timeframe, geo=row['geo_code'], gprop='')
		country_df = request.interest_over_time()
		if len(country_df) == 0:
			next
		country_df['geoCode'] = row['geo_code']
		country_dfs.append(country_df.reset_index())

	data = pd.concat(country_dfs)
	data.date = pd.to_datetime(data.date)

	search_col_name = 'Google_' + search_name + '_Interest'
	data = data.rename(columns={search_term: search_col_name})[['date', 'geoCode', search_col_name]]

	if data.geoCode.str.len().max() <= 2:
		data.to_csv('data/03_primary/%s.csv' % ('Google_' + search_name), index=False)
	else:
		data.to_csv('data/03_primary/%s.csv' % ('Google_Region_' + search_name), index=False)

	return data


def consolidate_results(*args):
	# Merge all dataframes
	result = None
	for df in args:
		if result is None:
			result = df
		else:
			result = result.merge(df, on=['date', 'geoCode'])

	# Rename geo variables to match other datasets
	if result.geoCode.str.len().max() <= 2:
		result = result.rename(columns={'geoCode': 'country_code'})
	else:
		result = result.rename(columns={'geoCode': 'geo_code'})
	print(result.date.max())
	return result
