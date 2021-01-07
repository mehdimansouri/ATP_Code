import datetime as dt
import json
import logging
import numpy as np
import os
import pandas as pd
import requests
import time
import re

from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from typing import Any, Dict, Tuple

# Import project utils
from src.iata_covid import utils

# Create logger
log = logging.getLogger(__name__)


def scroll_down(browser):
	"""A method for scrolling the page."""

	# Get scroll height.
	prev_height = 0
	new_height = browser.execute_script("return document.body.scrollHeight")

	# Wait for page to load
	time.sleep(3)

	while new_height != prev_height:
		# Set previous height
		prev_height = new_height

		# Calculate new scroll height
		new_height = browser.execute_script('return document.getElementsByClassName("notion-scroller vertical horizontal")[0].scrollHeight')

		# Scroll to bottom of page
		browser.execute_script('document.getElementsByClassName("notion-scroller vertical horizontal")[0].scrollTo(0, %d)' % new_height)

		# Wait for page to load
		time.sleep(3)


def scrape_country_restrictions(url: str) -> pd.DataFrame:
	options = webdriver.ChromeOptions()
	options.add_argument('headless')

	# Load and pull page
	browser = webdriver.Chrome(chrome_options=options)
	browser.get(url)
	scroll_down(browser)

	# Parse HTML
	html = browser.page_source
	soup = BeautifulSoup(html, 'lxml')
	entries = soup.find_all('div', class_='notion-selectable notion-page-block notion-collection-item')

	# Pull data entries from html
	rows = []
	for e in entries:
		divs = e.find_all('div', recursive=False)

		rows.append({
			'country': divs[0].text.strip(),
			'borders': divs[1].text.strip(),
			'proposed_end_date': divs[2].text.strip(),
			'restricted_countries': divs[3].text.strip(),
			'entry_restrictions': divs[4].text.strip(),
			'entry_requirements': divs[5].text.strip(),
			'sources': divs[6].text.strip(),
			'last_edit': divs[7].text.strip(),
		})

	# Generate and return dataframe 
	data = pd.DataFrame(rows)
	return data


def fetch_restrictions_matrix(
	iom_restriction_matrix_url: str,
	restriction_mappings: Dict[str, str],
	start_date: dt.datetime=dt.datetime.now()) -> Tuple[pd.DataFrame, dt.datetime]:

	print(restriction_mappings)
	# Fetch the latest data
	date = start_date + dt.timedelta(days=1)
	print("Starting loop for date {}".format(date))
	for i in range(60):
		date = date - dt.timedelta(days=1)
		date_string = date.strftime('%Y-%m-%d')

		url = iom_restriction_matrix_url.format(date=date_string)
		resp = requests.get(url,verify=False)

		if resp.status_code == 200:
			print("Found the data for date {}".format(date_string))
			break
	# Load data
	print(url)  
	json_text = resp.text.replace('var RestrictionMatrix =', '').replace(';', '')
	matrix = json.loads(json_text)
#	print(matrix)
	country_cols = matrix.pop('ARRIVAL_ISO3')

	# Border number mapping
	border_map = {
		'0': 'Open',
		'1': 'Restricted',
		'2': 'Closed',
		'3': 'Open',
	}

	# Transform into row entries
	entries = []
	for country_destination, restrictions in matrix.items():
		for country_origin, text in zip(country_cols, restrictions):
			restrictions = text.split('-')[1].split(',')

			row = {
				'code_3_origin': country_origin,
				'code_3_destination': country_destination,
				'border': border_map[text.split('-')[0]],
				'restrictions': '\n\n'.join([restriction_mappings.get(r) for r in restrictions])
			}

			for number in restrictions:
				row[restriction_mappings[number]] = 1

			entries.append(row)

	# Load data into dataframe
	data = pd.DataFrame(entries).fillna(0)

	# Subtract a day from the date for the next time window
	date = start_date - dt.timedelta(days=1)
	print("Start date reset to {} dates".format(date))
	return data, date


def add_matrix_geographical_mappings(data: pd.DataFrame, country_mappings: pd.DataFrame) -> pd.DataFrame:
	data = data \
		.merge(
			country_mappings[['code_2', 'code_3', 'country', 'region']],
			how='left',
			left_on='code_3_origin',
			right_on='code_3') \
		.drop(['code_3', 'code_3_origin'], axis=1) \
		.rename(columns={'code_2': 'country_code_origin'}) \
		.merge(
			country_mappings[['code_2', 'code_3', 'country', 'region']],
			how='left',
			left_on='code_3_destination',
			right_on='code_3',
			suffixes=('_origin', '_destination')) \
		.drop(['code_3', 'code_3_destination'], axis=1) \
		.rename(columns={'code_2': 'country_code_destination'})

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
	data.columns = ['_'.join([word.capitalize() for word in col.split('_')]) for col in data.columns]
	return data


def capture_restrictions_matrix_changes(
	data: pd.DataFrame,
	previous: pd.DataFrame) -> pd.DataFrame:

	data = data.merge(
		previous[['Country_Code_Origin', 'Country_Code_Destination', 'Border']],
		how='left',
		on=['Country_Code_Origin', 'Country_Code_Destination'],
		suffixes=('', '_Previous'))

	return data

def add_country_geographical_mappings(data: pd.DataFrame, country_name_mappings: pd.DataFrame) -> pd.DataFrame:
	# Add country code
	joined = data.merge(country_name_mappings, how='left', on='country')
	data['country_code'] = joined.country_code

	return data


def fetch_airport_restrictions(
	icao_airport_restrictions_api_endpoint: str,
	icao_api_key: str) -> pd.DataFrame:

	# Call API and get json payload
	resp = requests.get(
		icao_airport_restrictions_api_endpoint,
		params={
			'api_key': icao_api_key,
			'states': '',
			'airports': '',
			'format': 'json'
		})
	raw = resp.json()

	# Extract messages and concatenate into separate field
	for elem in raw:
		if 'notams' in elem:
			msg_payload = json.loads(elem['notams'])['message']
			msg = '\n\n'.join(list(msg_payload.values()))
			elem['message'] = msg

	# Create dataframe with relevant fields
	cols_to_keep = ['airportCode', 'airportName', 'cityName', 'countryCode', 'countryName', 'latitude', 'longitude', 'NoTraffic', 'Closed', 'message']
	data = pd.DataFrame(raw)[cols_to_keep]

	return data


def add_airport_geographical_mappings(data: pd.DataFrame, country_mappings: pd.DataFrame) -> pd.DataFrame:
	# Add 2-letter country code
	data['countryCode_3'] = data['countryCode']
	joined = data.merge(country_mappings[['code_2', 'code_3']], how='left', left_on='countryCode_3', right_on='code_3')
	data['country_code'] = joined.code_2

	return data


def fetch_gov_response_time_series(
	github_file_path: str,
	country_mappings: pd.DataFrame,
	label_mappings: pd.DataFrame) -> pd.DataFrame:
	
	# Load latest data
	raw = pd.read_csv(github_file_path)

	# Map in 2-letter country codes
	data = raw.merge(
		country_mappings[['code_2', 'code_3']],
		how='left',
		left_on=['CountryCode'],
		right_on=['code_3'])

	data = data \
		.drop(['CountryCode'], axis=1) \
		.rename(columns={
			'code_3': 'CountryCode3',
			'code_2': 'CountryCode'
		})

	# Map labels
	for index, row in label_mappings.iterrows():
		if row['Name'] not in data.columns:
			continue

		label_col = row['Name'] + 'Label'
		if label_col not in data.columns:
			data[label_col] = None

		data.loc[data[row['Name']] == row['Value'], label_col] = row['Label']

	# Process dates
	data['Date'] = pd.to_datetime(data['Date'].astype(str))

	# Convert column names to snake case
	data.columns = [utils.to_snake_case(col) for col in data.columns]

	return data


def scrape_timatic_restrictions(url: str) -> pd.DataFrame:
	chrome_options = webdriver.ChromeOptions()
	chrome_options.add_argument('--headless')
	chrome_options.add_argument('--no-sandbox')
	chrome_options.add_argument('--disable-dev-shm-usage')	

	# Load and pull page
	# browser = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
	browser = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver', options=chrome_options)
	browser.get(url)

	# Get data
	raw = browser.execute_script('return svgMapDataGPD')

	# restriction_label
	restrictions_map = {
		1: 'Partially Restrictive',
		2: 'Totally Restrictive',
		3: 'No regulations related to Coronavirus (COVID-19) implemented',
	}


	# Process raw data	
	rows = []
	for country_code, entry in raw['values'].items():
		soup = BeautifulSoup(entry['gdp'].replace('<br/>', '\n'), 'lxml')
		rows.append([
			country_code,
			entry['gdp'],
			soup.get_text().strip(),
			entry['gdpAdjusted'],
			restrictions_map[entry['gdpAdjusted']]])
	
	# Generate and return dataframe 
	data = pd.DataFrame(data=rows, columns=['country_code', 'details_html', 'details', 'restriction_integer', 'restriction_label'])
	data.columns = ['_'.join([word.capitalize() for word in col.split('_')]) for col in data.columns]
	return data

def get_timatic_flat_file(timatic_data_path: str) -> pd.DataFrame:
	"""
	Runs the function to load and process the flat file for Timatic data
	"""
	master_list = []
	for filename in os.listdir(timatic_data_path):
		if filename.endswith(".xlsx"):
			print(filename)
			timatic_data = pd.read_excel(timatic_data_path + filename)
			master_list.append(timatic_data)

	data_timatics_flat_file = pd.concat(master_list, axis=0)

	# Filter out Null rows
	data_timatics_flat_file = data_timatics_flat_file[
		~data_timatics_flat_file['Latest Regulations'].isna()].reset_index(drop=True)

	# Filter to most recent data
	data_timatics_flat_file = data_timatics_flat_file[data_timatics_flat_file.Updated.dt.date ==
													  data_timatics_flat_file.Updated.dt.date.max()].reset_index(
		drop=True)

	data_timatics_flat_file['Latest Regulations_new'] = data_timatics_flat_file[
		'Latest Regulations'].str.replace("<br/>", '\n')
	data_timatics_flat_file['Latest Regulations_new'] = data_timatics_flat_file[
		'Latest Regulations_new'].str.replace("&#32;", " ")
	data_timatics_flat_file['Latest Regulations_new'] = data_timatics_flat_file[
		'Latest Regulations_new'].str.replace("<a href=", " ")

	data_timatics_flat_file['Latest Regulations_new'] = data_timatics_flat_file.apply(
		lambda x: re.sub('>.*</a>', '', x['Latest Regulations_new'],
						 flags=re.DOTALL), axis=1)

	data_timatics_flat_file.rename(columns={'Country Code': 'Country_Code',
											'Latest Regulations': 'Details_Html',
											'Latest Regulations_new': 'Details',
											'Country Restriction Level': 'Restriction_Label'},
								   inplace=True)
	data_timatics_flat_file[
		'Restriction_Integer'] = data_timatics_flat_file.Restriction_Label.replace(
		['Partially Restrictive',
		 'Totally Restrictive',
		 'Not Restrictive'], [1, 2, 3])

	data_timatics_flat_file = data_timatics_flat_file[['Country_Code', 'Details_Html', 'Details',
													   'Restriction_Integer', 'Restriction_Label',
													   'Updated']]
	return data_timatics_flat_file
