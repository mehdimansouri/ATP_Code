import datetime as dt
import logging
import numpy as np
import pandas as pd
import time
import json
import requests
import os
import sys

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
from datetime import datetime

# Create logger
log = logging.getLogger(__name__)


def get_employment_data(url: str, country_emp_excl: dict) -> pd.DataFrame:
    """
    Extracts data and does pre-processing
    """
    r = requests.get(url)
    content = r.json()

    colnames = [val['name'] for val in content['structure']['dimensions']['series']] + \
               [content['structure']['dimensions']['observation'][0]['name']]

    pos1_id = pd.DataFrame(content['structure']['dimensions']['series'][0]['values']).reset_index()
    pos2_id = pd.DataFrame(content['structure']['dimensions']['series'][1]['values']).reset_index()
    pos3_id = pd.DataFrame(content['structure']['dimensions']['series'][2]['values']).reset_index()
    pos4_id = pd.DataFrame(content['structure']['dimensions']['series'][3]['values']).reset_index()
    pos5_id = pd.DataFrame(
        content['structure']['dimensions']['observation'][0]['values']).reset_index()

    user_dict = content['dataSets'][0]['series']
    rows = []
    for i in user_dict.keys():
        for j in user_dict[i]['observations'].keys():
            row = i.split(':')
            row.extend([j, user_dict[i]['observations'][j][0]])
            rows.append(row)
    data = pd.DataFrame(rows, columns=['pos1', 'pos2', 'pos3', 'pos4', 'pos5', 'value'])

    cols = [col for col in data.columns if col.startswith("pos")]
    data[cols] = data[cols].apply(pd.to_numeric, errors='coerce')

    # Merge with pos 1
    data = pd.merge(left=data,
                    right=pos1_id[['index', 'name']],
                    left_on=['pos1'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[0]}).drop(columns={'index'})

    # Merge with pos 2
    data = pd.merge(left=data,
                    right=pos2_id[['index', 'name']],
                    left_on=['pos2'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[1]}).drop(columns={'index'})

    # Merge with pos 3
    data = pd.merge(left=data,
                    right=pos3_id[['index', 'name']],
                    left_on=['pos3'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[2]}).drop(columns={'index'})

    # Merge with pos 4
    data = pd.merge(left=data,
                    right=pos4_id[['index', 'name']],
                    left_on=['pos4'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[3]}).drop(columns={'index'})

    # Merge with pos 5
    data = pd.merge(left=data,
                    right=pos5_id[['index', 'name']],
                    left_on=['pos5'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[4]}).drop(columns={'index'})

    data.drop(columns=['pos1', 'pos2', 'pos3', 'pos4', 'pos5'], inplace=True)
    # Filter out aggregated countries and only the metrics that are relevant
    data = data[(~data['Country'].isin(country_emp_excl))].reset_index(drop=True)

    print(data.shape)

    return data


def get_cli_data(url: str, country_cli_excl: dict) -> pd.DataFrame:
    """
    Extracts data and does pre-processing
    """
    r = requests.get(url)
    content = r.json()

    colnames = [val['name'] for val in content['structure']['dimensions']['series']] + \
               [content['structure']['dimensions']['observation'][0]['name']]

    pos1_id = pd.DataFrame(content['structure']['dimensions']['series'][0]['values']).reset_index()
    pos2_id = pd.DataFrame(content['structure']['dimensions']['series'][1]['values']).reset_index()
    pos3_id = pd.DataFrame(content['structure']['dimensions']['series'][2]['values']).reset_index()
    series_id = pd.DataFrame(
        content['structure']['dimensions']['observation'][0]['values']).reset_index()

    user_dict = content['dataSets'][0]['series']
    rows = []
    for i in user_dict.keys():
        for j in user_dict[i]['observations'].keys():
            row = i.split(':')
            row.extend([j, user_dict[i]['observations'][j][0]])
            rows.append(row)
    data = pd.DataFrame(rows, columns=['pos1', 'pos2', 'pos3', 'series', 'value'])

    cols = [col for col in data.columns if col.startswith("pos")] + ['series']
    data[cols] = data[cols].apply(pd.to_numeric, errors='coerce')

    # Merge with pos 1
    data = pd.merge(left=data,
                    right=pos1_id[['index', 'name']],
                    left_on=['pos1'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[0]}).drop(columns={'index'})

    # Merge with pos 2
    data = pd.merge(left=data,
                    right=pos2_id[['index', 'name']],
                    left_on=['pos2'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[1]}).drop(columns={'index'})

    # Merge with pos 3
    data = pd.merge(left=data,
                    right=pos3_id[['index', 'name']],
                    left_on=['pos3'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[2]}).drop(columns={'index'})

    # Merge with series id
    data = pd.merge(left=data,
                    right=series_id[['index', 'name']],
                    left_on=['series'],
                    right_on=['index'],
                    how="left").rename(columns={'name': colnames[3]}).drop(columns={'index'})

    data.drop(columns=['pos1', 'pos2', 'pos3', 'series'], inplace=True)
    # Filter out aggregated countries and only the metrics that are relevant
    data = data[(~data['Country'].isin(country_cli_excl))].reset_index(drop=True)

    return data


def oecd_data_engineering(data_emp: pd.DataFrame,
                          data_cli: pd.DataFrame,
                          data_price_index: pd.DataFrame,
                          country_mapping: pd.DataFrame) -> pd.DataFrame:
    # Convert to employment rate
    data_emp['value'] = 100 - data_emp['value']
    data_cli['Measure'] = "Index 2015=100"
    data_cli = data_cli[['value', 'Country', 'Subject', 'Measure', 'Frequency', 'Time']]

    data_price_index = data_price_index[data_price_index['Measure'].isin(
        ["Index 2015=100", "Level, rate or national currency, s.a."]) &
                                        ~(data_price_index['Subject'].isin([
                                            'Consumer Price Index > Restaurants and hotels ('
                                            'COICOP 11) > Total > Total',
                                            'Consumer opinion surveys > Economic Situation > '
                                            'Future tendency > National indicator']))]

    data_merged = pd.concat([data_emp, data_cli, data_price_index], axis=0)
    data_merged['Time'] = data_merged.apply(lambda x: datetime.strptime(x['Time'], '%b-%Y'), axis=1)
    data_merged = data_merged.sort_values(by=['Country', 'Subject', 'Measure', 'Time']).reset_index(
        drop=True)
    data_merged['value_lst'] = data_merged.groupby(['Country', 'Subject', 'Measure'])[
        'value'].shift(1)
    data_merged['value_lst_yr'] = data_merged.groupby(['Country', 'Subject', 'Measure'])[
        'value'].shift(12)
    data_merged = data_merged[
        data_merged.Subject != "Consumer opinion surveys > Confidence indicators > Composite " \
                               "indicators > National indicator"].reset_index(
        drop=True)
    # Replace countries to match to country mappings
    data_merged['country'] = data_merged['Country'].replace(
        ["China (People's Republic of)", 'Korea', 'Russia',
         'Slovak Republic', 'United States'], ["China",
                                               "Korea (Republic of)",
                                               "Russian Federation",
                                               "Slovakia",
                                               "United States of America"])
    data_merged.drop(columns=['Country'], inplace=True)

    data_merged = pd.merge(left=data_merged,
                           right=country_mapping,
                           on=['country'],
                           how="inner")

    return data_merged
