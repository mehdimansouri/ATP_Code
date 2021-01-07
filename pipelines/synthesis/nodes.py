import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
import time

from rpy2 import robjects
from rpy2.robjects import packages
from typing import Any, Dict, List


# Create logger
log = logging.getLogger(__name__)


### Helpers ###
def dds_country_bookings_helper(dds_country_bookings: pd.DataFrame) -> pd.DataFrame:
    dds_country_bookings = dds_country_bookings[dds_country_bookings.country_code != ''] \
        .replace('', '0') \
        .fillna('0')
    dds_country_bookings['Pax'] = dds_country_bookings['Pax'].astype(float)
    dds_country_bookings['date'] = pd.to_datetime(dds_country_bookings['date'])

    dds_country_bookings = pd.pivot_table(
            dds_country_bookings,
            values='Pax',
            index=['country_code', 'date'],
            columns=['travel_type'],
            aggfunc=np.sum)
    dds_country_bookings.columns = ['DDS Ticketings - ' + col for col in dds_country_bookings.columns]
    dds_country_bookings['DDS Ticketings'] = dds_country_bookings.sum(axis=1)
    dds_country_bookings = dds_country_bookings.reset_index()

    return dds_country_bookings


def identify_change_points_helper(data: pd.DataFrame, column_name: str) -> pd.DataFrame:
    pivot = pd.pivot_table(data, index='date', columns='country_code', values=column_name) \
        .sort_index()
    
    change_points = pivot[[]]
    for country in pivot.columns:
        time_series = robjects.FloatVector(pivot[country])
        robjects.globalenv['time_series'] = time_series

        # Compute change points
        cpt = robjects.r("""change_points <- cpt.np(time_series[which(!is.na(time_series))], method = "PELT",
                        test.stat = "empirical_distribution", class = TRUE, minseglen = 2,
                        nquantiles = 4*log(length(time_series)))""")
        cpt = robjects.r('cpts(change_points)')
        cpt = np.asarray(cpt)

        # Create indicator data		
        column = np.zeros(len(change_points))

        cpt = cpt[cpt > 0] - 1
        column[cpt] = 100
        change_points[country] = column

    # Unpivot and return results
    result = pd.melt(
        change_points.reset_index(),
        id_vars=['date'],
        var_name='country_code',
        value_name=column_name + '_Change_Points')
    return result


def before_after_helper(data, before_range, after_range, columns):
    before = data[pd.to_datetime(data.date).between(*before_range)] \
        .groupby(['country_code', 'continent', 'region']) \
        .agg({col: np.mean for col in columns})

    after = data[pd.to_datetime(data.date).between(*after_range)] \
        .groupby(['country_code', 'continent', 'region']) \
        .agg({col: np.mean for col in columns})

    # Align dataframes
    after = after.join(before[[]], how='outer').sort_index()
    before = before.join(after[[]], how='outer').sort_index()
    
    # Compute indexes and clip accoringly
    result = 100 * after.divide(before).abs() * np.where((after < 0) & ((after - before) < 0), -1, 1)
    result = result.clip(-300, 300)

    return result


### Main nodes ###

def consolidate_time_series_dataframes(
    covid_time_series: pd.DataFrame,
    dds_country_bookings_by_purchase_date: pd.DataFrame,
    dds_country_bookings_by_travel_date: pd.DataFrame,
    gds_country_searches: pd.DataFrame,
    google_trends: pd.DataFrame,
    government_response_time_series: pd.DataFrame,
    oag_time_series: pd.DataFrame) -> pd.DataFrame:

    # Prepare datasets
    covid_time_series = covid_time_series.drop('Country/Region', axis=1)
    covid_time_series.date = pd.to_datetime(covid_time_series.date)

    google_trends.date = pd.to_datetime(google_trends.date)

    government_response_time_series = government_response_time_series \
        .drop(['Country_Code3', 'Country_Name'], axis=1) \
        .rename(columns={'Country_Code': 'country_code', 'Date': 'date'})
    government_response_time_series.date = pd.to_datetime(government_response_time_series.date)

    oag_time_series = oag_time_series.drop('Region', axis=1) \
        .rename(columns={'Country_Code': 'country_code', 'Date': 'date'})
    oag_time_series.date = pd.to_datetime(oag_time_series.date)

    # Pivot GDS dataset
    gds_country_searches = gds_country_searches[gds_country_searches.country_code_origin != ''] \
        .rename(columns={'country_code_origin': 'country_code'}) \
        .replace('', '0') \
        .fillna('0')
    gds_country_searches['number_of_requests'] = gds_country_searches['number_of_requests'].astype(float)
    gds_country_searches.date = pd.to_datetime(gds_country_searches.date)

    gds_country_searches = pd.pivot_table(
            gds_country_searches,
            values='number_of_requests',
            index=['country_code', 'date'],
            columns=['travel_type'],
            aggfunc=np.sum)
    gds_country_searches.columns = ['GDS Searches - ' + col for col in gds_country_searches.columns]
    gds_country_searches['GDS Searches'] = gds_country_searches.sum(axis=1)
    gds_country_searches = gds_country_searches.reset_index()

    # Pivot DDS dataset
    max_date = dds_country_bookings_by_purchase_date.date.max()
    dds_country_bookings_by_travel_date = dds_country_bookings_by_travel_date[dds_country_bookings_by_travel_date.date <= max_date] \
        .rename(columns={'country_code_origin': 'country_code'})
    travel_dds_bookings = dds_country_bookings_helper(dds_country_bookings_by_travel_date)
    travel_dds_bookings.columns = [col.replace('DDS Ticketings', 'DDS Trips') for col in travel_dds_bookings.columns]

    dds_country_bookings_by_purchase_date = dds_country_bookings_by_purchase_date \
        .rename(columns={'country_code_origin': 'country_code'})
    purchase_dds_bookings = dds_country_bookings_helper(dds_country_bookings_by_purchase_date)
    purchase_dds_bookings.columns = [col.replace('DDS Ticketings', 'DDS Purchases') for col in purchase_dds_bookings.columns]

    # Merge datasets
    data = covid_time_series \
        .merge(google_trends, how='outer', on=['country_code', 'date']) \
        .merge(gds_country_searches, how='outer', on=['country_code', 'date']) \
        .merge(travel_dds_bookings, how='outer', on=['country_code', 'date']) \
        .merge(purchase_dds_bookings, how='outer', on=['country_code', 'date']) \
        .merge(government_response_time_series, how='outer', on=['country_code', 'date']) \
        .merge(oag_time_series, how='outer', on=['country_code', 'date'])

    latest_date = pd.to_datetime(data.loc[(~pd.isnull(data['Google_Coronavirus_Interest'])) & (
        ~pd.isnull(data['DDS Purchases'])), 'date'].max())
    print(latest_date)
    return data


def identify_change_points(data: pd.DataFrame, cpt_columns: List[str]) -> pd.DataFrame:
    # Install changepoints package in case it's not installed yet
    robjects.r['options'](warn=-1)
    utils = robjects.packages.importr('utils')
    utils.chooseCRANmirror(ind=1)

    utils.install_packages('changepoint')
    utils.install_packages('changepoint.np')

    robjects.r("library(changepoint)")
    robjects.r("library(changepoint.np)")

    # Compute change points over a set of columns
    change_points = data.copy()
    for col in cpt_columns:
        change_points = identify_change_points_helper(data, col)
        data = data.merge(change_points, how='left', on=['date', 'country_code'])

    return data


def add_geographical_mappings(data: pd.DataFrame, country_mappings: pd.DataFrame) -> pd.DataFrame:
    country_mappings = country_mappings[['code_2', 'country', 'continent_code', 'continent', 'region']] \
        .rename(columns={'code_2': 'country_code'})

    joined = data.merge(country_mappings, how='left', on='country_code')
    return joined


def compute_scorecard(data: pd.DataFrame, country_restrictions_matrix: pd.DataFrame) -> pd.DataFrame:
    # Remove changepoint columns
    data = data[[col for col in data.columns if 'Change_Point' not in col and 'Label' not in col]]

    # Compute date ranges
    cols = data.columns.values.tolist()
    print(cols)
    columns = data.columns[cols.index('covid_cases'): cols.index('country')]
    print(columns)
    #print(data.columns[data.isin(['Alaska']).any()])
    #print(data['Region_Name'].unique())
    columns = columns[data[columns].dtypes == 'float64']
    data[columns] = data[columns].replace('', np.nan).astype(float)

    latest_date = pd.to_datetime(data.loc[(~pd.isnull(data['Google_Coronavirus_Interest'])) & (~pd.isnull(data['DDS Purchases'])), 'date'].max())
    latest_range = [latest_date - pd.Timedelta(days=6), latest_date]
    print(latest_range)
    # Country restrictions
    restrictions = pd.pivot_table(
        country_restrictions_matrix,
        values='Restrictions',
        index=['Country_Code_Destination', 'Country_Destination', 'Region_Destination'],
        columns=['Border'],
        aggfunc='count')

    restrictions = restrictions \
        .fillna(0) \
        .reset_index() \
        .rename(columns={col: col.lower().replace('_destination', '') for col in restrictions.reset_index().columns})
    restrictions = restrictions[['country_code', 'region', 'closed']].rename(columns={'closed': 'border_closures'})

    # Last week averages of new covid cases
    new_cases = data[pd.to_datetime(data.date).between(*latest_range)] \
        .groupby(['country_code', 'continent', 'region']) \
        .agg({'covid_new_cases': np.mean for col in columns}) \
        .rename(columns={'covid_new_cases': 'covid_new_cases_(last_week_avg)'})

    # Weekly changes in new covid cases
    new_cases_wow = before_after_helper(
        data,
        [(d - pd.Timedelta(days=7)) for d in latest_range],
        latest_range,
        ['covid_new_cases'])
    new_cases_wow = new_cases_wow.rename(columns={'covid_new_cases': 'covid_new_cases_WoW_changes'})

    # Before and after comparison of non-covid case data (pre-crisis)
    scorecard = before_after_helper(
        data,
        [dt.datetime.strptime('2020-01-13', '%Y-%m-%d'), dt.datetime.strptime('2020-01-19', '%Y-%m-%d')],
        latest_range,
        columns[columns.values.tolist().index('covid_recoveries') + 1:].values) \
        .reset_index()
    scorecard = scorecard.rename(columns={col: col + ' Index' for col in columns if not col.startswith('covid_')})
    scorecard['Indexed_on'] = 'Pre-crisis (First week of 2020)'
    # scorecard['Indexed_on'] = 'Pre-crisis (%s) v. current week (%s)' % ('2020-01-06', latest_date.strftime('%Y-%m-%d'))

    # Before and after comparison of non-covid case data (WoW)
    wow_scorecard = before_after_helper(
        data,
        [(d - pd.Timedelta(days=6)) for d in latest_range],
        latest_range,
        columns[columns.values.tolist().index('covid_recoveries') + 1:].values) \
        .reset_index()
    wow_scorecard = wow_scorecard.rename(columns={col: col + ' Index' for col in columns if not col.startswith('covid_')})
    wow_scorecard['Indexed_on'] = 'Previous week'
    # wow_scorecard['Indexed_on'] = 'Last week (%s) v.current week (%s)' % (
    # 	(latest_date - pd.Timedelta(days=7)).strftime('%Y-%m-%d'), latest_date.strftime('%Y-%m-%d'))

    # Combine indexes
    scorecard = pd.concat([scorecard, wow_scorecard], ignore_index=True)

    # Merge datasets
    result = new_cases \
        .merge(new_cases_wow, on=['country_code', 'continent', 'region']) \
        .merge(scorecard, on=['country_code', 'continent', 'region']) \
        .merge(restrictions, on=['country_code', 'region'])

    # Set infinity values to null
    result = result.replace([np.inf, -np.inf], np.nan)

    return result
