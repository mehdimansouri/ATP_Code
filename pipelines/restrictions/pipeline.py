# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#	 or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""Pipeline construction."""

import os
import logging

from typing import Dict

from kedro.config import ConfigLoader
from kedro.pipeline import Pipeline, node

from . import nodes

# Here you can define your data-driven pipeline by importing your functions
# and adding them to the pipeline as follows:
#
# from nodes.data_wrangling import clean_data, compute_features
#
# pipeline = Pipeline([
#	 node(clean_data, 'customers', 'prepared_customers'),
#	 node(compute_features, 'prepared_customers', ['X_train', 'Y_train'])
# ])
#
# Once you have your pipeline defined, you can run it from the root of your
# project	 by calling:
#
# $ kedro run


def create_pipeline() -> Pipeline:
	"""Create the project's pipeline.

	Returns:
		A ``Pipeline`` object built from a list of nodes.

	"""

	return Pipeline([
			# Restrictions matrix
			node(
				nodes.fetch_restrictions_matrix,
				["params:iom_restriction_matrix_url", "restriction_mappings"],
				["scraped_restrictions_matrix", "latest_restriction_matrix_date"],
			),
			node(
				nodes.fetch_restrictions_matrix,
				["params:iom_restriction_matrix_url", "restriction_mappings"],
				["previous_scraped_restrictions_matrix", "previous_restriction_matrix_date"],
			),
			node(
				nodes.add_matrix_geographical_mappings,
				["scraped_restrictions_matrix", "country_mappings"],
				"country_restrictions_matrix_mapped",
			),
			node(
				nodes.add_matrix_geographical_mappings,
				["previous_scraped_restrictions_matrix", "country_mappings"],
				"country_previous_restrictions_matrix",
			),
			node(
				nodes.capture_restrictions_matrix_changes,
				["country_restrictions_matrix_mapped", "country_previous_restrictions_matrix"],
				"country_restrictions_matrix",
			),
			# Airport restrictions
			node(
				nodes.fetch_airport_restrictions,
				["params:icao_airport_restrictions_api_endpoint", "params:icao_api_key"],
				"airport_restrictions_extracted",
			),
			node(
				nodes.add_airport_geographical_mappings,
				["airport_restrictions_extracted", "country_mappings"],
				"airport_restrictions"
			),

			# Coronavirus government response tracker
			node(
				nodes.fetch_gov_response_time_series,
				[
					"params:government_response_time_series_github_file_path",
					"country_mappings",
					"government_response_label_mappings"
				],
				"government_response_time_series"
			),

			# IATA Timatic data
			# node(
			# 	nodes.scrape_timatic_restrictions,
			# 	"params:iata_timatic_heatmap_url",
			# 	"timatic_country_restrictions"
			# )
			node(
				nodes.get_timatic_flat_file,
				"params:timatic_data_path",
				"timatic_country_restrictions_flat"
			)
		])

