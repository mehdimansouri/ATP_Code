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

	Args:
		is_scoring: Boolean flag to configure whether or not the pipeline is a scoring or training pipeline

	Returns:
		A ``Pipeline`` object built from a list of nodes.

	"""

	return Pipeline([
			node(
				lambda: ['request_date', 'request_outbound_date'],
				None,
				['gds_purchase_date_field', 'gds_travel_date_field']
			),
			node(
				nodes.combine_data,
				["params:gds_searches_folder_path", "winglet_historical", "airport_codes", "multiple_airport_cities"],
				"merged_gds_searches",
			),
			# node(
			# 	nodes.map_countries,
			# 	["merged_gds_searches", "airport_codes"],
			# 	"merged_gds_searches_countries",
			# ),
			# node(
			# 	nodes.handle_airport_cities,
			# 	["merged_gds_searches_countries", "multiple_airport_cities"],
			# 	"airport_city_mapped"
			# ),
			node(
				nodes.add_features,
				# ["airport_city_mapped", "country_mappings"],
				["merged_gds_searches", "country_mappings"],
				"gds_searches",
			),
			node(
				nodes.aggregate_country_searches,
				["gds_searches", "gds_purchase_date_field"],
				"gds_country_searches",
			),
			# node(
			# 	nodes.aggregate_country_searches,
			# 	["gds_searches", "gds_travel_date_field"],
			# 	"gds_country_searches_by_travel_date",
			# ),
		])


