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
			node(
				nodes.load_latest_date_file,
				"params:oag_data_folder_path",
				"raw_oag_data"),
			node(
				nodes.previous_year_capacity,
				"raw_oag_data",
				"oag_previous_years"),
			# node(
			# 	nodes.merge_innovata_data,
			# 	["oag_previous_years", "innovata_data", "innovata_country_mappings"],
			# 	"oag_innovata_data"),
			node(
				nodes.merge_schedule_data,
				["oag_previous_years", "oag_schedules"],
				"oag_schedule_data"),
			node(
				nodes.add_geographical_mappings,
				["oag_schedule_data", "country_mappings"],
				"oag_dataset"),
			node(
				nodes.aggregate_data_by_orig_dest,
				"oag_dataset",
				"oag_time_series_dataset"),
		])

