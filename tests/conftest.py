# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the
# License.

import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("JAVA_HOME", os.path.join(os.path.dirname(sys.executable), "Library"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
if os.name == "nt":
    os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
    os.environ["PATH"] = os.environ.get("PATH", "") + ";C:\\hadoop\\bin"

    _repo_root = Path(__file__).parent.parent
    _airflow_home = _repo_root / ".airflow_home"
    _airflow_home.mkdir(exist_ok=True)
    os.environ.setdefault("AIRFLOW_HOME", str(_airflow_home))
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        f"sqlite:////{str(_airflow_home).replace(chr(92), '/')}/airflow.db",
    )
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(_repo_root / "dags"))
    os.environ.setdefault("AIRFLOW__LOGGING__BASE_LOG_FOLDER", str(_airflow_home / "logs"))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ingestion-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
