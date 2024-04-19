:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: LICENSING                                                                    :
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
::
:: Copyright 2020 Esri
::
:: Licensed under the Apache License, Version 2.0 (the "License"); You
:: may not use this file except in compliance with the License. You may
:: obtain a copy of the License at
::
:: http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
:: implied. See the License for the specific language governing
:: permissions and limitations under the License.
::
:: A copy of the license is available in the repository's
:: LICENSE file.

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: VARIABLES                                                                    :
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

SETLOCAL
SET PROJECT_DIR=%cd%
SET PROJECT_NAME=spark-utils
SET SUPPORT_LIBRARY = spark_utils
SET CONDA_DIR="%~dp0env"

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: COMMANDS                                                                     :
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: Jump to command
GOTO %1

:: Make documentation using Sphinx!
:docs
    CALL conda run -p %CONDA_DIR% sphinx-build -a -b html docsrc docs
    GOTO end

:: Get all resources needed to run Geoanalytics locally
:setup
    CALL mkdir C:\opt\geoanalytics
    CALL robocopy \\metro\GeoAnalyticsEngine\1.4.0\ C:\opt\geoanalytics\ *.jar /mt /z
    CALL robocopy \\metro\GeoAnalyticsEngine\1.4.0\peData\ C:\opt\geoanalytics\ *.jar /mt /z
    CALL copy \\metro\Released\Authorization_Files\ArcGISGeoAnalyticsEngine\1.0\GeoAnalytics_OnDemand_Engine.ecp C:\opt\geoanalytics\geoanalytics_license.ecp
    CALL robocopy \\esri.com\software\Esri\Released\StreetMap_Premium_for_ArcGIS\HERE\North_America_2023R4\Pro_and_Enterprise_GCS_MMPK\ C:\opt\geoanalytics\ United_States.mmpk /mt /z
    CALL copy \\metro\Released\Authorization_Files\ArcGISRuntimeSDK\Version100.0\licenseKeys.txt C:\opt\geoanalytics\smp_license.ecp
    GOTO env

:: Build the local environment
:env
    CALL conda create -p %CONDA_DIR% --clone "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3"
    GOTO add_dependencies

:env_dev
    CALL conda create -p %CONDA_DIR% --clone "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3"
    GOTO add_dev_dependencies

:: Add python dependencies from environment.yml to the project environment
:add_dependencies
    CALL conda env update -p %CONDA_DIR% -f environment.yml
    CALL conda run -p %CONDA_DIR% python -m pip install .
    GOTO end

:add_dev_dependencies
    CALL conda env update -p %CONDA_DIR% -f environment_dev.yml
    CALL conda run -p %CONDA_DIR% python -m pip install -e .
    GOTO end

:: Remove the environment
:remove_env
    CALL conda deactivate
    CALL conda env remove -p %CONDA_DIR% -y
	GOTO end

:: Start Jupyter
:jupyter
    CALL conda run -p %CONDA_DIR% python -m jupyterlab --ip=0.0.0.0 --allow-root --NotebookApp.token=""
    GOTO end

:: code formatting
:black
    CALL conda run -p %CONDA_dIR% black src/ --verbose
    GOTO end

:lint
    GOTO black

:linter
    GOTO black

:end
    EXIT /B
