__title__ = "spark-utils"
__version__ = "0.0.0"
__author__ = "Joel McCune (https://github.com/knu2xs)"
__license__ = "Apache 2.0"
__copyright__ = "Copyright 2023 by Joel McCune (https://github.com/knu2xs)"

import logging
import os
from glob import glob
from typing import Optional, Union, List, Dict

from pyspark import SparkConf, SparkContext

# from py4j.java_gateway import JavaGateway
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as fns
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
    MapType,
)
from subprocess import Popen

__all__ = [
    "get_spark_session",
    "spark_stop",
    "get_column_name_if_exists",
    "get_schema_dataframe",
    "save_to_parquet",
    "save_parquet_with_schema",
]

# detect if in AWS EMR...only environment using 'livy' users in both these contexts
in_aws_emr = os.environ.get("USER") == "livy" and os.environ.get("SPARK_USER") == "livy"

# detect if windows
is_win = os.name == "nt"

# detect if mac
is_mac = os.name == "darwin"

# detect if in Azure Databricks
in_databricks = os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None


def get_pro_install_path(raise_err: bool = False) -> str:
    """
    Retrieve the ArcGIS Pro install path.

    Args:
        raise_err: Whether to raise an error if the ArcGIS Pro install location is not found.
    """

    try:
        # late import
        import winreg
        from pathlib import WindowsPath

        # get the key keeping the install location for ArcGIS Pro
        agp_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, "SOFTWARE\ESRI\ArcGISPro")

        # pull out the install path
        pro_install_pth = winreg.QueryValueEx(agp_key, "InstallDir")[0]

        # catch the error when the key is not present
    except NameError:
        # if raising an error, do so
        if raise_err:
            raise EnvironmentError("It does not appear ArcGIS Pro is installed.")

        # otherwise, just log the warning and populate the returning
        else:
            logging.info("It does not appear ArcGIS Pro is installed.")
            pro_install_pth = None

        # catch the error when not in windows, when the winreg
    except ModuleNotFoundError:
        # otherwise, just log not being a windows environment
        logging.debug("It does not appear this is a Windows environment.")
        pro_install_pth = None

    return pro_install_pth


def get_spark_session(
    spark_application_name: Optional[str] = "Business Analyst Data Engineering",
    java_home_path: str = None,
    spark_home_path: str = None,
    hadoop_home_path: str = None,
    geoanalytics_resources: Union[str, Dict[str, str]] = None,
) -> SparkSession:
    """
    Get a spark session with the spatial data libraries loaded.

    Args:
        spark_application_name: If desiring to uniquely identify the spatial data pipeline with a different name.
        java_home_path: Path to directory where Java is saved.
        spark_home_path: Path to directory where Spark is saved.
        hadoop_home_path: Path to directory where Hadoop is saved.
        geoanalytics_resources: Path to directory where geometry engine resources are saved.

    Returns:
        SparkSession ready to do work.
    """
    # get a dict of input parameters to interact with
    kwargs = locals()

    # directory to find ArcGIS Pro
    pro_pth = get_pro_install_path()

    # start building up the configuration
    conf = SparkConf()

    # ensure when saving, we can save to specific partitions without affecting other partitions
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # so can load geometries for countries locally
    conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    # dictionary to hold resource paths to look for
    resc_dict = {}

    # if in AWS EMR
    if in_aws_emr and geoanalytics_resources is not None:
        # ensure needed resources are included in the input dict
        if not isinstance(geoanalytics_resources, dict):
            raise ValueError(
                "geoanalytics_resources must be provided as a dict in AWS EMR"
            )

        # for necessary resources, extract
        ga_res_tpls = [
            ("JARS", "jar_pth"),
            ("PY_WHL", "py_pth"),
            ("GA_LIC", "ga_lic_pth"),
            ("SMP_LIC", "smp_lic_pth"),
        ]
        for dict_key, var_key in ga_res_tpls:
            resc_dict[var_key] = geoanalytics_resources.get(dict_key)

            # catch any errors
            if dict_key != "SMP_LIC" and kwargs[var_key] is None:
                raise ValueError(
                    f"A value is required for {dict_key} in geoanalytics_resources."
                )
            else:
                logging.warning(
                    "No license was provided for StreetMap Premium, so this will not be available."
                )

    # if running locally
    else:
        # ensure will work without connection to internet and be able to load larger datasets
        conf.set("spark.master", "local[*]")
        conf.set("spark.driver.host", "127.0.0.1")
        conf.set(
            "spark.kryoserializer.buffer.max", "2047"
        )  # one less than the allowed max of 2048

        # https://esrips.github.io/bdt3/notebooks/documentation/Setup_InWindows.htm
        conf.set("spark.sql.execution.pyarrow.enabled", "true")

        # if resource paths are not provided, try to get from system environment variables
        sys_var_tpls = [
            ("java_home_path", "JAVA_HOME"),
            ("spark_home_path", "SPARK_HOME"),
            ("hadoop_home_path", "HADOOP_HOME"),
            ("geoanalytics_resources", "GEOANALYTICS_HOME"),
        ]
        for pth_var, var_key in sys_var_tpls:
            if kwargs.get(pth_var) is None:
                pth = os.environ.get(var_key)
                resc_dict[pth_var] = pth

        # if ArcGIS Pro installed, and other paths are not explicitly defined, piggyback on Pro's included assets
        if os.path.exists(pro_pth):
            resc_dict["java_home_path"] = os.path.join(
                pro_pth, "java", "runtime", "jvm"
            )
            resc_dict["hadoop_home_path"] = os.path.join(
                pro_pth, "java", "runtime", "hadoop"
            )
            resc_dict["spark_home_path"] = os.path.join(
                pro_pth, "java", "runtime", "spark"
            )

        # unpack resource paths
        java_home_path = resc_dict.get("java_home_path")
        hadoop_home_path = resc_dict.get("hadoop_home_path")
        spark_home_path = resc_dict.get("spark_home_path")
        geoanalytics_resources = resc_dict.get("geoanalytics_resources")

        # if adding geoanalytics, locate resources and add to config
        if geoanalytics_resources is not None:
            # get all included JARS; this should include the H3 jar as well for GA H3 functions to work
            jar_pth_lst = glob(os.path.join(geoanalytics_resources, "*.jar"))
            if len(jar_pth_lst) == 0:
                raise ValueError(
                    "Cannot locate any JAR files in the provided Geoanalytics directory. Please check the "
                    "path and ensure these resources are present in this location."
                )
            jar_pth = ",".join([str(pth) for pth in jar_pth_lst])

            # get the python wheel file for geoanalytics
            py_pth = glob(os.path.join(geoanalytics_resources, "geoanalytics*.whl"))
            if len(py_pth) == 0:
                raise ValueError(
                    "Cannot locate Geoanalytics wheel file. Please check the path and ensure it is "
                    "present in this location."
                )
            py_pth = str(py_pth[0])

            # get all license files
            lic_pth_lst = glob(os.path.join(geoanalytics_resources, "*.ecp"))

            # pull out the streetmap license file
            smp_lic_pth = [
                pth
                for pth in lic_pth_lst
                if "smp" in os.path.basename(pth).lower()
                or "streetmap" in os.path.basename(pth).lower().lower()
            ]
            if len(smp_lic_pth) == 0:
                logging.warning("StreetMap Premium license file not located.")
            else:
                smp_lic_pth = str(smp_lic_pth[0])

            # pull out GA license file
            ga_lic_pth = [
                pth
                for pth in lic_pth_lst
                if "smp" not in os.path.basename(pth).lower().lower()
                and "streetmap" not in os.path.basename(pth).lower().lower()
            ]
            if len(ga_lic_pth) == 0:
                raise ValueError(
                    "Geoanalytics license file not located. Please check the path and ensure it is "
                    "present in this location."
                )
            ga_lic_pth = str(ga_lic_pth[0])

            # set locations for geoanalytics resources
            conf.set("spark.jars", jar_pth)
            conf.set("spark.submit.pyFiles", py_pth)

            # register plugin with spark
            # ref: https://developers.arcgis.com/geoanalytics/install/local_mode/#start-a-pyspark-session-with-geoanalytics-engine
            conf.set("spark.plugins", "com.esri.geoanalytics.Plugin")
            conf.set("spark.kryo.registrator", "com.esri.geoanalytics.KryoRegistrator")

            # set license file
            # ref: https://developers.arcgis.com/geoanalytics/reference/authorization/#authorize-using-spark-properties
            conf.set("spark.geoanalytics.auth.license.file", ga_lic_pth)

            # set the street map premium license file
            conf.set("spark.geoanalytics.smp.license.file", smp_lic_pth)

            # shut down any currently active local sessions (only one can be active in a local environment)
            spark_stop()

    # create the spark session using the provided parameters
    spark = (
        SparkSession.builder.appName(spark_application_name)
        .config(conf=conf)
        .getOrCreate()
    )

    # if using Spark shipped with Pro in a local environment, have to manage the py4j gateway manually
    # if pro_pth is not None:
    #     # launch the gateway (borrowed from mraad/spark-esri)
    #     gateway = JavaGateway.launch_gateway(
    #         # must redirect stdout & stderr when running in Pro or JVM fails immediately
    #         redirect_stdout=None,
    #         redirect_stderr=None,
    #         enable_auth=True,
    #         # keeps the command-line window from showing
    #         use_shell=True,
    #     )
    #
    #     # use the gateway to create a spark context
    #     spark_context = SparkContext()

    #     # use the spark context to build the spark session
    #     spark = (
    #         SparkSession.builder.appName(spark_application_name)
    #         .config(conf=conf)
    #         .getOrCreate()
    #     )
    #
    # # otherwise, use spark session builder directly
    # else:
    #     spark = (
    #         SparkSession.builder.appName(spark_application_name)
    #         .config(conf=conf)
    #         .getOrCreate()
    #     )

    return spark


def spark_stop() -> None:
    """
    Provide universal way to stop spark. If not on a Windows machine, ``spark.stop()``, works fine, but if using
    Spark bundled with Pro, there are a few more loose ends needing to be tied up. This function provides a
    standard way to perform this task regardless of the environment.
    """
    # retrieve the active spark session
    spark = SparkSession.getActiveSession()

    # ensure there is an active spark session
    if isinstance(spark, SparkSession):
        # determine if using Spark bundled with ArcGIS Pro by checking if Spark in Pro path (path contains "arcgis")
        if (
            "arcgis" in os.environ["SPARK_HOME"].lower()
            and SparkContext._gateway is not None
        ):
            # extract the gateway out of the spark session configuration
            gateway = spark.sparkContext._gateway

            # stop the spark session
            spark.stop()

            # ensure the gateway is completely axed
            gateway.shutdown()
            gateway.proc.stdin.close()

            # ensure that process and all children are killed
            Popen(
                ["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(gateway.proc.pid)],
                shell=True,
                stdout=os.devnull,
                stderr=os.devnull,
            )

            # set the spark context gateway to nothing to tie up loose ends
            SparkContext._gateway = None

        # otherwise, stop using SparkSession stop method...simple
        else:
            spark.stop()


def get_column_name_if_exists(
    df: DataFrame, column_name: str, throw_error: bool = False
) -> str:
    """
    Return the column name if it exists in the dataframe schema.

    .. note::

        The name provided can be partial. The function searches for the first column containing the string provdied
        in the ``column_name`` parameter. Hence, ``iso2`` returns ``country_iso2``.

    Args:
        df: PySpark ``DataFrame`` to be searched.
        column_name: Name string to search for.
        throw_error: Whether to error if the name is not found. Default is ``False``.
    """
    # variable to populate when column found
    ret_col: Optional[str] = None

    # ensure column name is not adversely affected by case
    column_name = column_name.lower()

    # iterate the column names in the dataframe
    for col in df.columns:
        # if the column name string is in the current column, get it and break out of the loop
        if col.lower().find(column_name) >= 0:
            ret_col = col
            break

    # if the column was not found, and we're supposed to throw an error, do it
    if ret_col is None and throw_error:
        raise ValueError(f"Cannot locate {column_name} in the DataFrame schema.")

    return ret_col


def save_to_parquet(
    dataset: DataFrame,
    parquet_path: str,
    partitioning_columns: Optional[Union[str, List[str]]] = None,
    max_records_per_file: int = 300000,
) -> str:
    """
    Write a Spark data frame with defaults.

    * ``.mode("overwrite")``
    * ``.option("partitionOverwriteMode", "dynamic")``
    * ``.option("maxRecordsPerFile", max_records_per_file)``

    Optionally, if ``partitioning_columns`` provided.

    * ``.partitionBy(partitioning_columns)``

    Args:
        dataset: PySpark DataFrame to be exported.
        parquet_path: Path to where output parquet will be stored.
        partitioning_columns: Column name or list of column names to partition by.

    """
    # ensure is string so can be used...accounts for Path objects being passed in
    if not isinstance(parquet_path, str):
        parquet_path = str(parquet_path)

    # if not partitioning, save without partitioning options
    if partitioning_columns is None:
        (
            dataset.write.mode("overwrite")
            .option("maxRecordsPerFile", max_records_per_file)
            .parquet(parquet_path)
        )

    # if partitioning, save with partitioning options
    else:
        (
            dataset.write.mode("overwrite")
            .partitionBy(partitioning_columns)
            .option("partitionOverwriteMode", "dynamic")
            .option("maxRecordsPerFile", max_records_per_file)
            .parquet(parquet_path)
        )

    return parquet_path


def get_schema_dataframe(
    dataframe: DataFrame,
    output_schema_file: Optional[str] = None,
    percent_padding: Optional[float] = None,
) -> DataFrame:
    """
    Get a CSV file formatted to be used with the ``arcpy-parquet`` Parquet to Feature Class capability.

    Args:
        dataframe: Parquet data frame used for introspecting the string columns with lengths.
        output_schema_file: Path to where the schema file will be saved.
        percent_padding: Percent to pad the lengths by. This must be a decimal value between zero and one.
    """
    # ensure percent is valid if provided
    if percent_padding is not None:
        err_msg = "percent_padding must be a float value between zero and one."

        if not isinstance(percent_padding, float):
            raise ValueError(err_msg)
        elif percent_padding < 0.0 or percent_padding > 1.0:
            raise ValueError(err_msg)

    # ensure the s3 paths have the correct prefix
    if output_schema_file.lower().startswith(
        "s3:/"
    ) and not output_schema_file.lower().startswith("s3://"):
        output_schema_file = output_schema_file.replace("s3:/", "s3://")

    # output CSV column list
    out_col_lst = [
        "field_name",
        "field_type",
        "field_precision",
        "field_scale",
        "field_length",
        "field_alias",
        "field_is_nullable",
        "field_is_required",
        "field_domain",
    ]

    # get all the string columns in the data frame
    string_cols = [
        col for col in dataframe.schema if isinstance(col.dataType, StringType)
    ]

    # create aliases for of the column dataframe - identical to column
    string_col_names = [c.name for c in string_cols]
    string_len_cols = [fns.length(col.name).alias(col.name) for col in string_cols]

    # get the string column maximum lengths, filling in nulls with zero length
    df_str_len = dataframe.select(string_len_cols).groupby().max().fillna(0)

    # rename columns to match with input column names
    for idx, col in enumerate(df_str_len.columns):
        df_str_len = df_str_len.withColumnRenamed(col, string_col_names[idx])

    # transpose the schema - make columns rows and rows columns
    row_dict = df_str_len.first().asDict()

    # create a data frame with the desired schema
    schm = StructType(
        [
            StructField("field_name", StringType(), True),
            StructField("field_length", LongType(), True),
        ]
    )

    spark = SparkSession.getActiveSession()
    schm_df = spark.createDataFrame(
        data=[(k, v) for k, v in row_dict.items()], schema=schm
    )

    # add other columns with values
    schm_df = schm_df.withColumn("field_alias", fns.col("field_name"))
    schm_df = schm_df.withColumn("field_type", fns.lit("String"))
    schm_df = schm_df.withColumn("field_precision", fns.lit(0))
    schm_df = schm_df.withColumn("field_scale", fns.lit(0))
    schm_df = schm_df.withColumn("field_is_nullable", fns.lit("TRUE"))
    schm_df = schm_df.withColumn("field_is_required", fns.lit("FALSE"))
    schm_df = schm_df.withColumn("field_domain", fns.lit(None).cast(StringType()))

    # add a percentage padding if desired
    if percent_padding:
        schm_df = schm_df.withColumn(
            colName="field_length",
            col=fns.ceil(fns.col("field_length") * (1 + percent_padding)),
        )

    # organize schema
    schm_df = schm_df.select(out_col_lst).repartition(1)

    # save the schema file
    if output_schema_file is not None:
        schm_df.write.mode("overwrite").csv(str(output_schema_file), header=True)

    return schm_df


def save_parquet_with_schema(
    dataset: DataFrame,
    output_directory: str,
    partitioning_columns: Optional[Union[str, List[str]]] = None,
    schema_percent_padding: Optional[float] = None,
    bool_to_int: bool = True,
    complex_to_json: bool = True,
) -> str:
    """
    Write a PySpark data frame to a Parquet dataset with the schema string lengths as a comma-separated-file (CSV) in
    the standard directory structure automatically recognized for importing into ArcGIS Pro.

    .. note::
        The ``output_directory`` will include two subdirectories, ``parquet`` and ``schema``. The Parquet dataset part
        files will be saved in the ``parquet`` directory, and a single CSV part file will be saved in the ``schema``
        directory. This is the directory structure expected by the Parquet to Feature Class tool.

    Args:
        dataset: PySpark dataframe being exported.
        output_directory: Path to store the exported data.
        partitioning_columns: Any optional columns being used to partition the data.
        schema_percent_padding: Percent padding to apply to the string schema lengths.
        bool_to_int: Optionally convert any booleans to integers (useful since ArcGIS does not have a boolean type).
            Default is ``True``.
        complex_to_json: Optionally downcast any complex columns (``ArrayType``, ``MapType``, ``StructType``) to
            JSON string representations for compatibility with ArcGIS Pro. Default is ``True``.
    """
    # if the last part of the path is the parquet directory, use the parent
    if os.path.basename(output_directory) == "parquet":
        output_directory = os.path.dirname(output_directory)

    # cast any boolean to integers if desired
    if bool_to_int:
        for col in [c[0] for c in dataset.dtypes if c[1] == "boolean"]:
            dataset = dataset.withColumn(col, fns.col(col).cast("smallint"))

    # downcast any complex columns to strings if desired
    if complex_to_json:
        # iterate the full schema, columns with metadata
        for c in dataset.schema:
            # if one of the complex data types, convert to a JSON string
            if isinstance(c.dataType, (ArrayType, MapType, StructType)):
                dataset = dataset.withColumn(c.name, fns.to_json(fns.col(c.name)))

    # save the parquet data with any optional partitions
    save_to_parquet(
        dataset,
        parquet_path=os.path.join(output_directory, "parquet"),
        partitioning_columns=partitioning_columns,
    )

    # save the schema to the same location
    get_schema_dataframe(
        dataset,
        output_schema_file=os.path.join(output_directory, "schema"),
        percent_padding=schema_percent_padding,
    )

    return output_directory
