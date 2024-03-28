Spark-Utils API
================================

Spark-Utils (``spark_utils``) is a collection of utilities streamlining many of the common tasks I have to do with
Spark.

Spark Session Management
------------------------

.. autofunction:: spark_utils.get_spark_session
.. autofunction:: spark_utils.spark_stop

Saving Data Frames and Schema File
----------------------------------

.. autofunction:: spark_utils.save_to_parquet
.. autofunction:: spark_utils.get_schema_dataframe
.. autofunction:: spark_utils.save_parquet_with_schema

Data Frame Interrogation
------------------------

.. autofunction:: spark_utils.get_column_name_if_exists
