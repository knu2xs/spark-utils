Windows with Pro Setup
=============================================================================================================

.. note::

    If you don't care, and just want to get started quickly, after downloading the repo, from in the repo, just run ``make setup``.

    ```
    git clone https:\\github.com\knu2xs\spark-utils
    cd spark-utils
    make setup
    ```

## Procure Geoanalytics and StreeetMap Premium Resources

1. Create location to store resources, such as ``C:\opt\geoanalytics``.

```
mkdir C:\opt\geoanalytics
```

2. Procure Geoanalytics JAR files. Copy JAR files in root of the directory, and in the ``peData`` directory, to the root of the local Geoanalytics directory.

```
robocopy \\metro\GeoAnalyticsEngine\1.4.0\ C:\opt\geoanalytics\ *.jar /mt /z
robocopy \\metro\GeoAnalyticsEngine\1.4.0\peData\ C:\opt\geoanalytics\ *.jar /mt /z
```

3. Procure Geoanalytics license file.

```
copy \\metro\Released\Authorization_Files\ArcGISGeoAnalyticsEngine\1.0\GeoAnalytics_OnDemand_Engine.ecp C:\opt\geoanalytics\geoanalytics_license.ecp
```

4. StreeetMap Premium North America Data

```
robocopy \\esri.com\software\Esri\Released\StreetMap_Premium_for_ArcGIS\HERE\North_America_2023R4\Pro_and_Enterprise_GCS_MMPK\ C:\opt\geoanalytics\ United_States.mmpk /mt /z
```

5. StreetMap Premium License File:

```
copy \\metro\Released\Authorization_Files\ArcGISRuntimeSDK\Version100.0\licenseKeys.txt C:\opt\geoanalytics\smp_license.ecp
```
