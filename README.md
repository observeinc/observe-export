# Observe-Csv
A python tool to easily extract data from observe worksheets and datasets.

# Dependencies & Requirements

To use ```observe-csv``` you will need at least python3.8. This package has, so far, only been tested on Linux 
(Ubuntu 20.04) and Mac OS X 11.2. 

The data is exported via ```curl``` which needs to be set up properly on the machine. 

# Installation

Python libraries should generally be installed in individual virtual environments to avoid clashes of dependencies.
Hence, you should first create a virtual environment, e.g., using ```python3.X -m venv venv``` which creates the
folder ```venv``` in the local directory. You may then activate this environment using ```source venv/bin/activate```.

To install observe-csv it suffices to download the repository and execute the setup.py, e.g., via
``` pip install . ```. ```pip``` will automatically install all dependencies (currently, including development 
requirements as sphinx).

In case that you want to extend the functionality of this package, it makes sense to install it as
editable via ``` pip install . -e```

Once installed, you can readily use the exporter using the command ```observe-csv```. Building on top of 

# Usage

## observe-csv

This module allows to export data from Observe worksheets and datasets to CSV files.
Data can be exported via the command `observe-csv export <config> <output_directory>`.

An example dataset export configuration may look like this:

```json
{
    "datasource": {
        "dataset": 12345678,
        "opal_query": "col_pick col1, col2, col3\nfilter col1=100",
        "initial_interval_to_query_seconds": 300,
        "check_completeness_of_data": true,
        "comment": "Any comment you put here will not be processed."
    },
    "filename_prefix": "dataset_filename",
    "string_columns": [
        "col1"
    ],
    "sort_keys": [
        "col2"
    ],
    "columns_to_keep": [
        "col1",
        "col2",
        "col3"
    ],
    "start_time": "2022-03-07T22:34:56.219209",
    "end_time": "2022-03-07T23:34:56.219215",
    "url": "xzy.observe.com",
    "user": "xzy",
    "token": "access token for user xzy on xzy.observe.com"
}
```

An example worksheet export configuration may look like this:

```json
{
    "datasource": {
        "worksheet": 12345678,
        "stage": "stage-abcdefg",
        "initial_interval_to_query_seconds": 300,
        "comment": "Any comment you put here will not be processed."
    },
    "filename_prefix": "worksheet_filename",
    "string_columns": [
        "col1"
    ],
    "sort_keys": [
        "col2"
    ],
    "columns_to_keep": [
        "col1",
        "col2",
        "col3"
    ],
    "start_time": "2022-03-07T22:34:48.591796",
    "end_time": "2022-03-07T23:34:48.591804",
    "url": "xzy.observe.com",
    "user": "xzy",
    "token": "access token for user xzy on xzy.observe.com"
}
```

In the following, we shortly discuss the main ingredients:


* `datasource` specifies the destination the data should be loaded from. This may either be a dataset or a worksheet.
When a dataset is selected, some arbitrary OPAL may be given. As Observe enforces a 100k limit on returned rows
the user may specify a temporal granularity according to which data may be retrieved via `initial_interval_to_query_seconds`.

**WARNING**: The value of `initial_interval_to_query_seconds` will dictate the query time window. Do not use this setting,
i.e., leave it `null` when you want to query aggregate data for example originating from a worksheet.

For dataset exports, you may specify that the completeness of the data shall be verified. Here, an additional
statsby query is executed to count the number of rows expected. If not all data was returned a warning is output
on stderr.


* `filename_prefix` specifies the prefix of the output file. Additional parts of the filename are the dataset source,

    and the start and end time.


* `string_columns` specifies that the given columns are to be loaded explicitly as strings into when processing
the data. This is at times needed, as exported NULL values may otherwise be converted to NaN values by pandas
hence forcing the row to be converted to floats. This field is optional and can be omitted or set to `null`.


* `sort_keys` specifies the columns according to which the data is to be sorted before being stored in the csv file.

    This field is optional and can be omitted or set to `null`.


* `columns_to_keep` can be used to filter down the output columns.

**NOTE**: Instead of filtering the data after having downloaded it, for datasets it makes sense to include a `pickl_col`
directive in the OPAL such that only the data really needed is downloaded.


* `start_time` defines the start of the query window and must be given as UTC time.


* `start_time` defines the end of the query window and must be given as UTC time.


* `url` defines the observe url to query the data from.


* `user` defines the user account to use for the export of the data.


* `token` the access token provisioned for the user to export the data/

**NOTE**: Dates and times are always interpreted as UTC times.

To get the user started with writing configurations example configurations can be glanced at using the following
commands:


* ```observe-csv [minimal/full]-example-dataset-config```


* ```observe-csv [minimal/full]-example-worksheet-config```

```shell
observe-csv [OPTIONS] COMMAND [ARGS]...
```

### export

Performs an export of data via the given JSON file which contains a series of export configurations which
may reference each other. As second mandatory argument a path needs to be specified
where the output is stored.

As export configurations are executed in the order specified in the JSON file, export stages referenced by other
stages must come after the respective stages.

```shell
observe-csv export [OPTIONS] CONFIG_FILE OUTPUT_DIR
```

### Options


### --start-time
If given overwrites the start_time contained in the JSON config file. Must be given if it is not set in the config file.


### --end-time
If given overwrites the end_time contained in the JSON config file. Must be given if it is not set in the config file.


### --url
If given overwrites the URL to access observe in the JSON config file. If a $OBSERVE_URL environment variable is set, its value is used as a default.

### --user
If given overwrites the observe user to access observe in the JSON config file. If a $OBSERVE_USER environment variable is set, its value is used as a default.

### --token
If given overwrites the token to access Observe contained in the JSON file. If a $OBSERVE_TOKEN environment variable is set, its value is used as a default.


### --yes
If set does not require manual interaction: directories are created without confirmation and previously downloaded files are reused when applicable.


* **Default**

    False


### Arguments


### CONFIG_FILE
Required argument


### OUTPUT_DIR
Required argument

### full-example-dataset-config

Outputs a dataset JSON example config with all fields set.

```shell
observe-csv full-example-dataset-config [OPTIONS]
```

### full-example-worksheet-config

Outputs a worksheet JSON example config with all fields set.

```shell
observe-csv full-example-worksheet-config [OPTIONS]
```

### minimal-example-dataset-config

Outputs a minimal dataset JSON example config with optional fields nulled.

```shell
observe-csv minimal-example-dataset-config [OPTIONS]
```

### minimal-example-worksheet-config

Outputs a minimal worksheet JSON example config with optional fields nulled.

```shell
observe-csv minimal-example-worksheet-config [OPTIONS]
```
