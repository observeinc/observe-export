# Observe-Export
A python tool (and library) to easily export data as CSV or JSON from Observe worksheets and datasets from the command line, including files larger than the maximum size configurable in the UI (100k rows).

## Dependencies & Requirements

>**_NOTE:_** This package has, so far, only been tested on Linux (Ubuntu 20.04) and Mac OS X 11.2. 

#### Observe User Id
Your Observe User ID can be found by going to ```https://${CUSTOMER_ID}.observeinc.com/settings/members```

#### Observe Access Token
You will require an Observe Access Token which can be generated via ```curl```, additional details can be found in the [Observe Docs](https://docs.observeinc.com/en/latest/content/common-topics/FAQ.html#how-do-i-create-an-access-token-that-can-do-more-than-just-ingest-data)

```curl
curl -s \
  https://${OBSERVE_CUSTOMER}.observeinc.com/v1/login -d \
  '{"user_email":"you@your-company.com", "user_password":"so secret", \
  "tokenName":"My token name"}'
```

#### Observe Stage Identifier
If exporting a worksheet you will require the stage identifier for the stage of the worksheet that you'd like to export.
You can retrieve the stage identifier by engaging with Observe support. Otherwise if you'd like the last stage in the worksheet you can simply put ```_last```

#### Observe Dataset Identifier
If exporting the results of a dataset query you will need the Observe Dataset ID, this can be found in the URL of the Observe Dataset Landing Page, ```https://${CUSTOMER_ID}.observeinc.com/workspace/${WORKSPACE_ID}/dataset/(event|resource)/${DATASET_ID}```


| Name | Required | Version | Example |
|------|----------|---------|---------|
| <a name="requirement_python"></a> [python](#requirement\_python) | Yes | >= 3.8 | N/A |
| <a name="requirement_cURL"></a> [cURL](#requirement\_cURL) | Yes | N/A | N/A |
| <a name="requirement_CUSTOMER_ID"></a> [customer](#requirement\_CUSTOMER_ID_) | Yes | N/A | 12345 |
| <a name="requirement_accessToken"></a> [token](#requirement\_accessToken) | Yes | N/A | Ga21uay2vAGrzxfZHgJN4gNhuCBC9oKD |
| <a name="requirement_stage"></a> [stage](#requirement\_stage) | No | N/A | stage-123abc |
| <a name="requirement_dataset"></a> [dataset](#requirement\_dataset) | No | N/A | 41023123 |

## Installation

Python libraries should generally be installed in individual virtual environments to avoid clashes of dependencies.
Hence, you should first create a virtual environment, e.g., using ```python3.X -m venv venv``` which creates the
folder ```venv``` in the local directory. You may then activate this environment using ```source venv/bin/activate```.

To install observe-export it suffices to download the repository and execute the setup.py, e.g., via
``` pip install . ```. ```pip``` will automatically install all dependencies (currently, including development 
requirements as sphinx).

In case that you want to extend the functionality of this package, it makes sense to install it as
editable via ``` pip install . -e```

Once installed, you can readily use the exporter using the command ```observe-export```. 

## Usage

The following documentation is automatically generated from the source using sphinx.
You can create a HTML documentation by calling `make html` in the docs directory.

> **_NOTE:_** While this package comes with a command-line interface, you may also use its contents
as library.


### observe-export

This module allows to export data from Observe worksheets and datasets to CSV files.
Data can be exported via the command `observe-export export <config> <output_directory>`.

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
    "output_format": "json",
    "crawling_format": "json",
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
    "url": "xzy.observeinc.com",
    "user": "xzy",
    "token": "access token for user xzy on xzy.observeinc.com"
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
    "output_format": "json",
    "crawling_format": "json",
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
    "url": "xzy.observeinc.com",
    "user": "xzy",
    "token": "access token for user xzy on xzy.observeinc.com"
}
```

In the following, we shortly discuss the main ingredients:


* `datasource` specifies the destination the data should be loaded from. This may either be a dataset or a worksheet.
When a dataset is selected, some arbitrary OPAL may be given via `opal_query`. For worksheet exports the stage
must be specified via `stage`.
As Observe enforces a 100k limit on returned rows for single queries the user may specify a temporal granularity
via initial_interval_to_query_seconds\`  when it is likely that more than 100k will be returned.

**WARNING**: The value of `initial_interval_to_query_seconds` will dictate the query time window. Do not use this setting,
i.e., leave it `null`, when you want to query aggregate data for example originating from a worksheet.

For dataset exports, you may specify that the completeness of the data shall be verified. Here, an additional
statsby query is executed to count the number of rows expected. If not all data was returned a warning is output
on stderr.
For worksheet queries this additional check is not available.


* `filename_prefix` specifies the prefix of the output file. Additional parts of the filename are the dataset source,

    and the start and end time.


* `output_format` specifies whether the exported data shall be written as CSV or JSON. Acceptable values are
“csv” or “json”.


* `crawling_format` specifies whether the data shall be exported as CSV or JSON. CSV exports run generally faster
due to the more compact data representation but may lead to problems when handling strings including commas or
quotes. JSON is more robust but will take longer to download. Acceptable values for this field are again
“csv” or “json”.


* `string_columns` specifies that the given columns are to be loaded explicitly as strings into when processing
the data. This is at times needed, as exported NULL values may otherwise be converted to NaN values by pandas
hence forcing the row to be converted to floats. This field is optional and can be omitted or set to `null`.


* `sort_keys` specifies the columns according to which the data is to be sorted before being stored in the csv file.

    This field is optional and can be omitted or set to `null`.


* `columns_to_keep` can be used to filter down the output columns.

**NOTE**: Instead of filtering the data after having downloaded it, for datasets it makes sense to include a `pickl_col`
directive in the OPAL such that only the data really needed is downloaded.


* `start_time` defines the start of the query window and must be given according to the UTC timezone.


* `start_time` defines the end of the query window and must be given according to the UTC timezone.


* `url` defines the observe url to query the data from. It should not contain "https://".


* `user` specifies the customer id, i.e., the first part of the url before `observeinc.com`; as this is part of the url already, specifying it is optional. 


* `token` the access token provisioned for the user to export the data/

**NOTE**: The following keys are optional: `initial_interval_to_query_seconds`, `comment`, `string_columns`,
`sort_keys`, `columns_to_keep`. Within the config these may either be set to  `null`, e.g.,
`"string_columns": null`, or may not be specified at all.

**NOTE**: One may also omit setting all runtime specific settings, namely, `output_format`, `crawling_format`,
`start_time`, `end_time`, `url`, `user`, and `token`. If these are `null`’ed or omitted the
respective values must be provided via options when calling the `observe-export export` command.

**WARNING**: Dates and times are always interpreted as UTC times.

To get the user started with writing configurations example configurations some samples can be glanced at using
the following commands:


* `observe-export minimal-example-dataset-config` or `observe-export full-example-dataset-config`


* `observe-export minimal-example-worksheet-config` or `observe-export full-example-worksheet-config`

```shell
observe-export [OPTIONS] COMMAND [ARGS]...
```

#### export

Performs an export of data via the given JSON config which specifies the input datasource and a set of additional
postprocessing steps. Please see `observe-export --help` for a description of the configuration file format.

If the configuration does not specify either of the following attributes, then these must be specified via
the respective options: `output_format`, `crawling_format`, `start_time`, `end_time`, `url`, `user`,
`token`. For the `start_time` and `end_time` the following formats may be used:
[%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]. Note that any time entered is interpreted as UTC.

When large quantities of data are to be exported the user should specify the `initial_interval_to_query_seconds`
within the JSON config such that the time range to query starts off with something reasonable.

**WARNING**: When the dataset or worksheet contains already aggregated data (for example some statistics of error types),
then the `initial_interval_to_query_seconds` should NOT be set, as otherwise the data is
downloaded for various time intervals and then afterwards aggregated, which probably was is not the intended
use case.

When exporting chunks of data due to the 100k row limitation intermediate results are written in temporary files
which are afterwards aggregated. The export configuration allows to perform some post-processing:


* enforce columns to be interpreted as strings via the `string_columns` list


* filter columns via the `columns_to_keep` entry


* sort the rows by (a list of columns) via the `sort_keys` entry

**NOTE**: When using CSV as output format the exported data is always fully quoted to avoid ambiguities when reading it
in other applications.

```shell
observe-export export [OPTIONS] CONFIG_FILE OUTPUT_DIR
```

### Options


### --output-format( <output_format>)
If given overwrites the output_format field contained in the JSON config. Must be given if it is not set in the config file. Must either be “json’ or “csv’.


### --crawling-format( <crawling_format>)
If given overwrites the crawling_format field contained in the JSON config. If not given and not specified in the JSON config “json” is used. Must either be “json’ or “csv’.


### --start-time( <start_time>)
If given overwrites the start_time contained in the JSON config file. Must be given if it is not set in the config file.


### --start-time( <start_time>)
If given overwrites the start_time contained in the JSON config file. Must be given if it is not set in the config file.


### --end-time( <end_time>)
If given overwrites the end_time contained in the JSON config file. Must be given if it is not set in the config file.


### --url( <url>)
If given overwrites the URL to access observe in the JSON config file. If a $OBSERVE_URL environment variable is set, its value is used as a default.


* **Default**

    <function <lambda> at 0x10dc725e0>



### --user( <user>)
If given overwrites the observe user to access observe in the JSON config file. If a $OBSERVE_USER environment variable is set, its value is used as a default.


* **Default**

    <function <lambda> at 0x10dc72700>



### --token( <token>)
If given overwrites the token to access Observe contained in the JSON file. If a $OBSERVE_TOKEN environment variable is set, its value is used as a default.


* **Default**

    <function <lambda> at 0x10dc72820>



### --yes()
If set does not require manual interaction: directories are created without confirmation and previously downloaded files are reused when applicable.


* **Default**

    False



### --log-curl-statements()
If set the executed ‘curl’ commands for retrieving data are logged on stdout.


* **Default**

    False


### Arguments


### CONFIG_FILE()
Required argument


### OUTPUT_DIR()
Required argument

#### full-example-dataset-config

Outputs a dataset JSON example config with all fields set.

```shell
observe-export full-example-dataset-config [OPTIONS]
```

#### full-example-worksheet-config

Outputs a worksheet JSON example config with all fields set.

```shell
observe-export full-example-worksheet-config [OPTIONS]
```

#### minimal-example-dataset-config

Outputs a minimal dataset JSON example config with optional fields nulled.

```shell
observe-export minimal-example-dataset-config [OPTIONS]
```

#### minimal-example-worksheet-config

Outputs a minimal worksheet JSON example config with optional fields nulled.

```shell
observe-export minimal-example-worksheet-config [OPTIONS]
```

## Code Documentation


### _class_ observe_export.observe_export.DataSourceDataset(dataset: int, opal_query: Optional[str] = None, initial_interval_to_query_seconds: Optional[int] = None, check_completeness_of_data: Optional[bool] = None, comment: Optional[str] = '')
Specifies that a dataset data should be retrieved.
You may specify an arbitrary opal_query as string. If none is provided, then the dataset is returned as it.
If the initial_interval_to_query_seconds is specified, the dataset will be queried in chunks starting at the given size.
If the initial_interval_to_query_seconds is None the whole dataset will be queried as it.
If check_completeness_of_data is set to True, the query will be repeated with an appended statsby count
to see whether there is any gap between the exported data and the data that should have been there.
The comment attribute can be set to help creators set attribute the datasource with a meaningful name.


### _class_ observe_export.observe_export.DataSourceWorksheet(worksheet: int, stage: str, initial_interval_to_query_seconds: Optional[int] = None, comment: Optional[str] = '')
Specifies that worksheet data from the given stage name should be retrieved.
If the initial_interval_to_query_seconds is specified, the worksheet will be queried in chunks starting at the given size.
If the initial_interval_to_query_seconds is None the whole worksheet will be queried as it.
The comment attribute can be set to help creators set attribute the datasource with a meaningful name.


### _class_ observe_export.observe_export.ExportConfig(datasource: Union[DataSourceDataset, DataSourceWorksheet], filename_prefix: str, output_format: str = '', crawling_format: str = 'json', string_columns: Optional[List[str]] = None, sort_keys: Optional[List[str]] = None, columns_to_keep: Optional[List[str]] = None, start_time: Optional[datetime] = '1970-01-01T01:00:00', end_time: Optional[datetime] = '1970-01-01T01:00:00', url: Optional[str] = None, user: Optional[str] = None, token: Optional[str] = None, log_curl_statements: Optional[bool] = None)
Specifies a data source to export. The datasource can either be a dataset (with an OPAL query) or a worksheet.
The exported data will be written into a file prefixed by the one given in the configuration.
Valid output formats are ‘csv’ and ‘json’. You may also specify the format with which the data is crawled.
CSV is generally much more compact but is troublesome when dealing with strings containing quotations or
commas. JSON is more robust but file are larger, slowing down the downloading process. By default JSON is used.

The other fields have the following meaning:


* string_columns: forces columns included in the list to be interpreted as strings. This is particularly helpful
when dealing with NULLs in the export that would otherwise force pandas to interpret the column as floats to set
NaN values.


* sort_keys: can be used (optional) to sort the output of the export via the given column names (ascending order).


* columns_to_keep: specifies which columns to export (optional); especially useful for worksheets.


* start_time: the start time of data to be queried from observe.


* end_time: the end time of data to be queried from observe.


* url: the observe url to query the data from. It should not contain "https://".


* user: it specifies the customer id, i.e., the first part of the url before `observeinc.com`; as this is part of the url already, specifying it is optional. 


* token: the observe access token to use for exporting the data.


* log_curl_statements: is a field set at runtime and is not read when parsing the configuration due to the leaking
of tokens


### _class_ observe_export.observe_export.Format(value)
An enumeration.


### observe_export.observe_export.ask_for_necessary_creation_of_directory(dir: Path, auto_create: bool = False)
Simple helper to ask for the creation of a directory, unless auto_create is set to true, in which case
the directory  is just created.


* **Parameters**

    
    * **dir** – the directory to create


    * **auto_create** – whether to not ask the user to confirm the creation of the directory



* **Returns**

    None



### observe_export.observe_export.check_completeness_of_config(ec: ExportConfig)
Checks that all essential fields that are optional are provided;
raises ValueError if some parameter is missing
:param ec: config to be checked
:return:


### observe_export.observe_export.crawl(output_dir: Path, file_prefix: str, file_suffix: str, start_time: datetime, end_time: datetime, initial_interval_in_seconds: Optional[int], get_crawling_command: Callable[[datetime, datetime, Path], str], crawling_format: Format, few_lines_warning: bool = False, yes: bool = False, log_curl_statements: bool = False)
This function perform a series of crawl commands (defined by the get_crawling_command callback) to crawl
either a dataset or worksheet from Observe. Multiple crawl commands may be executed as Observe’s CSV export
only allows to obtain 100k rows at a time. For larger datasets/worksheets, the requested time period
is split into smaller sub-intervals which are adaptively queried.
The downloaded csv files are written to disk and returned as a list of paths to be aggregated by another function.


* **Parameters**

    
    * **output_dir** – the directory to create the downloaded csv files in


    * **file_prefix** – the prefix of the files to create


    * **file_suffix** – the suffix (including the extension) of the files to create


    * **start_time** – the overall start time of the data we want to retrieve


    * **end_time** – the overall end time of the data we want to retrieve


    * **initial_interval_in_seconds** – if given, defines the initial query window


    * **get_crawling_command** – function which returns the actual crawling command


    * **few_lines_warning** – whether to warn about few returned rows or not


    * **yes** – whether to answer all potential questions per default with yes


    * **log_curl_statements** – whether executed curl commands are to be logged on stdout



* **Returns**

    the list of paths that were downloaded from Observe



### observe_export.observe_export.floor_timedelta_to_seconds(td: timedelta)
Removes any sub-second time from the timedelta


* **Parameters**

    **td** – input timedelta



* **Returns**

    the timedelta with any sub-second time removed



### observe_export.observe_export.get_curl_dataset_payload(dataset_id: int, pipeline_steps: List[str])
returns a “curl-passable” payload for requesting data from a dataset under the given OPAL pipeline_steps


* **Parameters**

    
    * **dataset_id** – the id of the dataset to query


    * **pipeline_steps** – the list of OPAL commands to perform to shape / filter the dataset’s data. Commands are to be
    passed as individual strings in this list.



* **Returns**

    a payload that can be passed to curl



### observe_export.observe_export.get_first_line_and_line_count(file: Path)

* **Parameters**

    **file** – a file to read



* **Returns**

    the first line of the file and the number of remaining lines



### observe_export.observe_export.get_iso_file_format(time: datetime)
Returns a given datetime in iso format where dashes and colons are replaced by underscores.


* **Parameters**

    **time** – 



* **Returns**

    


### observe_export.observe_export.get_iso_format(time: datetime)
Returns a given datetime in iso format as string


* **Parameters**

    **time** – 



* **Returns**

    .




### observe_export.observe_export.get_output_file_globbing_template(output_dir: Path, file_prefix: str, file_suffix: str)
Returns a canonical path with the given parameters where the start and end times are replaced with a globbing
expression (?).


* **Parameters**

    
    * **output_dir** – the path for the globbing expression


    * **file_prefix** – the prefix of the file


    * **file_suffix** – the suffix of the file to create



* **Returns**

    path with globbing filename



### observe_export.observe_export.get_output_file_simple(output_dir: Path, file_prefix: str, start_time: datetime, end_time: datetime, file_suffix: str)
Returns a canonical path based on the parameters


* **Parameters**

    
    * **output_dir** – path to create the file in


    * **file_prefix** – the prefix of the file


    * **start_time** – the start time which will be formatted in ISO format


    * **end_time** – the end time which will be formatted in ISO format


    * **file_suffix** – the suffix of the file to create



* **Returns**

    


### observe_export.observe_export.load_export_config_from_file(file: Path)
Given a path reads the JSON and resolves Union types that otherwise would need “hidden” identifier fields.


* **Parameters**

    **file** – 



* **Returns**

    the ExportConfig pertaining to the path passed as argument



### observe_export.observe_export.parse_dates_from_filename(file: Path)
Extracts the start and end time from an existing file.


* **Parameters**

    **file** – 



* **Returns**

    tuple of start and end time



### observe_export.observe_export.post_process_csv(output_dir: Path, file_prefix: str, input_files: List[Path], ec: ExportConfig, remove_input_files: Optional[bool] = False)
Performs some post-processing on a (list of) CSV files:


* merges all input files


* if a time_column was given filters rows to lie in the given interval [start_time, end_time]


* if a sort_keys list is given in the export configuration, sorts all values according to the given values (ascendingly)


* if remove_input_files is set, removes all input files


* if the export configuration has actions defined, executes the respective actions


* **Parameters**

    
    * **output_dir** – the directory to create the output file in


    * **file_prefix** – the prefix of the file to create


    * **input_files** – the list of all input files to be read (and merged)


    * **ec** – the export configuration that the post-processing pertains to, important for the output filename



* **Returns**

    the part of the (main) created file



### observe_export.observe_export.process_dataset_config(output_dir: Path, ec: ExportConfig, yes: bool)
Processes a single DATASET export configuration and executes all actions specified by it (downloading, processing).


* **Parameters**

    
    * **output_dir** – the directory the output files shall be stored in


    * **ec** – the export configuration to handle


    * **yes** – auto-confirm all otherwise asked questions



* **Returns**

    the path of the csv file created



### observe_export.observe_export.process_export_config(output_dir: Path, ec: ExportConfig, yes: bool)
Handle an export config according to its datasource.


* **Parameters**

    
    * **output_dir** – the directory to create the files in


    * **ec** – the actual export configuration


    * **yes** – auto-confirm all otherwise asked questions



* **Returns**

    None



### observe_export.observe_export.process_worksheet_config(output_dir: Path, ec: ExportConfig, yes: bool)
Processes a single WORKSHEET export configuration for downloading it.


* **Parameters**

    
    * **output_dir** – the directory the output files shall be stored in


    * **ec** – the export configuration to handle


    * **yes** – auto-confirm all otherwise asked questions



* **Returns**

    the path of the csv file created



### observe_export.observe_export.remove_files(file_or_files: Union[Path, Iterable[Path]])

* **Parameters**

    **file_or_files** – either a single path pointing to a file or a collection of paths



* **Returns**

    None



### observe_export.observe_export.replace_tilde_with_home_directory(path: Union[str, Path])
Resolves the home directory if encountered in a path


* **Parameters**

    **path** – 



* **Returns**

    absolute path with the home directory resolved
