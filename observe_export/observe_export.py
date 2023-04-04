import copy
import csv
import enum
import subprocess
import sys
import traceback
import warnings
from typing import Dict, Union, Set, Tuple, List, Optional, Callable, Iterable, Any
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
import dataclasses_json
import marshmallow
import os
from datetime import datetime, timedelta, date
import json
import re
import time
import pandas as pd
import click
from pathlib import Path

########################
#    Some Utilities
########################

def replace_tilde_with_home_directory(path: Union[str, Path]) -> Path:
    """
    Resolves the home directory if encountered in a path

    :param path:

    :return: absolute path with the home directory resolved
    """
    path = str(path)
    if "~" in path:
        path = path.replace(
            "~", str(Path.home().resolve()))
    return Path(path).resolve()


def parse_dates_from_filename(file: Path) -> Tuple[datetime, datetime]:
    """
    Extracts the start and end time from an existing file.

    :param file:

    :return: tuple of start and end time
    """
    split_names = file.stem.split("__")
    if len(split_names) < 4:
        raise ValueError(f"Cannot extract datetimes from file name: {file.name}")
    start_time = split_names[2]
    end_time = split_names[3].replace("_temp", "")
    start_time = datetime.strptime(start_time, "%Y_%m_%dT%H_%M_%SZ")
    end_time = datetime.strptime(end_time, "%Y_%m_%dT%H_%M_%SZ")
    return start_time, end_time


def ask_for_necessary_creation_of_directory(dir: Path, auto_create: bool = False) -> None:
    """
    Simple helper to ask for the creation of a directory, unless auto_create is set to true, in which case
    the directory  is just created.

    :param dir: the directory to create
    :param auto_create: whether to not ask the user to confirm the creation of the directory

    :return: None
    """
    if not dir.exists():
        if auto_create or click.confirm(f"The path {dir.resolve()} does not exist, shall we create it?"):
            dir.mkdir(parents=True)
        else:
            raise ValueError(f"Directory {dir.resolve()} does not exist.")


def get_iso_format(time: datetime) -> str:
    """
    Returns a given datetime in iso format as string

    :param time:

    :return: .
    """
    return time.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_iso_file_format(time: datetime) -> str:
    """
    Returns a given datetime in iso format where dashes and colons are replaced by underscores.

    :param time:

    :return:
    """
    return time.strftime('%Y_%m_%dT%H_%M_%SZ')



def get_output_file_simple(output_dir: Path, file_prefix: str, start_time: datetime, end_time: datetime,
                           file_suffix: str) -> Path:
    """
    Returns a canonical path based on the parameters

    :param output_dir: path to create the file in
    :param file_prefix: the prefix of the file
    :param start_time: the start time which will be formatted in ISO format
    :param end_time: the end time which will be formatted in ISO format
    :param file_suffix: the suffix of the file to create

    :return:
    """
    return Path(output_dir,
                f"{file_prefix}"
                f"{get_iso_file_format(start_time)}__"
                f"{get_iso_file_format(end_time)}"
                f"{file_suffix}")


def get_output_file_globbing_template(output_dir: Path, file_prefix: str, file_suffix: str) -> Path:
    """
    Returns a canonical path with the given parameters where the start and end times are replaced with a globbing
    expression (?).

    :param output_dir: the path for the globbing expression
    :param file_prefix: the prefix of the file
    :param file_suffix: the suffix of the file to create

    :return: path with globbing filename
    """
    pattern = get_iso_file_format(datetime.fromtimestamp(0))
    pattern = re.sub('\d', "?", pattern)
    return Path(output_dir,
                f"{file_prefix}"
                f"{pattern}__"
                f"{pattern}"
                f"{file_suffix}")


def floor_timedelta_to_seconds(td: timedelta) -> timedelta:
    """
    Removes any sub-second time from the timedelta

    :param td: input timedelta

    :return: the timedelta with any sub-second time removed
    """
    return timedelta(seconds=int(td.total_seconds()))


def get_first_line_and_line_count(file: Path) -> Tuple[str, int]:
    """
    :param file: a file to read

    :return: the first line of the file and the number of remaining lines
    """
    with open(file) as f:
        first_line = f.readline()
        number_of_lines = len(f.readlines())
        return first_line, number_of_lines


def remove_files(file_or_files: Union[Path, Iterable[Path]]) -> None:
    """
    :param file_or_files: either a single path pointing to a file or a collection of paths

    :return: None
    """
    if isinstance(file_or_files, Path):
        print(f"\tremoving file {file_or_files.resolve()}")
        file_or_files.unlink()
    else:
        for file in file_or_files:
            print(f"\tremoving file {file.resolve()}")
            file.unlink()


###########################################################################
#    Definition of JSON parsable dataclasses to store configuration in
###########################################################################

@dataclass_json
@dataclass
class DataSourceDataset:
    """
    Specifies that a dataset data should be retrieved.
    You may specify an arbitrary opal_query as string. If none is provided, then the dataset is returned as it.
    If the initial_interval_to_query_seconds is specified, the dataset will be queried in chunks starting at the given size.
    If the initial_interval_to_query_seconds is None the whole dataset will be queried as it.
    If check_completeness_of_data is set to True, the query will be repeated with an appended statsby count
    to see whether there is any gap between the exported data and the data that should have been there.
    The comment attribute can be set to help creators set attribute the datasource with a meaningful name.
    """
    dataset: int
    opal_query: Optional[str] = None
    initial_interval_to_query_seconds: Optional[int] = None
    check_completeness_of_data: Optional[bool] = None
    comment: Optional[str] = ""


@dataclass_json
@dataclass
class DataSourceWorksheet:
    """
    Specifies that worksheet data from the given stage name should be retrieved.
    If the initial_interval_to_query_seconds is specified, the worksheet will be queried in chunks starting at the given size.
    If the initial_interval_to_query_seconds is None the whole worksheet will be queried as it.
    The comment attribute can be set to help creators set attribute the datasource with a meaningful name.
    """
    worksheet: int
    stage: str
    initial_interval_to_query_seconds: Optional[int] = None
    comment: Optional[str] = ""

class Format(enum.Enum):
    CSV = "CSV"
    JSON = "JSON"
    INVALID = "INVALID"

    @staticmethod
    def from_string(string: str) -> 'Format':
        if string == "json":
            return Format.JSON
        elif string == "csv":
            return Format.CSV
        else:
            return Format.INVALID


# dataclass_json cannot handle None's as default for datetime fields, so we use the following to denote an
# unitializaed datetime
uninitialized_datetime = datetime.fromtimestamp(0)

@dataclass_json
@dataclass()
class ExportConfig:
    """
    Specifies a data source to export. The datasource can either be a dataset (with an OPAL query) or a worksheet.
    The exported data will be written into a file prefixed by the one given in the configuration.
    Valid output formats are 'csv' and 'json'. You may also specify the format with which the data is crawled.
    CSV is generally much more compact but is troublesome when dealing with strings containing quotations or
    commas. JSON is more robust but file are larger, slowing down the downloading process. By default JSON is used.

    The other fields have the following meaning:

    - string_columns: forces columns included in the list to be interpreted as strings. This is particularly helpful
      when dealing with NULLs in the export that would otherwise force pandas to interpret the column as floats to set
      NaN values.
    - sort_keys: can be used (optional) to sort the output of the export via the given column names (ascending order).
    - columns_to_keep: specifies which columns to export (optional); especially useful for worksheets.
    - start_time: the start time of data to be queried from observe.
    - end_time: the end time of data to be queried from observe.
    - url: the observe url to query the data from.
    - user: the observe user for querying the data.
    - token: the observe access token to use for exporting the data.
    - log_curl_statements: is a field set at runtime and is not read when parsing the configuration due to the leaking
      of tokens
    """
    datasource: Union[DataSourceDataset, DataSourceWorksheet]
    filename_prefix: str
    output_format: str = ""
    crawling_format: str = "json"
    string_columns: Optional[List[str]] = None
    sort_keys: Optional[List[str]] = None
    columns_to_keep: Optional[List[str]] = None
    start_time: Optional[datetime] = field(
        metadata=dataclasses_json.config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=marshmallow.fields.DateTime(format='iso8601')
        ),
        default=uninitialized_datetime.isoformat())
    end_time: Optional[datetime] = field(
        metadata=dataclasses_json.config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=marshmallow.fields.DateTime(format='iso8601')
        ),
        default=uninitialized_datetime.isoformat())
    url: Optional[str] = None
    user: Optional[str] = None
    token: Optional[str] = None
    log_curl_statements: Optional[bool] = None

    def get_output_format(self) -> Format:
        return Format.from_string(self.output_format)

    def get_crawling_format(self) -> Format:
        return Format.from_string(self.crawling_format)


def get_curl_dataset_payload(dataset_id: int, pipeline_steps: List[str]) -> str:
    """
    returns a "curl-passable" payload for requesting data from a dataset under the given OPAL pipeline_steps

    :param dataset_id: the id of the dataset to query
    :param pipeline_steps: the list of OPAL commands to perform to shape / filter the dataset's data. Commands are to be
        passed as individual strings in this list.

    :return: a payload that can be passed to curl
    """
    payload = {'query': {'stages': [{'stageID': 'first', 'input': [{'inputName': 'in', 'datasetId': f'{dataset_id}'}],
                                     'pipeline': " | ".join(pipeline_steps)}], 'outputStage': 'first'}}
    payload = "-d '{}'".format(str(payload).replace('"', '\\"').replace("'", "\""))
    return payload


##########################
#    Actual export code
##########################

def crawl(output_dir: Path,
          file_prefix: str,
          file_suffix: str,
          start_time: datetime,
          end_time: datetime,
          initial_interval_in_seconds: Optional[int],
          get_crawling_command: Callable[[datetime, datetime, Path], str],
          crawling_format: Format,
          few_lines_warning: bool = False,
          yes: bool = False,
          log_curl_statements: bool = False) -> List[Path]:
    """
    This function perform a series of crawl commands (defined by the get_crawling_command callback) to crawl
    either a dataset or worksheet from Observe. Multiple crawl commands may be executed as Observe's CSV export
    only allows to obtain 100k rows at a time. For larger datasets/worksheets, the requested time period
    is split into smaller sub-intervals which are adaptively queried.
    The downloaded csv files are written to disk and returned as a list of paths to be aggregated by another function.

    :param output_dir: the directory to create the downloaded csv files in
    :param file_prefix: the prefix of the files to create
    :param file_suffix: the suffix (including the extension) of the files to create
    :param start_time: the overall start time of the data we want to retrieve
    :param end_time: the overall end time of the data we want to retrieve
    :param initial_interval_in_seconds: if given, defines the initial query window
    :param get_crawling_command: function which returns the actual crawling command
    :param few_lines_warning: whether to warn about few returned rows or not
    :param yes: whether to answer all potential questions per default with yes
    :param log_curl_statements: whether executed curl commands are to be logged on stdout

    :return: the list of paths that were downloaded from Observe
    """
    files_downloaded = []

    # lookup existing files and use them such that we don't need to download stuff again.
    output_file_template = get_output_file_globbing_template(output_dir, file_prefix, file_suffix)
    existing_files = []
    for file in output_file_template.parent.glob(output_file_template.name):
        start_time_of_file, end_time_of_file = parse_dates_from_filename(file)
        existing_files.append((file, start_time_of_file, end_time_of_file))

    # sort by timestamps
    existing_files = sorted(existing_files, key=lambda x: x[1])
    matching_files = []
    temp_start_time = start_time
    for (file, start_time_of_file, end_time_of_file) in existing_files:
        if start_time_of_file < temp_start_time:
            continue
        if temp_start_time == start_time_of_file and end_time_of_file <= end_time:
            temp_start_time = end_time_of_file
            matching_files.append(file)
        else:
            break

    if temp_start_time != start_time:
        files_string = "\n\t".join(map(lambda x: str(x.resolve()), matching_files))
        if yes:
            print("Found existing files seemingly downloaded before and will re-use them: \n\t{files_string}\n\n")
        elif click.confirm(f"Found existing files seemingly downloaded before:\n\t{files_string}\n\nDo you want to reuse these? "
                      f"(If you select No these will be downloaded again)"):
            start_time = temp_start_time
            files_downloaded = matching_files

    current_start_time = start_time
    if initial_interval_in_seconds is None:
        current_interval = end_time - start_time
    else:
        current_interval = timedelta(seconds=initial_interval_in_seconds)

    # perform the actual crawling in smaller time intervals
    while current_start_time < end_time:
        current_end_time = current_start_time + current_interval
        if current_end_time > end_time:
            current_end_time = end_time

        output_file = get_output_file_simple(output_dir, file_prefix, current_start_time, current_end_time, file_suffix)
        actual_command_to_execute = get_crawling_command(current_start_time, current_end_time, output_file)

        if log_curl_statements:
            print(f"going to execute the following curl command:\n{actual_command_to_execute}")
        # execute command using subprocess!
        exec_start = time.time()
        subprocess.run(actual_command_to_execute, shell=True, check=True)
        exec_finish = time.time()
        # check the number of lines of the file
        first_line, number_of_lines = get_first_line_and_line_count(output_file)
        if crawling_format == Format.CSV:
            if first_line.startswith("{") and number_of_lines == 1:
                # we've got a JSON result, probably indiciating an error:
                print(f"\tfile's {output_file.resolve()} first line seemingly contains JSON: {first_line}")
                print(f"\twill assume that this is an error and abort")
                sys.exit(1)
        else:
            if first_line.startswith('{"ok":false,"message"') and number_of_lines == 1:
                # this generally indicates an error unless the user has constructed a dataset/worksheet exporting
                # exactly this output. TODO: error handling should be improved in the future here.
                print(f"\tfile's {output_file.resolve()} first line {first_line} was interpreted as error message.")
                print(f"\taborting execution")
                sys.exit(1)

        print(f"\tgot {number_of_lines} rows for period of {current_start_time} to {current_end_time} "
              f"(interval {current_interval}) in {exec_finish - exec_start:.2f} seconds")
        if (number_of_lines >= 100000 and crawling_format == Format.CSV) or\
                (number_of_lines >= 99999 and crawling_format == Format.JSON):  #JSON doesn't have the header file
            current_interval = floor_timedelta_to_seconds(current_interval / 2)
            print(f"\tsaw more than 100k lines. Going to retry with a halved time interval: {current_interval}")
            if current_interval.total_seconds() < 1.0:
                raise RuntimeError("Too much data. Cannot crawl for durations less than 1s. Aborting.")
            # remove the file just downloaded
            output_file.unlink()
            continue

        if number_of_lines < 10 and few_lines_warning:
            ## this could mean we don't have data for this time
            ## or someone pressed CTRL-C, or something else went wrong
            warnings.warn("Warning: got only few results\n"
                          f"\t       call: {actual_command_to_execute}\n"
                          f"\toutput file: {str(output_file.resolve())}\n"
                          f"\t      lines: {number_of_lines}")
        # everything checked out
        files_downloaded.append(output_file)
        current_start_time = current_end_time
        # adapt the time interval to query
        if number_of_lines > 0:
            next_interval = current_interval.total_seconds() * (50000.0 / float(number_of_lines))
            next_interval = floor_timedelta_to_seconds(timedelta(seconds=next_interval))
            if next_interval > 2 * current_interval:
                # at most double the interval
                current_interval = 2 * current_interval
            else:
                current_interval = next_interval

    return files_downloaded


def post_process_csv(output_dir: Path,
                     file_prefix: str,
                     input_files: List[Path],
                     ec: ExportConfig,
                     remove_input_files: Optional[bool] = False,
                     ) -> Path:
    """
    Performs some post-processing on a (list of) CSV files:

    - merges all input files
    - if a time_column was given filters rows to lie in the given interval [start_time, end_time]
    - if a sort_keys list is given in the export configuration, sorts all values according to the given values (ascendingly)
    - if remove_input_files is set, removes all input files
    - if the export configuration has actions defined, executes the respective actions

    :param output_dir: the directory to create the output file in
    :param file_prefix: the prefix of the file to create
    :param input_files: the list of all input files to be read (and merged)
    :param ec: the export configuration that the post-processing pertains to, important for the output filename

    :return: the part of the (main) created file
    """
    output_df = None
    read_dfs = []
    # load all data and merge it using pandas
    for file in input_files:
        print(f"\treading file contents of {file.resolve()} ..")
        dtype = {}
        if ec.string_columns is not None:
            dtype.update({
                column_name: str for column_name in ec.string_columns
            })
        if ec.get_crawling_format() == Format.CSV:
            input_df = pd.read_csv(file, header=0, sep=",", quotechar='"', doublequote=False, escapechar='\\', dtype=dtype)
        else:
            input_df = pd.read_json(file, lines=True, dtype=dtype)
        if ec.columns_to_keep is not None:
            input_df = input_df[ec.columns_to_keep]
        read_dfs.append(input_df)
    if len(input_files) > 1:
        print(f"\tgoing to merge all read dataframes now ..")
    output_df = pd.concat(read_dfs)

    if ec.sort_keys is not None:
        print(f"\tgoing to sort values in output dataframe according to the columns {ec.sort_keys}")
        output_df.sort_values(ec.sort_keys, inplace=True)

    aggregated_result_file = get_output_file_simple(output_dir, file_prefix, ec.start_time, ec.end_time, "."+ec.output_format)
    print(f"\tgoing to write output file {aggregated_result_file.resolve()}")
    if ec.get_output_format() == Format.CSV:
        output_df.to_csv(aggregated_result_file, index=False, quoting=csv.QUOTE_ALL)
    else:
        output_df.to_json(aggregated_result_file, orient='records', lines=True)
    if remove_input_files:
        remove_files(input_files)

    return aggregated_result_file


def process_dataset_config(output_dir: Path, ec: ExportConfig, yes: bool) -> Path:
    """
    Processes a single DATASET export configuration and executes all actions specified by it (downloading, processing).

    :param output_dir: the directory the output files shall be stored in
    :param ec: the export configuration to handle
    :param yes: auto-confirm all otherwise asked questions

    :return: the path of the csv file created
    """

    if not isinstance(ec.datasource, DataSourceDataset):
        raise ValueError(f"Was expecting to be passed a dataset config but got {ec.datasource}")
    ds: DataSourceDataset = ec.datasource

    crawling_format = ec.get_crawling_format()

    pipeline_steps = ""
    if ds.opal_query is not None and ds.opal_query != "":
        pipeline_steps = ds.opal_query.split("\n")
    else:
        pipeline_steps = []

    # we will perform a query request
    payload = get_curl_dataset_payload(ds.dataset, pipeline_steps)
    curl_str = f'curl -H "Authorization: Bearer {ec.user} {ec.token}"'
    if crawling_format == Format.CSV:
        conf_str = "-H 'Accept: text/csv' -H 'Content-Type: application/json'"
    else:
        conf_str = "-H 'Accept: application/x-ndjson' -H 'Content-Type: application/json'"
    url_and_time ='https://' + ec.url + '/v1/meta/export/query?startTime={XYZstartTimeZYX}&endTime={XYZendTimeZYX}'
    url_and_time = f"\'{url_and_time}\'"

    def get_crawling_command_for_payload(payload_string: str) -> Callable:
        def get_crawling_command(start_time: datetime, end_time: datetime, output_file: Path) -> str:
            curl_command = f"{curl_str} {conf_str} "
            curl_command += url_and_time.format(
                XYZstartTimeZYX=get_iso_format(start_time),
                XYZendTimeZYX=get_iso_format(end_time))
            curl_command += " " + payload_string
            curl_command += f" > {str(output_file.resolve())}"
            return curl_command

        return get_crawling_command

    file_prefix = f"{ec.filename_prefix}__dataset{ds.dataset}__"
    if crawling_format == Format.CSV:
        file_suffix = "_temp.csv"
    else:
        file_suffix = "_temp.json"

    files_downloaded = crawl(output_dir,
                             file_prefix,
                             file_suffix,
                             ec.start_time,
                             ec.end_time,
                             ds.initial_interval_to_query_seconds,
                             get_crawling_command_for_payload(payload),
                             crawling_format,
                             yes=yes,
                             log_curl_statements=ec.log_curl_statements,
                             )
    main_result_file = post_process_csv(output_dir, file_prefix, files_downloaded, ec, remove_input_files=True)

    if ds.check_completeness_of_data:
        pipeline_steps.extend(["make_col TEMPTEMPTEMP: 0", "statsby count:count(1), group_by(TEMPTEMPTEMP)"])
        payload = get_curl_dataset_payload(ds.dataset, pipeline_steps)
        files_downloaded = crawl(output_dir,
                                 "correctness_check_" + file_prefix,
                                 file_suffix,
                                 ec.start_time,
                                 ec.end_time,
                                 None,
                                 get_crawling_command_for_payload(payload),
                                 crawling_format,
                                 few_lines_warning=False,
                                 yes=yes,
                                 log_curl_statements=ec.log_curl_statements,
                                 )
        if len(files_downloaded) != 1:
            raise ValueError(
                f"Was expecting to find exactly one csv file with the correctness data but found multiple: {files_downloaded}")
        correctness_file = files_downloaded[0]
        if crawling_format == Format.CSV:
            expected_number_of_lines = pd.read_csv(correctness_file, header=0)["count"][0]
        else:
            expected_number_of_lines = pd.read_json(correctness_file, lines=True)["count"][0]
        _, number_of_lines_exported = get_first_line_and_line_count(main_result_file)
        if expected_number_of_lines == number_of_lines_exported:
            print("\tCompleteness check passed!\n\t\t all rows were exported!")
        else:
            missing_percent = 1 - float(number_of_lines_exported) / expected_number_of_lines
            warnings.warn(f"\tCompleteness check failed \n\t\t.. only {number_of_lines_exported} rows were exported "
                          f"but should have exported {expected_number_of_lines}!"
                          f"\n\t\t.. we are missing {100 * missing_percent:.3f}% for {main_result_file.resolve()}!")
        remove_files(correctness_file)

    return main_result_file


def process_worksheet_config(output_dir: Path, ec: ExportConfig, yes: bool) -> Path:
    """
    Processes a single WORKSHEET export configuration for downloading it.

    :param output_dir: the directory the output files shall be stored in
    :param ec: the export configuration to handle
    :param yes: auto-confirm all otherwise asked questions

    :return: the path of the csv file created
    """

    if not isinstance(ec.datasource, DataSourceWorksheet):
        raise ValueError(f"Was expecting to be passed a worksheet config but got {ec.datasource}")
    ds: DataSourceWorksheet = ec.datasource

    crawling_format = ec.get_crawling_format()

    # we will perform a query request
    if crawling_format == Format.CSV:
        curl_str = f'curl -H "Authorization: Bearer {ec.user} {ec.token}" -H "Accept: text/csv" -H "Content-Type: application/json"'
    else:
        curl_str = f'curl -H "Authorization: Bearer {ec.user} {ec.token}" -H "Accept: application/x-ndjson" -H "Content-Type: application/json"'
    url = f"'https://{ec.url}/v1/meta/export/worksheet/{ds.worksheet}'"

    def get_worksheet_payload(start_time: datetime, end_time: datetime) -> str:
        payload = {
            "startTime": get_iso_format(start_time),
            "endTime": get_iso_format(end_time),
            "stage": ds.stage,
        }
        return "-d '{}'".format(str(payload).replace('"', '\\"').replace("'", "\""))

    def get_crawling_command(start_time: datetime, end_time: datetime, output_file: Path) -> str:
        curl_command = f"{curl_str} "
        curl_command += url
        curl_command += " " + get_worksheet_payload(start_time, end_time)
        curl_command += f" > {str(output_file.resolve())}"
        return curl_command

    file_prefix = f"{ec.filename_prefix}__worksheet{ds.worksheet}__"
    if crawling_format == Format.CSV:
        file_suffix = "_temp.csv"
    else:
        file_suffix = "_temp.json"

    files_downloaded = crawl(output_dir,
                             file_prefix,
                             file_suffix,
                             ec.start_time,
                             ec.end_time,
                             ds.initial_interval_to_query_seconds,
                             get_crawling_command,
                             crawling_format,
                             yes=yes,
                             log_curl_statements=ec.log_curl_statements,
                             )
    main_result_file = post_process_csv(output_dir, file_prefix, files_downloaded, ec, remove_input_files=True)

    return main_result_file


def process_export_config(output_dir: Path, ec: ExportConfig, yes: bool) -> None:
    """
    Handle an export config according to its datasource.

    :param output_dir: the directory to create the files in
    :param ec: the actual export configuration
    :param yes: auto-confirm all otherwise asked questions

    :return: None
    """
    if not isinstance(ec, ExportConfig):
        raise ValueError("Was expecting an ExportConfigs type (plural!)")
    result_file = None
    if isinstance(ec.datasource, DataSourceDataset):
        result_file = process_dataset_config(output_dir, ec, yes)
    elif isinstance(ec.datasource, DataSourceWorksheet):
        result_file = process_worksheet_config(output_dir, ec, yes)
    else:
        raise ValueError(f"Unknown datasource type {type(ec.datasource)} of export configuration {ec} ")
    if result_file is not None:
        print(f"Successfully exported the data to {str(result_file.resolve())}")


def load_export_config_from_file(file: Path) -> ExportConfig:
    """
    Given a path reads the JSON and resolves Union types that otherwise would need "hidden" identifier fields.

    :param file:

    :return: the ExportConfig pertaining to the path passed as argument
    """
    with open(file) as f:
        export_config: ExportConfig = ExportConfig.from_json(f.read())
    # remove default values by overwriting them with None; we wouldn't want to crawl from 1970...
    if export_config.start_time == uninitialized_datetime:
        export_config.start_time = None
    if export_config.end_time == uninitialized_datetime:
        export_config.end_time = None
    # resolve datasources as the this is not done correctly, unless one renders the JSON more ugly than necessary:
    # https://github.com/lidatong/dataclasses-json/issues/82
    if "dataset" in export_config.datasource:
        export_config.datasource = DataSourceDataset.from_dict(export_config.datasource)
    elif "worksheet" in export_config.datasource:
        export_config.datasource = DataSourceWorksheet.from_dict(export_config.datasource)
    else:
        raise ValueError(f"Could not resolve the following data source: {export_config.datasource}")
    return export_config


def check_completeness_of_config(ec: ExportConfig) -> None:
    """
    Checks that all essential fields that are optional are provided;
    raises ValueError if some parameter is missing
    :param ec: config to be checked
    :return:
    """
    def check_single_field(field: str, value: Any, forbidden_value: Any) -> List[str]:
        if value is None or value == forbidden_value:
            return [field]
        return []
    missing_fields = []
    missing_fields.extend(check_single_field("url", ec.url, ""))
    missing_fields.extend(check_single_field("user", ec.user, ""))
    missing_fields.extend(check_single_field("token", ec.token, ""))
    missing_fields.extend(check_single_field("output_format", ec.output_format, ""))
    missing_fields.extend(check_single_field("crawling_format", ec.crawling_format, ""))
    missing_fields.extend(check_single_field("start_time", ec.start_time, uninitialized_datetime))
    missing_fields.extend(check_single_field("end_time", ec.end_time, uninitialized_datetime))
    if len(missing_fields) > 0:
        raise ValueError("The following field(s) must be set either via the config or via the respective command-line "
                         f"parameter:\n            <{', '.join(missing_fields)}>")


@click.group()
def cli():
    """
    This module allows to export data from Observe worksheets and datasets to CSV files.
    Data can be exported via the command ``observe-export export <config> <output_directory>``.

    An example dataset export configuration may look like this:

    .. code-block:: json

        {
            "datasource": {
                "dataset": 12345678,
                "opal_query": "col_pick col1, col2, col3\\nfilter col1=100",
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

    An example worksheet export configuration may look like this:

    .. code-block:: json

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


    In the following, we shortly discuss the main ingredients:

    - ``datasource`` specifies the destination the data should be loaded from. This may either be a dataset or a worksheet.
      When a dataset is selected, some arbitrary OPAL may be given via ``opal_query``. For worksheet exports the stage
      must be specified via ``stage``.
      As Observe enforces a 100k limit on returned rows for single queries the user may specify a temporal granularity
      via `initial_interval_to_query_seconds``  when it is likely that more than 100k will be returned.

      .. warning::

        The value of ``initial_interval_to_query_seconds`` will dictate the query time window. Do not use this setting,
        i.e., leave it ``null``, when you want to query aggregate data for example originating from a worksheet.

      For dataset exports, you may specify that the completeness of the data shall be verified. Here, an additional
      statsby query is executed to count the number of rows expected. If not all data was returned a warning is output
      on stderr.
      For worksheet queries this additional check is not available.

    - ``filename_prefix`` specifies the prefix of the output file. Additional parts of the filename are the dataset source,
       and the start and end time.
    - ``output_format`` specifies whether the exported data shall be written as CSV or JSON. Acceptable values are
      "csv" or "json".
    - ``crawling_format`` specifies whether the data shall be exported as CSV or JSON. CSV exports run generally faster
      due to the more compact data representation but may lead to problems when handling strings including commas or
      quotes. JSON is more robust but will take longer to download. Acceptable values for this field are again
      "csv" or "json".
    - ``string_columns`` specifies that the given columns are to be loaded explicitly as strings into when processing
      the data. This is at times needed, as exported NULL values may otherwise be converted to NaN values by pandas
      hence forcing the row to be converted to floats. This field is optional and can be omitted or set to ``null``.
    - ``sort_keys`` specifies the columns according to which the data is to be sorted before being stored in the csv file.
       This field is optional and can be omitted or set to ``null``.
    - ``columns_to_keep`` can be used to filter down the output columns.

      .. note::

        Instead of filtering the data after having downloaded it, for datasets it makes sense to include a ``pickl_col``
        directive in the OPAL such that only the data really needed is downloaded.

    - ``start_time`` defines the start of the query window and must be given according to the UTC timezone.
    - ``start_time`` defines the end of the query window and must be given according to the UTC timezone.
    - ``url`` defines the observe url to query the data from.
    - ``user`` defines the user account to use for the export of the data.
    - ``token`` the access token provisioned for the user to export the data/

    .. note::

      The following keys are optional: ``initial_interval_to_query_seconds``, ``comment``, ``string_columns``,
      ``sort_keys``, ``columns_to_keep``. Within the config these may either be set to  ``null``, e.g.,
      ``"string_columns": null``, or may not be specified at all.

    .. note::

      One may also omit setting all runtime specific settings, namely, ``output_format``, ``crawling_format``,
      ``start_time``, ``end_time``, ``url``, ``user``, and ``token``. If these are ``null``'ed or omitted the
      respective values must be provided via options when calling the ``observe-export export`` command.

    .. warning::

        Dates and times are always interpreted as UTC times.


    To get the user started with writing configurations example configurations some samples can be glanced at using
    the following commands:

    - ``observe-export minimal-example-dataset-config`` or ``observe-export full-example-dataset-config``
    - ``observe-export minimal-example-worksheet-config`` or ``observe-export full-example-worksheet-config``

    """
    pass

@cli.command()
@click.argument("config-file", type=click.STRING)
@click.argument("output_dir", type=click.STRING)
@click.option('--output-format', type=click.STRING, default=None, show_default=True,
              help="If given overwrites the output_format field contained in the JSON config. "
                   "Must be given if it is not set in the config file. Must either be \"json\' or \"csv\'.")
@click.option('--crawling-format', type=click.STRING, default=None, show_default=True,
              help="If given overwrites the crawling_format field contained in the JSON config. "
                   "If not given and not specified in the JSON config \"json\" is used. "
                   "Must either be \"json\' or \"csv\'.")
@click.option('--start-time', type=click.DateTime(), default=None, show_default=True,
              help="If given overwrites the start_time contained in the JSON config file. "
                   "Must be given if it is not set in the config file.")
@click.option('--start-time', type=click.DateTime(), default=None, show_default=True,
              help="If given overwrites the start_time contained in the JSON config file. "
                   "Must be given if it is not set in the config file.")
@click.option('--end-time', type=click.DateTime(), default=None, show_default=True,
              help="If given overwrites the end_time contained in the JSON config file. "
                   "Must be given if it is not set in the config file.")
@click.option('--url', type=click.STRING, default=lambda: os.environ.get("OBSERVE_URL", None), show_default=True,
              help="If given overwrites the URL to access observe in the JSON config file. "
                   "If a $OBSERVE_URL environment variable is set, its value is used as a default.")
@click.option('--user', type=click.STRING, default=lambda: os.environ.get("OBSERVE_USER", None), show_default=True,
              help="If given overwrites the observe user to access observe in the JSON config file. "
                   "If a $OBSERVE_USER environment variable is set, its value is used as a default.")
@click.option('--token', type=click.STRING, default=lambda: os.environ.get("OBSERVE_TOKEN", None), show_default=True,
              help="If given overwrites the token to access Observe contained in the JSON file. "
                   "If a $OBSERVE_TOKEN environment variable is set, its value is used as a default.")
@click.option('--yes', is_flag=True, default=False, show_default=True,
              help="If set does not require manual interaction: directories are created without confirmation and "
                   "previously downloaded files are reused when applicable.")
@click.option('--log-curl-statements', is_flag=True, default=False, show_default=True,
              help="If set the executed 'curl' commands for retrieving data are logged on stdout.")
def export(config_file: str,
           output_dir: str,
           output_format: str,
           crawling_format: str,
           start_time: datetime,
           end_time: datetime,
           url: str,
           user: str,
           token: str,
           yes: bool,
           log_curl_statements: bool):
    """
    Performs an export of data via the given JSON config which specifies the input datasource and a set of additional
    postprocessing steps. Please see ``observe-export --help`` for a description of the configuration file format.

    If the configuration does not specify either of the following attributes, then these must be specified via
    the respective options: ``output_format``, ``crawling_format``, ``start_time``, ``end_time``, ``url``, ``user``,
    ``token``. For the ``start_time`` and ``end_time`` the following formats may be used:
    [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]. Note that any time entered is interpreted as UTC.

    When large quantities of data are to be exported the user should specify the ``initial_interval_to_query_seconds``
    within the JSON config such that the time range to query starts off with something reasonable.

    .. warning::

      When the dataset or worksheet contains already aggregated data (for example some statistics of error types),
      then the ``initial_interval_to_query_seconds`` should NOT be set, as otherwise the data is
      downloaded for various time intervals and then afterwards aggregated, which probably was is not the intended
      use case.

    When exporting chunks of data due to the 100k row limitation intermediate results are written in temporary files
    which are afterwards aggregated. The export configuration allows to perform some post-processing:

    - enforce columns to be interpreted as strings via the ``string_columns`` list

    - filter columns via the ``columns_to_keep`` entry

    - sort the rows by (a list of columns) via the ``sort_keys`` entry

    .. note::

        When using CSV as output format the exported data is always fully quoted to avoid ambiguities when reading it
        in other applications.

    """
    try:
        output_dir = replace_tilde_with_home_directory(output_dir)
        ec = load_export_config_from_file(Path(config_file))
        ask_for_necessary_creation_of_directory(output_dir, auto_create=yes)

        patched_config = False
        def patch_config_notification(name: str, value: str, hide: bool = False):
            nonlocal patched_config
            print(f"Have read the following {name} from the CLI and will overwrite value in export configuration: {value if not hide else '*'*len(value)}")
            patched_config = True

        if output_format is not None:
            ec.output_format = output_format
            patch_config_notification("output_format", str(output_format))
        if crawling_format is not None:
            ec.crawling_format = crawling_format
            patch_config_notification("crawling_format", str(crawling_format))
        if start_time is not None:
            ec.start_time = start_time
            patch_config_notification("start_time", str(start_time))
        if end_time is not None:
            ec.end_time = end_time
            patch_config_notification("end_time", str(end_time))
        if token is not None and token != "":
            ec.token = token
            patch_config_notification("token", str(token), hide=True)
        if url is not None:
            ec.url = url
            patch_config_notification("url", str(url))
        if user is not None:
            ec.user = user
            patch_config_notification("user", str(user))

        # this flag is always set, and cannot be stored in the config
        ec.log_curl_statements = log_curl_statements


        check_completeness_of_config(ec)
        if ec.get_crawling_format() == Format.INVALID:
            raise RuntimeError(
                f"The chosen crawling_format \"{ec.crawling_format}\" is invalid. You must specify \"json\" or \"csv\".")
        if ec.get_output_format() == Format.INVALID:
            raise RuntimeError(f"The chosen output_format \"{ec.output_format}\" is invalid. You must specify \"json\" or \"csv\".")

        if patched_config:
            copy_with_masked_token = copy.deepcopy(ec)
            copy_with_masked_token.token = '*'*len(copy_with_masked_token.token) + "(sensitive content is not displayed)"
            pretty_printed_config = json.dumps(json.loads(copy_with_masked_token.to_json()), indent=4)
            if yes:
                print(f"Configuration to be executed is: \n\n{pretty_printed_config}\n\n")
            elif not click.confirm(
                    f"Configuration is now: \n\n{pretty_printed_config}\n\nShall the above configuration be executed?"):
                raise RuntimeError("User aborted execution")
        process_export_config(output_dir, ec, yes)
        sys.exit(0)
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)

@cli.command()
def full_example_dataset_config():
    """
    Outputs a dataset JSON example config with all fields set.
    """
    ec = ExportConfig(
        datasource=DataSourceDataset(
            dataset=12345678,
            opal_query="col_pick col1, col2, col3\\nfilter col1=100",
            initial_interval_to_query_seconds=300,
            check_completeness_of_data=True,
            comment="Any comment you put here will not be processed."
        ),
        filename_prefix="dataset_filename",
        output_format="json",
        crawling_format="json",
        string_columns=["col1"],
        sort_keys=["col2"],
        columns_to_keep=["col1", "col2", "col3"],
        start_time=datetime.now() - timedelta(hours=2),
        end_time=datetime.now() - timedelta(hours=1),
        url="xzy.observeinc.com",
        user="xzy",
        token="access token for user xzy on xzy.observeinc.com",
    )
    print(json.dumps(json.loads(ec.to_json()), indent=4))

@cli.command()
def full_example_worksheet_config():
    """
    Outputs a worksheet JSON example config with all fields set.
    """
    ec = ExportConfig(
        datasource=DataSourceWorksheet(
            worksheet=12345678,
            stage="stage-abcdefg",
            initial_interval_to_query_seconds=300,
            comment="Any comment you put here will not be processed."
        ),
        filename_prefix="worksheet_filename",
        output_format="json",
        crawling_format="json",
        string_columns=["col1"],
        sort_keys=["col2"],
        columns_to_keep=["col1", "col2", "col3"],
        start_time=datetime.now() - timedelta(hours=2),
        end_time=datetime.now() - timedelta(hours=1),
        url="xzy.observeinc.com",
        user="xzy",
        token="access token for user xzy on xzy.observeinc.com",
    )
    print(json.dumps(json.loads(ec.to_json()), indent=4))


@cli.command()
def minimal_example_dataset_config():
    """
    Outputs a minimal dataset JSON example config with optional fields nulled.
    """
    ec = ExportConfig(
        datasource=DataSourceDataset(
            dataset=12345678,
        ),
        filename_prefix="dataset_filename",
        output_format="json",
        crawling_format="json",
        start_time=datetime.now() - timedelta(hours=2),
        end_time=datetime.now() - timedelta(hours=1),
        url="xzy.observeinc.com",
        user="xzy",
        token="access token for user xzy on xzy.observeinc.com",
    )
    print(json.dumps(json.loads(ec.to_json()), indent=4))

@cli.command()
def minimal_example_worksheet_config():
    """
    Outputs a minimal worksheet JSON example config with optional fields nulled.
    """
    ec = ExportConfig(
        datasource=DataSourceWorksheet(
            worksheet=12345678,
            stage="stage-abcdefg",
        ),
        filename_prefix="worksheet_filename",
        output_format="json",
        crawling_format="json",
        start_time=datetime.now() - timedelta(hours=2),
        end_time=datetime.now() - timedelta(hours=1),
        url="xzy.observeinc.com",
        user="xzy",
        token="access token for user xzy on xzy.observeinc.com",
    )
    print(json.dumps(json.loads(ec.to_json()), indent=4))

