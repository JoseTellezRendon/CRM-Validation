from __future__ import annotations
import numpy as np
import pandas as pd
import pathlib
import warnings
import math
import re


warning_message_absolute_path = """Warning:
                        The filepath provided for the {} file is absolute.
                        Check that the file is located in the exact path.
                        Common errors can arise from passing absolute paths that reference an incorrect user folder.
                        """
warning_msg_no_extension = """Warning:
                    The filepath provided for the {} file has no extension.
                    Reading the file as a Comma-Separated Values (csv) file.
                    """


def find_key(dict_str, key_val):
    try:
        dict_var = eval(dict_str)
    except Exception:
        return False
    if isinstance(dict_var, dict):
        return key_val in dict_var
    else:
        return False


def get_key_value(dict_str, key_val):
    if find_key(dict_str, key_val):
        return eval(dict_str)[key_val]
    else:
        return None


def __create_col_map(map_buffer, search_key, map_col_name, map_col_type, map_col_xcheck):
    """Specific function to generate column mapping for Numeric columns that will be rounded to a
    number of decimal places, and Date columns"""
    return {col_name: get_key_value(col_type, search_key) for col_name, col_type, col_xcheck in
            zip(map_buffer[map_col_name], map_buffer[map_col_type], map_buffer[map_col_xcheck])
            if find_key(col_type, search_key) and all([col_name is not np.nan, col_xcheck is not np.nan])}


def data_validation_orchestrator(filepath: pathlib.Path | str = None,
                                 data_base_folder: pathlib.Path | str = None,
                                 encoding: str = None,
                                 output_folder: pathlib.Path | str = '',
                                 debug: bool = False):
    if filepath is None:
        return None
    else:
        if isinstance(filepath, str):
            filepath = pathlib.Path(filepath)
        if isinstance(data_base_folder, str):
            data_base_folder = pathlib.Path(data_base_folder)
        if isinstance(output_folder, str):
            output_folder = pathlib.Path(output_folder)
            if output_folder.exists() and not output_folder.is_dir():
                raise Exception("Output folder specified already exists and is not a folder.")
            else:
                output_folder.mkdir(parents=True, exist_ok=True)

        if filepath.is_absolute():
            warnings.warn(
                warning_message_absolute_path.format("orchestrator")
            )

        if filepath.suffix == ".xlsx":
            table_mapping = pd.read_excel(filepath)
        elif filepath.suffix == ".csv":
            table_mapping = pd.read_csv(filepath, encoding=encoding)
        elif filepath.suffix is None:
            warnings.warn(
                warning_msg_no_extension.format("orchestrator")
            )
            table_mapping = pd.read_csv(filepath, encoding=encoding)
        else:
            raise Exception("""Unrecognized file format.
            Supported file extensions: .csv .xlsx""")

        if not data_base_folder.is_dir():
            raise Exception("""data_base_folder has to be a folder.""")

    origin_filepath_col = 'origin_filename'
    target_filepath_col = 'target_filename'
    table_name_col = 'table_name'
    origin_file_encoding = 'origin_encoding'
    target_file_encoding = 'target_encoding'
    is_join_field_col = 'join_field'  # To Be Used
    is_id_field_col = 'id_field'
    origin_field_col = 'origin_field'
    origin_field_type_col = 'origin_type'
    origin_ffill_col = 'origin_ffill'
    target_field_col = 'target_field'
    target_field_type_col = 'target_type'
    target_ffill_col = 'target_ffill'
    origin_prep_text = 'preprocessing'
    origin_single_col_prep_code = 'preproc_pycode'
    xwalk_filepath_col = 'crosswalk_file'
    xwalk_sheetname_col = 'sheet_name'
    xwalk_origin_val_col = 'origin_val_col'
    xwalk_target_val_col = 'target_val_col'

    table_groups = table_mapping.groupby([origin_filepath_col, target_filepath_col, table_name_col])

    map_cols = [origin_field_col, origin_field_type_col,
                target_field_col, target_field_type_col,
                origin_single_col_prep_code,
                origin_ffill_col, target_ffill_col,
                xwalk_filepath_col, xwalk_sheetname_col, xwalk_origin_val_col, xwalk_target_val_col]

    for (origin_file, target_file, table_name), mapping_df in table_groups:
        print(f"****************************************************************************************")
        print(f"{table_name}")
        print(f"****************************************************************************************")
        origin_filepath = data_base_folder / origin_file
        target_filepath = data_base_folder / target_file
        mapping_buffer = mapping_df[map_cols].to_dict('list')

        if debug:
            print("Mapping Buffer")
            print(mapping_buffer)

        mapping_dict = [{"baseline_col_name": origin_col,
                         "validate_col_name": target_col,
                         "common_col_type": origin_type}
                        for origin_col, target_col, origin_type in
                        zip(mapping_buffer[origin_field_col],
                            mapping_buffer[target_field_col],
                            mapping_buffer[origin_field_type_col])
                        if all([origin_col is not np.nan, target_col is not np.nan])]
        # col_id = mapping_df[mapping_df[is_id_field_col] == True][origin_field_col].to_list()
        col_id = mapping_df[mapping_df[is_id_field_col]][origin_field_col].to_list()

        column_preprocessing = {col_name: col_prep for col_name, col_prep in
                                zip(mapping_buffer[origin_field_col], mapping_buffer[origin_single_col_prep_code])
                                if all([not pd.isna(col_name), not pd.isna(col_prep)])
                                }

        column_crosswalk = {}
        # Get Excel spreadsheet filepath and sheet name per field
        crosswalk_buffer = {col_name: {'file': xwalk_file,
                                       'sheet': sheet_name,
                                       'origin_col': origin_col,
                                       'target_col': target_col}
                            for col_name, xwalk_file, sheet_name, origin_col, target_col in
                            zip(mapping_buffer[origin_field_col],
                                mapping_buffer[xwalk_filepath_col],
                                mapping_buffer[xwalk_sheetname_col],
                                mapping_buffer[xwalk_origin_val_col],
                                mapping_buffer[xwalk_target_val_col])
                            if all([xwalk_file is not np.nan, sheet_name is not np.nan,
                                    origin_col is not np.nan, target_col is not np.nan])
                            }

        for col_name, xwalk_info in crosswalk_buffer.items():
            # Read Excel sheet
            xwalk_df = pd.read_excel(xwalk_info['file'], sheet_name=xwalk_info['sheet'])
            # Generate dictionary in format {col_name: {search_val: replace_val}}
            val_buffer = xwalk_df[[xwalk_info['origin_col'], xwalk_info['target_col']]].to_dict('list')
            column_crosswalk[col_name] = {search_val: replace_val for search_val, replace_val in
                                          zip(val_buffer[xwalk_info['origin_col']],
                                              val_buffer[xwalk_info['target_col']])
                                          }
        # Remove empty crosswalks (for efficiency)
        column_crosswalk = {k: v for k, v in column_crosswalk.items() if v}

        origin_date_cols = __create_col_map(mapping_buffer, 'date', origin_field_col, origin_field_type_col,
                                            target_field_col)
        target_date_cols = __create_col_map(mapping_buffer, 'date', target_field_col, target_field_type_col,
                                            origin_field_col)

        # change type of date columns in mapping dict
        temp_map_dict = []
        for cur_col_map in mapping_dict:
            temp_col_dict = {"baseline_col_name": cur_col_map["baseline_col_name"],
                             "validate_col_name": cur_col_map["validate_col_name"]}
            if 'date' not in cur_col_map['common_col_type']:
                temp_col_dict["common_col_type"] = cur_col_map["common_col_type"]
            else:
                temp_col_dict["common_col_type"] = "date"
            temp_map_dict.append(temp_col_dict)

        mapping_dict = temp_map_dict

        origin_round_cols = __create_col_map(mapping_buffer, 'round', origin_field_col, origin_field_type_col,
                                             target_field_col)
        target_round_cols = __create_col_map(mapping_buffer, 'round', target_field_col, target_field_type_col,
                                             origin_field_col)

        origin_encoding = mapping_df[origin_file_encoding].value_counts()
        if len(origin_encoding) > 1:
            print("Multiple encodings found for the same origin file. Using the most frequent")
        target_encoding = mapping_df[target_file_encoding].value_counts()
        if len(target_encoding) > 1:
            print("Multiple encodings found for the same target file. Using the most frequent")
        origin_encoding = origin_encoding.index[0]
        target_encoding = target_encoding.index[0]

        unify_text = False
        text_cols = None

        origin_cols_ffill = [col_name for col_name, col_ffill in
                             zip(mapping_buffer[origin_field_col], mapping_buffer[origin_ffill_col])
                             if (col_ffill and (col_ffill is not np.nan))]
        target_cols_ffill = [col_name for col_name, col_ffill in
                             zip(mapping_buffer[target_field_col], mapping_buffer[target_ffill_col])
                             if (col_ffill and (col_ffill is not np.nan))]

        if debug:
            print(f"Origin filepath: {origin_filepath}")
            print(f"Target filepath: {target_filepath}")
            print(f"Mapping Dict: {mapping_dict}")
            print(f"Column ID: {col_id}")
            print(f"Column Preprocessing: {column_preprocessing}")
            print(f"Column Crosswalk: {column_crosswalk}")
            print(f"Origin Date Cols: {origin_date_cols}")
            print(f"Target Date Cols: {target_date_cols}")
            print(f"Origin Round Cols: {origin_round_cols}")
            print(f"Target Round Cols: {target_round_cols}")
            print(f"Origin encoding: {origin_encoding}")
            print(f"Target encoding: {target_encoding}")
            print(f"Unify Text: {unify_text}")
            print(f"Text Columns: {text_cols}")
            print(f"Origin ffill: {origin_cols_ffill}")
            print(f"Target ffill: {target_cols_ffill}")
            print()
            print(table_mapping)

        cur_validator = Validator(baseline_filepath=origin_filepath,
                                  validate_filepath=target_filepath,
                                  column_mapping=mapping_dict,
                                  col_id=col_id,
                                  column_preprocessing=column_preprocessing,
                                  column_crosswalk=column_crosswalk,
                                  baseline_date_cols=origin_date_cols,
                                  validate_date_cols=target_date_cols,
                                  baseline_round_cols=origin_round_cols,
                                  validate_round_cols=target_round_cols,
                                  baseline_encoding=origin_encoding,
                                  validate_encoding=target_encoding,
                                  unify_text=unify_text,
                                  text_cols=text_cols,
                                  origin_cols_ffill=origin_cols_ffill,
                                  target_cols_ffill=target_cols_ffill,
                                  )

        cur_validator.check_existence()
        print("Existence errors per Column")
        print(cur_validator.existence_result.sum())
        # Registers with existence flags
        print()
        print("Registers with existence flags")
        print((cur_validator.existence_result.sum(axis=1) > 0).sum())
        # Total existence flags
        print()
        print("Total existence flags")
        print(cur_validator.existence_result.sum().sum())
        print()
        print("Row Difference")
        print(cur_validator.row_difference)
        print()
        print("Col Difference")
        print(cur_validator.col_difference)
        print()
        print("Dupplicated Rows")
        print(cur_validator.duplicated_rows)
        cur_validator.check_equality()

        cur_validator.existence_result.to_excel(output_folder / f'{table_name}_Existence_Result.xlsx', index=False)
        cur_validator.equality_result.rename(columns={'origin': 'Source', 'target': 'Workday'}).to_excel(
            output_folder / f'{table_name}_Equality_Result.xlsx')
        print(f"****************************************************************************************")
        print(f"DONE")
        print(f"****************************************************************************************")
        print()


def read_dataset(filepath: str | pathlib.Path = None,
                 dataset_name: str = "baseline dataset",
                 encoding: str = 'latin',
                 date_cols: dict = None,
                 round_cols: dict = None,
                 ) -> pd.DataFrame | None:

    if filepath is None:
        return None
    else:
        if isinstance(filepath, str):
            filepath = pathlib.Path(filepath)

        if filepath.is_absolute():
            warnings.warn(
                warning_message_absolute_path.format(dataset_name)
            )

        if filepath.suffix == ".xlsx":
            temp_df = pd.read_excel(filepath)
        elif filepath.suffix == ".csv":
            temp_df = pd.read_csv(filepath, encoding=encoding)
        elif filepath.suffix is None:
            warnings.warn(
                warning_msg_no_extension.format(dataset_name)
            )
            temp_df = pd.read_csv(filepath, encoding=encoding)
        else:
            raise Exception("""Unrecognized file format.
            Supported file extensions: .csv .xlsx""")

        if date_cols:
            for col_name, date_format in date_cols.items():
                temp_df[col_name] = pd.to_datetime(temp_df[col_name], format=date_format, errors='coerce')
                if col_name == "HOLD_TO_DATE":
                    temp_df[col_name] = temp_df[col_name].dt.date

        if round_cols:
            for col_name, n_decimals in round_cols.items():
                temp_df[col_name] = temp_df[col_name].round(n_decimals)

        return temp_df


def generate_warn_message(cols_list: list, warn_type: str = 'type', dataset_name: str = 'origin') -> str:
    message = f"""Warning:
        Dataset {dataset_name.title()}
        """
    if warn_type == 'type':
        submessage_template = """
                Column: {}
                    Real Type: {}
                    Expected Type: {}"""
        for col_dict in cols_list:
            message += submessage_template.format(
                col_dict['col_name'],
                col_dict['real_type'],
                col_dict['expected_type']
            )
    elif warn_type == 'missing':
        submessage_template = """
                Missing Columns: {}"""
        message += submessage_template.format(cols_list)
    else:
        raise Exception(f"Incorrect value passed. Expected 'type' or 'missing', received '{warn_type}'")

    return message


class Validator:

    @property
    def origin_data(self):
        return self.__origin_data

    @origin_data.setter
    def origin_data(self, new_path: pathlib.Path | str):
        self.__origin_data = read_dataset(new_path, "baseline")
        self.__mapping_checked = False
        self.__existence_checked = False
        self.__equality_checked = False

    @property
    def target_data(self):
        return self.__target_data

    @target_data.setter
    def target_data(self, new_path: pathlib.Path | str):
        self.__target_data = read_dataset(new_path, "validate")
        self.__mapping_checked = False
        self.__existence_checked = False
        self.__equality_checked = False

    @property
    def col_map(self):
        return self.__col_map

    @col_map.setter
    def col_map(self, new_map: list):
        self.__col_map = new_map
        self.__name_mapping = None
        self.__type_mapping = None
        self.__mapping_checked = False
        self.__existence_checked = False
        self.__equality_checked = False

    @property
    def name_mapping(self):
        return self.__name_mapping

    @property
    def type_mapping(self):
        return self.__type_mapping

    @property
    def col_id(self):
        return self.__col_id

    @col_id.setter
    def col_id(self, new_col: str):
        self.__col_id = new_col
        self.__existence_checked = False
        self.__equality_checked = False

    @property
    def existence_result(self):
        return self.__existence_result

    @property
    def equality_result(self):
        return self.__equality_result

    @property
    def origin_data_for_validation(self):
        return self.__origin_data_for_validation

    @property
    def target_data_for_validation(self):
        return self.__target_data_for_validation

    @property
    def origin_rows_not_in_target(self):
        return self.__origin_rows_not_in_target

    @property
    def target_rows_not_in_origin(self):
        return self.__target_rows_not_in_origin

    @property
    def duplicated_rows(self):
        return self.__duplicated_rows

    @property
    def row_difference(self):
        return self.__row_difference

    @property
    def col_difference(self):
        return self.__col_difference

    def __init__(self,
                 baseline_filepath: pathlib.Path | str = None,
                 validate_filepath: pathlib.Path | str = None,
                 column_mapping: list = None,
                 col_id: str = None,
                 baseline_date_cols: dict = None,
                 validate_date_cols: dict = None,
                 baseline_round_cols: dict = None,
                 validate_round_cols: dict = None,
                 baseline_encoding: str = 'latin',
                 validate_encoding: str = 'utf-8',
                 unify_text: bool = False,
                 text_cols: list = None,
                 ):

        # Inner variables for processing
        self.__name_mapping = None
        self.__type_mapping = None
        self.__origin_data_for_validation = None
        self.__target_data_for_validation = None

        # Inner variables for storing validation results
        self.__existence_result = None
        self.__equality_result = None
        self.__row_difference = None
        self.__col_difference = None
        self.__duplicated_rows = None
        self.__origin_rows_not_in_target = None
        self.__target_rows_not_in_origin = None

        # Flags for different validations
        self.__mapping_checked = False
        self.__existence_checked = False
        self.__equality_checked = False

        # Variables for pre-processing
        self.__unify_text = unify_text
        self.__text_cols = text_cols

        # BASELINE & VALIDATE DATASETS
        self.__origin_data = read_dataset(baseline_filepath, "baseline",
                                          encoding=baseline_encoding,
                                          date_cols=baseline_date_cols,
                                          round_cols=baseline_round_cols,
                                          )
        self.__target_data = read_dataset(validate_filepath, "validate",
                                          encoding=validate_encoding,
                                          date_cols=validate_date_cols,
                                          round_cols=validate_round_cols,
                                          )

        # IDENTIFIER COLUMN
        self.__col_id = col_id if col_id else self.__origin_data.columns.tolist()

        # COLUMN MAPPING
        self.__col_map = column_mapping
        if column_mapping:
            self.validate_col_map()

    def validate_col_map(self):
        """Checks if the mapping specified is valid given the datasets configured.
         The mapping must follow the next format:
         - A list of dictionaries, with the following fields:
             - baseline_col_name: str -> Column Name (str)
             - baseline_col_type: str -> Origin Type (str) [Optional]
             - validate_col_name: str -> Column Name (str)
             - validate_col_type: str -> Target Type (str) [Optional]
             - common_col_type: str -> Common type (str) [Optional]
        The passed columns must exist in the respective dataset. The column types are optional.
        common_col_type is used to define a common data type for the column in both datasets when doing the
        validations. If not passed, the origin data type is used.
        After a successful complete check (both datasets), the actual column mapping will be generated.
        """

        if not self.__col_map:
            raise Exception("No mapping configured to validate!")

        if self.__mapping_checked:
            warnings.warn("""Mapping has already been checked for the current configuration.
            Modify the datasets or column mapping and try again.""")
            return

        check_origin = True
        check_target = True

        warn_message = """Warning:
            No {} dataset has been configured. Cannot validate mapping for this dataset.
            """

        if self.__origin_data is None:
            warnings.warn(warn_message.format("baseline"))
            check_origin = False

        if self.__target_data is None:
            warnings.warn(warn_message.format("validate"))
            check_target = False

        if check_origin or check_target:

            origin_cols_missing = []
            origin_cols_mistype = []
            target_cols_missing = []
            target_cols_mistype = []
            name_map_temp = {}
            type_map_temp = {}

            for k_dic in self.__col_map:
                # Check baseline column exists
                origin_cur_col_result = {}
                target_cur_col_result = {}

                cur_origin_col_name = k_dic.get("baseline_col_name")
                cur_origin_col_type = k_dic.get("baseline_col_type")
                cur_target_col_name = k_dic.get("validate_col_name")
                cur_target_col_type = k_dic.get("validate_col_type")
                cur_common_col_type = k_dic.get("common_col_type")

                if check_origin:
                    origin_cur_col_result = self.__validate_column(cur_origin_col_name, cur_origin_col_type, 'origin')
                    if not origin_cur_col_result['exists']:
                        origin_cols_missing.append(cur_origin_col_name)
                    elif origin_cur_col_result['type_error']:
                        origin_cur_col_type_error = {
                            'col_name': cur_origin_col_name,
                            **origin_cur_col_result['type']
                        }
                        origin_cols_mistype.append(origin_cur_col_type_error)

                # Check validate column exists
                if check_target:
                    target_cur_col_result = self.__validate_column(cur_target_col_name, cur_target_col_type, 'target')
                    if not target_cur_col_result['exists']:
                        target_cols_missing.append(cur_target_col_name)
                    elif target_cur_col_result['type_error']:
                        target_cur_col_type_error = {
                            'col_name': cur_target_col_name,
                            **target_cur_col_result['type']
                        }
                        target_cols_mistype.append(target_cur_col_type_error)

                # Add name and type mapping to temp variables if respective columns exist on both sides
                if origin_cur_col_result['exists'] and target_cur_col_result['exists']:
                    name_map_temp[cur_target_col_name] = cur_origin_col_name
                    # If common type has been specified, use it (forces conversion during checks)
                    if cur_common_col_type:
                        if cur_origin_col_type != cur_common_col_type:
                            warnings.warn("    Common data type does not match origin data type. Validation"
                                          "    will force common data type.")
                        type_map_temp[cur_origin_col_name] = cur_common_col_type
                    # if not, use the origin type
                    else:
                        # Issue warning if ACTUAL type of the column is not equal to expected type
                        # --- An idea might be, when no matching types, force string for comparison (not implemented)
                        if target_cur_col_result['type_error']:
                            warnings.warn("    Expected data type does not match origin data type. Validation"
                                          "    will force expected type if present, otherwise, real type.")
                        type_map_temp[cur_origin_col_name] = cur_origin_col_type if cur_origin_col_type else \
                            target_cur_col_result['type']['real_type']

            # If any type is incorrect, throw a warning.
            if origin_cols_mistype:
                warnings.warn(generate_warn_message(origin_cols_mistype, warn_type='type',
                                                    dataset_name='baseline dataset'))
            if target_cols_mistype:
                warnings.warn(generate_warn_message(target_cols_mistype, warn_type='type',
                                                    dataset_name='validate dataset'))

            # If any column is missing on either dataset, throw warning and end
            if origin_cols_missing or target_cols_missing:
                warnings.warn(generate_warn_message(origin_cols_missing, warn_type='missing',
                                                    dataset_name='baseline dataset'))
                warnings.warn(generate_warn_message(target_cols_missing, warn_type='missing',
                                                    dataset_name='validate dataset'))
            elif name_map_temp:
                # Save mapping. Check if there is something saved in temp variable, as it checks for
                # existence of respective columns on both datasets
                self.__name_mapping = name_map_temp
                self.__type_mapping = type_map_temp

            self.__mapping_checked = True
        else:
            print("No datasets configured to validate mapping.")

    def check_existence(self):
        if self.__existence_checked:
            warnings.warn("""Existence has already been checked for the current configuration.
            Modify the datasets or column mapping and try again.""")
            return self.__existence_result

        # Only do something if both datasets are configured.
        if self.__target_data is not None and self.__origin_data is not None:
            # If mapping configured, apply it
            if self.__name_mapping:
                self.__target_data_for_validation = self.__target_data.rename(columns=self.__name_mapping)
            else:
                warnings.warn("""Warning:
                No mapping configured. Assuming both datasets have the same column names and types.""")
                self.__target_data_for_validation = self.__target_data
                self.__name_mapping = {col_name: col_name for col_name in self.__origin_data.columns}
                self.__type_mapping = {col_name: col_type for col_name, col_type in self.__origin_data.dtypes.items()}
            self.__origin_data_for_validation = self.__origin_data

            # How to verify existence:
            # 0. Verify data shape
            # 0.1 Count duplicates
            self.__duplicated_rows = {'origin': self.__origin_data_for_validation.duplicated(subset=self.__col_id).sum(),
                                      'target': self.__target_data_for_validation.duplicated(subset=self.__col_id).sum()}

            # 0.2 Remove duplicates, otherwise the Magic (step 4) doesn't work
            self.__origin_data_for_validation = self.__origin_data_for_validation.drop_duplicates(subset=self.__col_id)
            self.__target_data_for_validation = self.__target_data_for_validation.drop_duplicates(subset=self.__col_id)
            # 0.3 Number of rows
            self.__row_difference = self.__origin_data_for_validation.shape[0] - self.__target_data_for_validation.shape[0]
            # 0.4 Columns
            origin_cols_not_in_target = set(self.__origin_data_for_validation.columns) - set(self.__target_data_for_validation.columns)
            target_cols_not_in_origin = set(self.__target_data_for_validation.columns) - set(self.__origin_data_for_validation.columns)
            self.__col_difference = {'origin_cols_not_in_target': origin_cols_not_in_target,
                                     'target_cols_not_in_origin': target_cols_not_in_origin}
            # 0.5 Text cleaning, if specified
            #     Text cleaning is done at this point, so that previous validations that are internal for each dataset
            #     are not impacted by the text cleaning
            if self.__unify_text:
                self.__clean_text_columns()

            # 1. Align columns
            # 1.1 Select columns from origin and target that are in the column mapping
            self.__origin_data_for_validation = self.__origin_data_for_validation[self.__name_mapping.values()]
            # 1.2 Target columns selected based on origin columns to guarantee same column order
            self.__target_data_for_validation = self.__target_data_for_validation[self.__origin_data_for_validation.columns]

            # 2. Match data types for the columns
            for col_name, col_type in self.__type_mapping.items():
                if col_name in ["ID", "PIDM_KEY", "VETC_NUMBER"]:
                    warnings.warn(f"Column {col_name} is going to be casted as a string!")
                    self.__origin_data_for_validation[col_name] = self.__origin_data_for_validation[
                        col_name].astype('string').str.replace(".0", "")
                    self.__target_data_for_validation[col_name] = self.__target_data_for_validation[
                        col_name].astype('string').str.replace(".0", "")
                else:
                    if self.__origin_data_for_validation[col_name].dtype != col_type:
                        self.__origin_data_for_validation[col_name] = self.__origin_data_for_validation[col_name].astype(
                            col_type)
                    if self.__target_data_for_validation[col_name].dtype != col_type:
                        self.__target_data_for_validation[col_name] = self.__target_data_for_validation[col_name].astype(
                            col_type)

            # 3. Align rows
            # 3.0 Generate a column to mark rows that are different in both datasets
            self.__origin_data_for_validation['check'] = 0
            self.__target_data_for_validation['check'] = 0
            # 3.1 Use the id columns as index
            self.__origin_data_for_validation.set_index(self.__col_id, inplace=True)
            self.__target_data_for_validation.set_index(self.__col_id, inplace=True)
            # 3.2 Align based on index (which are the indexes now)
            origin_temp, target_temp = self.__origin_data_for_validation.align(self.__target_data_for_validation, axis=0)
            # 3.3 Rows in one dataset that are not in the other are marked as NAs in 'check' column
            #     Extract them for possible analysis later on
            self.__target_rows_not_in_origin = target_temp[origin_temp.check.isna()].reset_index().drop(columns='check')
            self.__origin_rows_not_in_target = origin_temp[target_temp.check.isna()].reset_index().drop(columns='check')
            # 3.4 Filter out records from target not in origin
            self.__origin_data_for_validation = origin_temp[~origin_temp.check.isna()].reset_index().drop(columns='check')
            self.__target_data_for_validation = target_temp[~origin_temp.check.isna()].reset_index().drop(columns='check')

            # 4. Magic
            # Using NA values, if there are NAs in the target (isNA == True) that are not in origin (isNA == False),
            # flag them.
            # Boolean operation: not(Origin_isNA) & Target_isNA
            self.__existence_result = ~self.__origin_data_for_validation.isna() & self.__target_data_for_validation.isna()

            self.__existence_checked = True

            return self.__existence_result

        else:
            raise Exception("Both datasets (origin & target) must be configured to check existence.")

    def check_equality(self):
        if self.__equality_checked:
            warnings.warn("""Equality has already been checked for the current configuration.
            Modify the datasets or column mapping and try again.""")
            return self.__equality_result

        if not self.__existence_checked:
            self.check_existence()

        # Only do something if both datasets are configured.
        if self.__target_data is not None and self.__origin_data is not None:
            # Mapping already applied during existence check

            # How to verify equality:
            # 0. Verify data shape (done in existence check)
            # 1. Align columns (done in existence check)
            # 2. Match data types for the columns (done in existence check)
            # 3. Align rows (done in existence check)
            # 4. Magic
            # Because the columns and rows are already aligned, a direct comparison does the work
            # origin.compare(target)
            self.__equality_result = self.__origin_data_for_validation.compare(
                self.__target_data_for_validation,
                result_names=('origin', 'target'))

            # Add column ID(s) at the start of the dataframe
            for idx, col_name in enumerate(self.__col_id):
                self.__equality_result.insert(idx, col_name, self.__origin_data_for_validation[col_name])

            return self.__equality_result
        else:
            raise Exception("Both datasets (origin & target) must be configured to check equality.")

    def __clean_text_columns(self) -> None:
        if self.__text_cols is None:
            warnings.warn("No columns specified as text. Text columns are not modified.")
        else:
            for text_col in self.__text_cols:
                # Unify apostrophe
                self.__origin_data_for_validation.loc[:, text_col] = \
                        self.__origin_data_for_validation[text_col].str.replace(u'\u0092', u"\u0027")
                self.__origin_data_for_validation.loc[:, text_col] = \
                    self.__origin_data_for_validation[text_col].str.replace(u'’', u"\u0027")

                self.__target_data_for_validation.loc[:, text_col] = \
                    self.__target_data_for_validation[text_col].str.replace(u'\u0092', u"\u0027")
                self.__target_data_for_validation.loc[:, text_col] = \
                    self.__target_data_for_validation[text_col].str.replace(u'’', u"\u0027")

                # # Make all lowercase
                # self.__origin_data_for_validation[text_col] = \
                #                         self.__origin_data_for_validation[text_col].str.lower()
                # self.__target_data_for_validation[text_col] = \
                #                         self.__target_data_for_validation[text_col].str.lower()

    def __validate_column(self, col_name: str, col_type: str, dataset: str = 'origin') -> dict:
        if dataset == 'origin':
            temp_dataset = self.__origin_data
        elif dataset == 'target':
            temp_dataset = self.__target_data
        else:
            raise Exception(f"Incorrect value passed. Expected 'origin' or 'target', received '{dataset}'")

        # Check column exists and type[optional]
        return_dic = {}
        if col_name in temp_dataset.columns:
            return_dic['exists'] = True
            if col_type:
                if temp_dataset[col_name].dtype == col_type:
                    return_dic['type_error'] = False
                else:
                    return_dic['type_error'] = True
                    return_dic['type'] = {
                        'real_type': temp_dataset[col_name].dtype,
                        'expected_type': col_type
                    }
            else:
                return_dic['type_error'] = True
                return_dic['type'] = {
                    'real_type': temp_dataset[col_name].dtype,
                    'expected_type': "NA"
                }
        else:
            return_dic['exists'] = False

        return return_dic
