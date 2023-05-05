import pandas as pd
import pathlib
import warnings


def read_dataset(filepath: pathlib.Path, dataset_name="baseline dataset") -> pd.DataFrame:
    warning_message_absolute_path = """Warning:
                        The filepath provided for the {} is absolute.
                        Check that the file is located in the exact path.
                        Common errors can arise from passing absolute paths that reference an incorrect user folder.
                        """
    warning_msg_no_extension = """Warning:
                        The filepath provided for the {} has no extension.
                        Reading the file as a Comma-Separated Values (csv) file.
                        """

    if filepath is None:
        return pd.DataFrame()
    else:
        if isinstance(filepath, str):
            filepath = pathlib.Path(filepath)

        if filepath.is_absolute():
            warnings.warn(
                warning_message_absolute_path.format(dataset_name)
            )
        if filepath.suffix == ".xlsx":
            return pd.read_excel(filepath)
        elif filepath.suffix == ".csv":
            return pd.read_csv(filepath)
        elif filepath.suffix is None:
            warnings.warn(
                warning_msg_no_extension.format(dataset_name)
            )
            return pd.read_csv(filepath)


class Validator:

    def __init__(self,
                 baseline_filepath: pathlib.Path | str = None,
                 validate_filepath: pathlib.Path | str = None,
                 column_mapping: dict = None,
                 ):

        # BASELINE DATASET
        self.baseline_data = read_dataset(baseline_filepath, "baseline dataset")

        # VALIDATE DATASET
        self.validate_data = read_dataset(validate_filepath, "validate dataset")

        # COLUMN MAPPING
        self.col_map = column_mapping

        # Add column mapping printing
        #   - useful for debugging before actual validation
        # Add column mapping validation!!!
        #   - Specified column exist on the respective dataset
        #   - Specified column is the correct type on both datasets

    def validate_col_map(self):
        """Checks if the mapping specified follows the next format:
         - Banner Column Name (str): dic
             - target_col_name: str -> Column Name (str)
             - origin_col_type: str -> Origin Type (str)
             - target_col_type: str -> Target Type (str) [Optional]"""

        check_baseline = True
        check_validate = True

        warn_message = """Warning:
            No {} dataset has been configured. Cannot validate mapping for this dataset.
            """

        if not self.baseline_data:
            warnings.warn(warn_message.format("baseline"))
            check_baseline = False

        if not self.validate_data:
            warnings.warn(warn_message.format("validate"))
            check_validate = False

        if check_baseline or check_validate:
            for origin_col_type, k_dic in self.col_map.items():
                pass
        else:
            print("No datasets configured to validate mapping.")
