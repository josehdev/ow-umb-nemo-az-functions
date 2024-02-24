import pandas as pd
import traceback
import logging

# The column order matters
BICAN_COLUMNS = [
    "program",
    "sgl_name",
    "library_lab_pool_name",
    "library_aliquot_name",
    "filename",
    "checksum",
    "sequence_center",
    "data_type",
    "file_format",
    "instrument",
    "flow_cell_type",
    "flow_cell_version",
    "flow_cell_name",
    "demultiplex_stats_filename",
    "run_parameters_filename",
    "top_unknown_barcodes_filename",
    "demux_recipe",
    "alternate_contact"
]

OPTIONAL_COLUMNS = [
    "alternate_contact"
]

RUN_METRICS_COLUMNS = [
    "demultiplex_stats_filename",
    "top_unknown_barcodes_filename",
    "run_parameters_filename"
]

# The following columns are empty when 'sequence_center' column is empty.
# This allows non-seqCore submitters to use the BICAN manifest.
COLUMNS_EMPTY_FOR_LAB_DIRECT_SUBMIT = [
    "sequence_center",
    "demultiplex_stats_filename",
    "top_unknown_barcodes_filename",
    "run_parameters_filename",
    "flow_cell_version"
]

COLUMNS_EMPTY_FOR_RUN_METRICS = [
    "library_lab_pool_name",
    "library_aliquot_name",
    "demultiplex_stats_filename",
    "top_unknown_barcodes_filename",
    "run_parameters_filename"
]

MANIFEST_ROW_LIMIT = 50000
PROGRAM_NAME = "bican"

def check_for_enum_errors(manifest_dataframe, column_name, cv_terms):
    """
    Returns a list of errors where values are not listed a controlled vocabulary.
    """
    error_message = "contains invalid value"

    has_invalid_val = ~manifest_dataframe[column_name].isin(cv_terms)
    rows_with_invalids = manifest_dataframe[has_invalid_val].index.tolist()

    errors = format_errors(rows_with_invalids, column_name, error_message)

    return errors


def check_for_length_range_errors(manifest_dataframe, column_name, lower_bound, upper_bound):
    """
    Returns a list of errors where value lengths are outside the specified range.
    """
    error_message = "does not match expected character length"

    # Returns values that:
    # 1) are not empty (NaN), and
    # 2) match values that are less than or greater than the provided length range
    has_wrong_length = (manifest_dataframe[column_name].notna()) & \
                           (
                                (manifest_dataframe[column_name].str.len() < int(lower_bound)) | \
                                (manifest_dataframe[column_name].str.len() > int(upper_bound))
                           )
    rows_with_len_error = manifest_dataframe[has_wrong_length].index.tolist()

    errors = format_errors(rows_with_len_error, column_name, error_message)

    return errors


def check_for_missing_values(manifest_dataframe):
    """
    Returns a list of errors where columns are missing values.
    All required columns are checked for missing values. Optional
    columns are only checked if values are detected.

    Determines if a manifest was directly submitted by the lab. If true, columns that are optional for this case skip validation because they are empty. 

    Special Cases:
    1. When a row data_type is 'run metrics', the following columns can be empty:
        demultiplex_stats_filename
        top_unknown_barcodes_filename
        run_parameters_filename
    2. When the 'sequence_center' column is empty, we assume the manifest is a
       lab direct submission.
    """
    global LAB_DIRECT_SUBMIT

    LAB_DIRECT_SUBMIT = False

    errors = []
    for column_name in manifest_dataframe.columns:
        if column_name in OPTIONAL_COLUMNS:
            # Optional columns (alternate_contact)

            # Check if column has any values. If values are present True,
            # otherwise False
            column_has_values = set(manifest_dataframe[column_name].notna())

            if True in column_has_values:
                # Confirm all rows are populated. Create errors for rows that
                # are missing values.
                df_mask = manifest_dataframe[column_name].isna()
                rows_missing_vals = manifest_dataframe[df_mask].index.tolist()

                missing_val_errors = format_errors(rows_missing_vals, column_name, "missing value")
                errors.extend(missing_val_errors)

        elif column_name in COLUMNS_EMPTY_FOR_LAB_DIRECT_SUBMIT:
            # This column is empty, all columns in
            # COLUMNS_EMPTY_FOR_LAB_DIRECT_SUBMIT must also be empty.

            seq_center_is_empty = manifest_dataframe["sequence_center"].isna()

            if all(seq_center_is_empty):
                # The "sequence_center" column is completely empty, so assume
                # the current column should also be completely empty.

                LAB_DIRECT_SUBMIT = True

                df_mask = manifest_dataframe[column_name].notna()
                rows_have_vals = manifest_dataframe[df_mask].index.tolist()

                has_val_errors = format_errors(
                    rows_have_vals,
                    column_name,
                    "has a value, but should be empty"
                )
                errors.extend(has_val_errors)

            else:
                # The "sequence_center" has one or more values, so assume the
                # column must be completely filled in.
                column_complete_errors = confirm_column_complete(
                manifest_dataframe,
                column_name
            )
            errors.extend(column_complete_errors)

        else:
            # All other columns must have a value for every row.
            column_complete_errors = confirm_column_complete(
                manifest_dataframe,
                column_name
            )
            errors.extend(column_complete_errors)

    return errors


def confirm_column_complete(manifest_dataframe, column_name):
    """
    Returns a list of error messages where a value is missing from a dataframe
    column.
    """
    errors = []

    # FASTQ rows must have values in all columns
    fastq_mask = (manifest_dataframe[column_name].isna()) & \
                (manifest_dataframe["data_type"] == "demultiplexed fastq")
    rows_missing_vals = manifest_dataframe[fastq_mask].index.tolist()

    # Run metrics rows have some empty columns. For columns that are
    # not, values must be present.
    if column_name not in COLUMNS_EMPTY_FOR_RUN_METRICS:
        metrics_mask = (manifest_dataframe[column_name].isna()) & \
                    (manifest_dataframe["data_type"] == "run metrics")
        metrics_missing_vals = manifest_dataframe[metrics_mask].index.tolist()

        rows_missing_vals.extend(metrics_missing_vals)

    column_errors = format_errors(
            rows_missing_vals,
            column_name,
            "missing required value")
    errors.extend(column_errors)

    return errors


def get_rows_from_string_check(manifest_dataframe, column_name, substring, match):
    """
    Returns a list of rows where values either:
    1) did not match, or contain the provided regex pattern or substring, or
    2) matched, or contained, the provided regex pattern or substring.

    The function can be set to return rows that matched the `substring` or did
    not match it. If `match` argument is True, rows that match/contain the
    pattern/substring will be returned. If set to False, rows that do not
    match/contain the pattern/substring will be returned.

    `substring` argument can be a regex pattern or a plain string.
    """

    # Capture values that match substring/pattern
    if match:
        # Returns values that:
        # 1) are not empty (NaN), and
        # 2) match/contain the pattern/substring
        df_mask = (manifest_dataframe[column_name].notna()) & \
                  (manifest_dataframe[column_name].str.contains(substring, na=False))

    # Capture values that do not match substring/pattern
    else:
        # Returns values that:
        # 1) are not empty (NaN), and
        # 2) do not match/contain the pattern/substring
        df_mask = (manifest_dataframe[column_name].notna()) & \
                  (~manifest_dataframe[column_name].str.contains(substring, na=False))

    #Get the row index positions of invalid entries
    rows_with_wrong_ptrn = manifest_dataframe[df_mask].index.tolist()

    return rows_with_wrong_ptrn


def format_errors(row_list, column_name, reason_text):
    """
    Formats validation error information into concise human readable messages.
    Returns list of error messages.
    """
    errors = []
    for row_idx in row_list:
        row_num = row_idx + 2
        errors.append(f"Row {row_num}, Column '{column_name}': {reason_text}.")

    return errors


def validate_column_alternate_contact(manifest_dataframe):
    """
    Checks that alternate contact values are ORCID IDs.
    Field is optional, but if populated all rows must be populated.
    Returns list of error messages.
    """
    column_name = "alternate_contact"
    pattern = "^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}$"
    errors = []

    # Check if column has any values. If values are present True, otherwise False
    column_has_values = set(manifest_dataframe[column_name].notna())

    if True in column_has_values:
        # Get rows that do not match expected ORCID ID pattern
        rows_with_wrong_ptrn = get_rows_from_string_check(
                manifest_dataframe,
                column_name,
                pattern,
                False)

        ptrn_errors = format_errors(
                rows_with_wrong_ptrn,
                column_name,
                "does not match expected ORCID ID pattern")

        errors.extend(ptrn_errors)

    return errors


def validate_column_filename(manifest_dataframe):
    """
    Checks that filename values are:
    1. Unique - filenames must be unique across all rows
    2. Free of directory structure
    3. Length is less than 255 characters
    """
    column_name = "filename"
    substring = "/"
    errors = []

    # 1. Confirm filenames are unique and don't contain directory structure
    unique_filename_count = manifest_dataframe[column_name].unique().shape[0]
    manifest_row_count = manifest_dataframe.shape[0]

    if manifest_row_count != unique_filename_count:
        has_dup_filename = manifest_dataframe[column_name].duplicated()

        #Get the row index positions of invalid entries
        rows_with_dup_filenames = manifest_dataframe[has_dup_filename].index.to_list()

        dup_errors = format_errors(
                rows_with_dup_filenames,
                column_name,
                "contains duplicated filename")

        errors.extend(dup_errors)

    # 2. Confirm filenames don't contain directory structure
    rows_with_dir_struct = get_rows_from_string_check(manifest_dataframe, column_name, substring, True)

    dir_errors = format_errors(
            rows_with_dir_struct,
            column_name,
            "contains directory structure")

    errors.extend(dir_errors)

    # 3. Confirm filename lengths
    len_errors = check_for_length_range_errors(manifest_dataframe, column_name, 8, 255)
    errors.extend(len_errors)

    return errors


def validate_column_data_type(manifest_dataframe, controlled_vocab):
    """
    Checks that data type values are valid controlled vocabulary terms.
    """
    column_name = "data_type"
    cv_terms = controlled_vocab[column_name]
    errors = []

    errors = check_for_enum_errors(manifest_dataframe, column_name, cv_terms)

    return errors


def validate_column_demux_recipe(manifest_dataframe):
    """
    Checks that demux recipe values are within the length constraints.
    Returns a list of error messages.
    """
    column_name = "demux_recipe"
    errors = []

    errors = check_for_length_range_errors(manifest_dataframe, column_name, 3, 32)

    return errors


def validate_column_file_format(manifest_dataframe, controlled_vocab):
    """
    Checks that file format values are:
        1. Valid controlled vocabulary terms.
    Returns a list of error messages.
    """
    column_name = "file_format"
    cv_terms = controlled_vocab[column_name]
    errors = []

    # 1) Confirm values are valid CV terms
    enum_errors = check_for_enum_errors(manifest_dataframe, column_name, cv_terms)
    errors.extend(enum_errors)

    return errors


def validate_column_flow_cell_type(manifest_dataframe):
    """
    Checks that flow cell type values are within the length constraints.
    Returns a list of error messages.
    """
    column_name = "flow_cell_type"
    errors = []

    errors = check_for_length_range_errors(manifest_dataframe, column_name, 3, 45)

    return errors


def validate_column_flow_cell_name(manifest_dataframe):
    """
    Checks that flow cell name values are within the length constraints.
    Returns a list of error messages.
    """
    column_name = "flow_cell_name"
    errors = []

    errors = check_for_length_range_errors(manifest_dataframe, column_name, 3, 45)

    return errors


def validate_column_flow_cell_version(manifest_dataframe):
    """
    Checks that flow cell version values are within the length constraints.
    Returns a list of error messages.
    """
    column_name = "flow_cell_version"
    errors = []

    errors = check_for_length_range_errors(manifest_dataframe, column_name, 3, 45)

    return errors


def validate_column_instrument(manifest_dataframe):
    """
    Checks that instrument values are within the length constraints.
    Returns a list of error messages.
    """
    column_name = "instrument"
    errors = []

    errors = check_for_length_range_errors(manifest_dataframe, column_name, 3, 45)

    return errors


def validate_column_library_aliquot_name(manifest_dataframe):
    """
    Checks that aliquot names are:
        1. Longer than 8 characters and shorter than 128 characters in length
        2. Match expected regex pattern
    Returns a list of error messages.
    """
    column_name = "library_aliquot_name"
    pattern = "^[a-zA-Z0-9_-]+$"
    errors = []

    # 1) Confirm checksum lengths
    len_errors = check_for_length_range_errors(manifest_dataframe, column_name, 8, 255)
    errors.extend(len_errors)

    # 2) Confirm values match expected pattern
    rows_with_wrong_ptrn = get_rows_from_string_check(manifest_dataframe, column_name, pattern, False)

    ptrn_errors = format_errors(
            rows_with_wrong_ptrn,
            column_name,
            "contains invalid characters")

    errors.extend(ptrn_errors)

    return errors


def validate_column_library_lab_pool_name(manifest_dataframe):
    """
    Checks that aliquot names are:
        1. Longer than 3 characters and shorter than 64 characters in length
        2. Match expected regex pattern
    Returns a list of error messages.
    """
    column_name = "library_lab_pool_name"
    pattern = "^[a-zA-Z0-9_-]+$"
    errors = []

    # 1) Confirm checksum lengths
    len_errors = check_for_length_range_errors(manifest_dataframe, column_name, 3, 255)
    errors.extend(len_errors)

    # 2) Confirm values match expected pattern
    rows_with_wrong_ptrn = get_rows_from_string_check(manifest_dataframe, column_name, pattern, False)

    ptrn_errors = format_errors(
            rows_with_wrong_ptrn,
            column_name,
            "contains invalid characters")

    errors.extend(ptrn_errors)

    return errors


def validate_run_metric_columns(manifest_dataframe):
    """
    Checks that run metric filenames are:
        1. Longer than 3 characters and shorter than 255 characters in length
        2. Listed in the manifest in their own rows
    Returns a list of error messages.
    """
    substring = "_"
    errors = []

    for run_metric in RUN_METRICS_COLUMNS:
        # 1) Confirm filename lengths
        rows_with_len_error = check_for_length_range_errors(manifest_dataframe, run_metric, 3, 255)

        len_errors = format_errors(
                rows_with_len_error,
                run_metric,
                "does not match expected character length")

        errors.extend(len_errors)

        # 2) Confirm there's a row for the run metric file listed for each FASTQ row
        fastq_missing_run_metric = (manifest_dataframe[run_metric].notna()) & \
                                    ~(manifest_dataframe[run_metric].isin(manifest_dataframe["filename"]))

        # Get the row index positions of invalid entries
        fastq_rows_missing_run_metric = manifest_dataframe[fastq_missing_run_metric].index.tolist()

        missing_val_errors = format_errors(
                fastq_rows_missing_run_metric,
                run_metric,
                "file not found included in manifest")

        errors.extend(missing_val_errors)

    return errors


def validate_column_md5(manifest_dataframe):
    """
    Checks that MD5 values are:
        1. Correct length - 32 characters
        2. Correct MD5 pattern
    Returns a list of error messages.
    """
    column_name = "checksum"
    pattern = "^[a-f0-9]{32}$"
    errors = []

    # 1) Confirm checksum lengths
    md5_has_wrong_length = manifest_dataframe[column_name].str.len() != 32

    # Get the row index positions of invalid entries
    rows_with_len_error = manifest_dataframe[md5_has_wrong_length].index.tolist()

    len_errors = format_errors(
            rows_with_len_error,
            column_name,
            "does not match expected character length")

    errors.extend(len_errors)

    # 2) Confirm checksums match expected MD5 pattern
    rows_with_wrong_ptrn = get_rows_from_string_check(manifest_dataframe, column_name, pattern, False)

    ptrn_errors = format_errors(
            rows_with_wrong_ptrn,
            column_name,
            "does not match expected MD5 checksum pattern")

    errors.extend(ptrn_errors)

    return errors


def validate_column_sequence_center(manifest_dataframe, controlled_vocab):
    """
    Checks that sequence center column has 1 value across all rows and the
    value is valid controlled vocabulary term.
    Returns a list of error messages.
    """
    column_name = "sequence_center"
    cv_terms = controlled_vocab[column_name]
    errors = []

    # Confirm sequence center has only 1 value listed across all rows
    seq_centers = manifest_dataframe[column_name].str.lower().unique().tolist()
    if len(seq_centers) > 1:
        errors.append(f"Manifest lists more than 1 sequence center for column '{column_name}'.")

    enum_errors = check_for_enum_errors(manifest_dataframe, column_name, cv_terms)
    errors.extend(enum_errors)

    return errors


def validate_column_sgl_name(manifest_dataframe, controlled_vocab):
    """
    Checks that SGL (specimen generating lab) column has 1 value across all
    rows and the value is valid controlled vocabulary term.
    Returns a list of error messages.
    TODO: Implement CV term check. Waiting on list from Suvvi.
    """
    column_name = "sgl_name"
    cv_terms = controlled_vocab[column_name]
    errors = []

    # Confirm sequence center has only 1 value listed across all rows
    sgls = manifest_dataframe[column_name].str.lower().unique().tolist()
    if len(sgls) > 1:
        errors.append(f"Manifest lists more than 1 sequence center for column '{column_name}'.")

    enum_errors = check_for_enum_errors(manifest_dataframe, column_name, cv_terms)
    errors.extend(enum_errors)

    return errors



def validate_manifest(manifest_filename, controlled_vocab):
    """
    Reads the BICAN file manifest and performs validation checks.
    Returns list of errors.
    """
    errors = []

    try:
        manifest_dataframe = pd.read_csv(manifest_filename, sep='\t')

        # Row count validation
        manifest_row_count = manifest_dataframe.shape[0]
        logging.info(f"manifest row count: {manifest_row_count}")

        if manifest_row_count > MANIFEST_ROW_LIMIT:
            errors.append("Manifest row count exceeds limit of {:,} rows.".format(str(MANIFEST_ROW_LIMIT)))

            # Exit here. Why validate something that's too big?!
            return errors

        # Confirm manifest columns are correctly ordered
        if manifest_dataframe.columns.tolist() != BICAN_COLUMNS:
            errors.append("Manifest columns are not in the correct order.")

            # Exit here. We are enforcing column order.
            return errors

        # Program validation
        # 1. Confirm only 1 program is list across all rows
        programs = manifest_dataframe["program"].str.lower().unique().tolist()
        if len(programs) > 1:
            errors.append("Manifest lists more than 1 program for column 'program'.")

        # 2. Confirm BICAN is the listed program
        if PROGRAM_NAME not in programs:
            errors.append("Incorrect manifest version for listed program in column 'program'.")

            # Exit here. The program listed uses a different manifest version/format.
            return errors

        # Check columns for missing values
        missing_vals_errors = check_for_missing_values(manifest_dataframe)
        errors.extend(missing_vals_errors)

        if len(errors) > 0:
            # Exit here. No point in continuing if values are missing
            return errors

        # Sequence center validation
        if not LAB_DIRECT_SUBMIT:
            seq_centers_errors = validate_column_sequence_center(
                manifest_dataframe,
                controlled_vocab
            )
            errors.extend(seq_centers_errors)

        # SGL name validation
        sgl_name_errors = validate_column_sgl_name(
            manifest_dataframe,
            controlled_vocab
        )
        errors.extend(sgl_name_errors)

        # Filename validation
        filename_errors = validate_column_filename(manifest_dataframe)
        errors.extend(filename_errors)

        # Checksum validation
        md5_errors = validate_column_md5(manifest_dataframe)
        errors.extend(md5_errors)

        # Library lab pool name validation
        pool_name_errors = validate_column_library_lab_pool_name(manifest_dataframe)
        errors.extend(pool_name_errors)

        # Library aliquot name validation
        aliqout_name_errors = validate_column_library_aliquot_name(manifest_dataframe)
        errors.extend(aliqout_name_errors)

        # Data type validation
        data_type_errors = validate_column_data_type(
            manifest_dataframe,
            controlled_vocab
        )
        errors.extend(data_type_errors)

        # File format validation
        file_format_errors = validate_column_file_format(
            manifest_dataframe,
            controlled_vocab
        )
        errors.extend(file_format_errors)

        # Run metric files validation
        if not LAB_DIRECT_SUBMIT:
            run_metric_errors = validate_run_metric_columns(manifest_dataframe)
            errors.extend(run_metric_errors)

        # Demux recipe validation
        demux_errors = validate_column_demux_recipe(manifest_dataframe)
        errors.extend(demux_errors)

        #Flow cell type validation
        fc_type_errors = validate_column_flow_cell_type(manifest_dataframe)
        errors.extend(fc_type_errors)

        #Flow cell name validation
        fc_name_errors = validate_column_flow_cell_name(manifest_dataframe)
        errors.extend(fc_name_errors)

        #Flow cell version validation
        if not LAB_DIRECT_SUBMIT:
            fc_version_errors = validate_column_flow_cell_version(manifest_dataframe)
            errors.extend(fc_version_errors)

        # Instrument validation
        instrument_errors = validate_column_instrument(manifest_dataframe)
        errors.extend(instrument_errors)

        # Alternate contact (optional field)
        alt_contact_errors = validate_column_alternate_contact(manifest_dataframe)
        errors.extend(alt_contact_errors)

    except Exception as error:
        # Gracefully exit.

        # Record error in cloud function log.
        traceback_msg = traceback.format_exc()
        logging.info(f"Error occurred while validating BICAN manifest." + \
                f" Error: {error}")
        logging.info(traceback_msg)

        # The error message sent to the submitter.
        error_msg = "An unexpected error occurring during validation." + \
                    " The NeMO Team is looking into it."

        # Overwrite any preexisting errors. Validation was able not to
        # complete, so we will not provide partial results.
        errors = [error_msg]

    return errors
