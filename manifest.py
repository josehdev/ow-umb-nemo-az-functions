import os
import csv
import re
import logging
from collections import Counter
from file_entity import file_entity_factory
from bundle_entity import bundle_entity_factory

# Manifest fields (In order on document)
ALL_FIELDS = [
    "File_name",
    "Sample_ID",
    "Program",
    "Sub-program",
    "Lab",
    "Species",
    "Modality",
    "Technique",
    "Subspecimen_type",
    "Data_type",
    "File_type",
    "Access",
    "Checksum",
    "Anatomical_site",
    "Counts_pipeline",
    "Read_aligner",
    "Genome_build",
    "Gene_set_release",
    "BCDC_Project",
    "BCDC_collection",
    "CA_usage",
    "CA_IC_id",
    "CA_donor",
    "CA_tissue_provider"
]

class Manifest:
    def __init__(self, filepath, controlled_vocabulary) -> None:
        self.filepath = filepath
        self.errors = []
        self.state = "SUBMITTED"
        self.cv = controlled_vocabulary

        # Fields are later used to confirm restricted bucket exists
        self.access_level = None
        self.program = None

    OPTIONAL_FIELDS = [
        "Anatomical_site", "Counts_pipeline", "Read_aligner", "Genome_build",
        "Gene_set_release", "BCDC_Project", "BCDC_collection",
        # Still option even if "controlled" access since data submitters may
        # provide these before or after submission
        "CA_usage", "CA_IC_id", "CA_donor", "CA_tissue_provider"
    ]

    # Fields are required for restricted submissions
    # Later used to upload files to GCP restricted bucket
    RESTRICTED_REQUIRED_FIELDS = ["BCDC_collection", "BCDC_Project", "CA_IC_id", "CA_usage"]

    STATES = ["SUBMITTED", "INVALID", "VALID"]

    def is_access_uniform(self, rows):
        #make sure the data access level is consistent throughout the manifest
        access_levels = set()
        for row in rows:
            access_levels.add(row["Access"])
        if len(access_levels) >=2:
            self.record_error("The manifest has more than one level of" + \
                              " access. There can only be one type of" + \
                              " data in each manifest.")
            return False
        return True

    def is_controlled_vocab_valid(self, rows) -> bool:
        """
        Validate that all manifest fields relying on controlled vocabulary
        are using it for each row.
        """
        valid = True

        for row in rows:
            for key, val in row.items():
                if key in self.OPTIONAL_FIELDS and not val:
                    continue
                if key in self.cv:
                    # Required field validation already check.  The len(val) is to check optional fields
                    if val and not val.lower() in self.cv[key]:
                        self.record_error(
                            "Row for '{}' has invalid controlled vocabulary term '{}' for field '{}'. Valid fields are [{}].".format(
                                row["File_name"],
                                val,
                                key,
                                ";".join(self.cv[key])
                            )
                        )
                        valid = False
                    elif not val:
                        valid = False
        if not valid:
            return False

        return True

    def is_directory_in_filename(self, rows):
        """
        Checks for any directory structure in File_name column. If any
        directory in found, the manifest fails validation (returns True),
        otherwise the manifest passes this check (returns False).
        """
        found_directory_in_filename = False

        for index, row in enumerate(rows):
            row_number = index + 2
            filename = row["File_name"]

            if "/" in filename:
                self.record_error(f"File name {filename} on row" + \
                                  f"#{row_number} contains directory structure.")
                found_directory_in_filename = True

        return found_directory_in_filename

    def is_filename_duplicated_within_row(self, rows):
        """
        Validate that filenames in all manifest rows are not duplicated in
        other columns.
        """
        dupes = []
        for index, row in enumerate(rows):
            row_number = index + 2
            filename = row["File_name"]

            # Count the times each value appears in the row
            row_values = list(row.values())
            counted_values = Counter(row_values)

            # File name is duplicated if it appears more than once within the row
            if counted_values[filename] > 1:
                dupes.append({"filename": filename, "row_number": row_number})

        if len(dupes) == 0:
            # Validation passed. Filenames are not duplicated within their rows
            return False

        for dupe in dupes:
            self.record_error(f"File name {dupe['filename']} was " + \
                              f"duplicated within row #{dupe['row_number']}.")

        return True

    def are_filenames_unique(self, rows) -> bool:
        """
        Validate that all manifest rows have unique filenames.
        """
        uniq_filenames = {row["File_name"] for row in rows}

        if len(uniq_filenames) == len(rows):
            return True

        # Find the filename duplicate
        seen = {}
        dupes = set()
        filenames =  [row["File_name"] for row in rows]
        for f in filenames:
            if f not in seen:
                seen[f] = 1
            else:
                dupes.add(f)

        for dupe in dupes:
            self.record_error("Duplicate file name found: {}.".format(dupe))

        return False

    def set_state(self, new_state) -> None:
        if new_state not in self.STATES:
            self.record_error("Tried to change state from {} to non-existing state {}.".format(
                self.state, new_state))
        self.state = new_state

    def read_data(self, skip_header_check=False):
        """
        Read file using the 'csv' module and return a list of dicts for
        each row.
        """
        delimiter = '\t'

        rows = []

        data_fh = open(self.filepath, 'r')
        lines = data_fh.readlines()
        logging.info(f"File contains {len(lines)} lines.")
        first_line = lines[0]
        header_line = first_line.strip()
        data_fh.close()

        headers = header_line.split(delimiter)

        if not (skip_header_check or headers == ALL_FIELDS):
            raise KeyError("Headers in file are not the same as those in the manifest template.")

        with open(self.filepath, encoding='utf-8-sig') as data_file:
            # "restval" sets the value insered into empty cells. Setting to ''
            # from None (default) protects against edge case where empty cells
            # in rows in the same manifest would be filled with None and others
            # would be ''.
            reader = csv.DictReader(
                data_file,
                delimiter=delimiter,
                dialect='excel',
                restval=''
            )

            # Row is a dictionary of headers => vals for that row
            for index, row in enumerate(reader):
                row_number = index + 2
                if len(row.keys()) != len(ALL_FIELDS):
                    msg = f"Row #{row_number} does not have the correct" + \
                          f" number of fields. Expected {len(ALL_FIELDS)}" + \
                          f" fields, but got {len(row.keys())}."
                    raise ValueError(msg)

                rows.append(row)
            return rows

    def read_file(self):
        """
        Read manifest file. A successful read returns the rows as list of
        dicts. A failed, or unsuccessful, read returns False.
        Column headers are the dict keys.
        """
        rows = None

        try:
            rows = self.read_data()
            return rows
        except FileNotFoundError as fe:
            self.set_state("INVALID")
            self.record_error("Manifest file {} was not found".format(
                                os.path.basename(self.filepath)))
            return False
        except KeyError as ke:
            self.set_state("INVALID")
            self.record_error("Manifest header row fields differ from" + \
                              " the header fields from the template.")
            return False
        except UnicodeDecodeError as ude:
            self.set_state("INVALID")
            self.record_error("The file could not be read. It must be a" + \
                              " plaintext csv file.")
            return False
        except BaseException as e:
            self.set_state("INVALID")
            self.record_error(str(e))
            return False

    def are_required_fields_filled(self, rows) -> bool:
        """
        Validate all manifest rows have required metadata filled.
        """
        valid = True

        for row in rows:
            for key, val in row.items():
                # Skip check on optional fields.
                if key in self.OPTIONAL_FIELDS:
                    continue
                if val is None or not len(val):
                    self.record_error("Row for '{}' does not have required field '{}' filled in.".format(
                        row["File_name"], key))
                    valid = False
                if key == "Checksum":
                    try:
                        if len(val) != 32:
                            self.record_error("Row for '{}' does not have a 32 character checksum.".format(row["File_name"]))
                            valid = False
                        # Make sure the checksum is hexadecimal
                        int(val, 16)
                    # Catch error if the md5 checksum is not a valid hexadecimal
                    # value
                    except ValueError:
                        self.record_error("The checksum for row '{}' is not a valid md5 checksum.".format(row["File_name"]))
                        valid = False

        if not valid:
            return False

        return True

    def has_incorrect_mapping_to_ic_form(self, rows, ic_form_mapping,
            ic_form_field_name, collection_field_name, project_field_name,
            ca_usage_field_name):
        """
        Restricted Manifest Validation.
        Confirms that the IC form, collection, project, and controlled access
        usage all map to each other. Returns True if validation fails,
        otherwise False.

        The relationships between them are the following:
        An IC form can have multiple projects.
        A projects can have multiple collections.
        A collection can have multiple data usage restrictions.
        """
        ic_form = rows[0][ic_form_field_name]
        project = rows[0][project_field_name]
        collection = rows[0][collection_field_name]
        ca_usage = rows[0][ca_usage_field_name]

        # Check if the IC form provided is valid
        if ic_form not in ic_form_mapping.keys():
            self.record_error(f"The '{ic_form_field_name}'" + \
                              f" value '{ic_form}' is invalid.")
            return True

        # Check if the project belongs to the IC form
        if project not in ic_form_mapping[ic_form].keys():
            self.record_error(f"The '{project_field_name}'" + \
                              f" value '{project}' does not map to" + \
                              f" the provided '{ic_form_field_name}'" + \
                              f" value '{ic_form}'.")
            return True

        # Check if the collection belongs to the project
        if collection not in ic_form_mapping[ic_form][project].keys():
            self.record_error(f"The '{collection_field_name}'" + \
                              f" value '{collection}' does not map to the" + \
                              f" provided '{project_field_name}'" + \
                              f" value '{project}'.")
            return True

        # Check if the data usage applies to the collection
        if ca_usage not in ic_form_mapping[ic_form][project][collection]:
            self.record_error(f"The '{ca_usage_field_name}'" + \
                              f" value '{ca_usage}' does not map to the" + \
                              f" provided '{collection_field_name}'" + \
                              f" value '{collection}'.")
            return True

        return False

    def has_multiple_values_in_restricted_columns(self, rows,
            ic_form_field_name, collection_field_name, project_field_name,
            ca_usage_field_name):
        """
        Restricted Manifest Validation.
        Checks if manifest has multiple values in each column used to map to a
        restricted cloud bucket. Returns True if validation fails, otherwise
        returns False.
        """
        failed_validation = False
        err_msg = "The manifest has more than value for the following" + \
                  " column(s). Each column can only have one value in it."

        # Each column can have only 1 value
        fields = {
            ic_form_field_name: set(),
            collection_field_name: set(),
            project_field_name: set(),
            ca_usage_field_name: set(),
        }

        for row in rows:
            fields[ic_form_field_name].add(row[ic_form_field_name])
            fields[collection_field_name].add(row[collection_field_name])
            fields[project_field_name].add(row[project_field_name])
            fields[ca_usage_field_name].add(row[ca_usage_field_name])

        # Check if any field has 2 or more values, if so then validation fails.
        for field, vals in fields.items():
            if len(vals) >= 2:
                failed_validation = True
                err_msg += "\n'{}' column has more than one value: {}.".format(
                           field, ", ".join(vals))

        if failed_validation:
            self.record_error(err_msg)

        return failed_validation

    def has_no_release_bucket(self, rows, bucket_listing, project_field_name, ca_usage_field_name):
        """
        Restricted Manifest Validation.
        Checks if the project short-name and controlled-acccess usage listed in
        the manifest map to an existing GCP restricted bucket.
        The project and usage values are formatted; then concatenated to create
        the expected bucket name. The bucket name is then checked against the
        list of GCP restricted buckets.
        """
        bucket_names = [bucket["name"] for bucket in bucket_listing]
        raw_project = rows[0][project_field_name]
        raw_ca_usage = rows[0][ca_usage_field_name]

        ### Format values to match expected GCP format
        # Bucket names must be lowercase
        project = raw_project.lower()
        ca_usage = raw_ca_usage.lower()

        # Remove _proj if present
        project = re.sub("_proj$","", project)

        # Replace underscores with hyphens
        project = project.replace("_","-")
        ca_usage = ca_usage.replace("_","-")

        expected_bucket_name = f"{project}-{ca_usage}"

        # Confirm expected bucket name actually exists
        if expected_bucket_name not in bucket_names:
            self.record_error(
                f"The provided '{project_field_name}'" + \
                f" ('{raw_project}') and '{ca_usage_field_name}'" + \
                f" ('{raw_ca_usage}') is an invalid combination."
            )
            return True

        return False

    def has_missing_data_in_required_fields(self, rows, ic_form_field_name,
                collection_field_name, project_field_name,
                ca_usage_field_name):
        """
        Restricted Manifest Validation.
        Checks that all file rows have values for the required fields.
        Returns True if data is missing; otherwise returns False.
        """
        has_missing_data = False

        required_fields = [
            ic_form_field_name,
            project_field_name,
            collection_field_name,
            ca_usage_field_name
        ]
        for row in rows:
            for field in required_fields:
                if field not in row or len(row[field]) == 0:
                    self.record_error("Row for '{}' does not have required field '{}' filled in.".format(
                        row["File_name"], field))
                    has_missing_data = True

        return has_missing_data

    def validate_manifest_file(self, rows):
        """
        Process file, creating file entities per row and validating metadata.
        """

        # Limit manifest row count to 50,000 while processing submissions on-prem.
        if len(rows) > 50000:
            self.record_error("Manifest row count exceeds limit of 50,000 rows.")
            self.set_state("INVALID")
            return False

        # Validate manifest contents
        if not self.are_required_fields_filled(rows):
            self.set_state("INVALID")
        if not self.are_filenames_unique(rows):
            self.set_state("INVALID")
        if not self.is_controlled_vocab_valid(rows):
            self.set_state("INVALID")
        if not self.is_access_uniform(rows):
            self.set_state("INVALID")

        if self.is_filename_duplicated_within_row(rows):
            self.set_state("INVALID")
        if self.is_directory_in_filename(rows):
            self.set_state("INVALID")

        self.access_level = rows[0]["Access"]
        self.program = rows[0]["Program"].lower()

        files = []

        try:
            for row in rows:
                for column in row:
                    if "\n" in row[column]:
                        self.record_error("Line break found in entry for file {}.".format(row["File_name"]))

                file_entity = file_entity_factory(row["File_name"], is_validation=True, **row)
                file_entity.sample_id = row["Sample_ID"]
                files.append(file_entity)
        except ValueError as ve:
            self.set_state("INVALID")
            # Propagating from file_entity_factory
            self.record_error(str(ve))
        except Exception as ee:
            # Catching 'build_nemo_dir_from_scratch' errors.
            # Most are probably duplicates of 'is_controlled_vocab_valid'
            self.set_state("INVALID")
            self.record_error(f"An unexpected error occurred: {str(ee)}.")
            return False
        # Check if all possible file bundles have their required files present
        file_prefixes = dict()

        for f in files:

            # Check that all files have a prefix, otherwise record error and
            # fail validation
            if len(f.file_prefix) == 0:
                self.set_state("INVALID")
                self.record_error(f"Row for '{os.path.basename(f.filepath)}' " + \
                                   "has invalid value for field 'File_name'. " + \
                                   "No file prefix detected.")

            # Skip files that not are bundled
            if f.is_bundled is False:
                continue

            #to avoid using filter, sort files into a dictionary of dictionaries of lists
            #file prefix -> file type -> FileEntity object
            if f.file_prefix in file_prefixes:
                if f.file_type in file_prefixes[f.file_prefix]:
                    file_prefixes[f.file_prefix][f.file_type].append(f)
                else:
                    file_prefixes[f.file_prefix][f.file_type] = list()
                    file_prefixes[f.file_prefix][f.file_type].append(f)
            else:
                file_prefixes[f.file_prefix] = dict()
                file_prefixes[f.file_prefix][f.file_type] = list()
                file_prefixes[f.file_prefix][f.file_type].append(f)
        for fp in file_prefixes:
            # Only get bundled file types
            files_by_prefix = file_prefixes[fp]

            for ft in files_by_prefix.keys():
                files_by_type = files_by_prefix[ft]

                try:
                    bundle_entity = bundle_entity_factory(files_by_type)
                    bundle_entity.validate_contents()
                    if bundle_entity.state == "INVALID":
                        self.set_state("INVALID")
                        self.record_error(bundle_entity.errors)
                except KeyError as ke:
                    logging.info(ke)

        if self.state == "INVALID":
            return False

        self.set_state("VALID")

        return True

    def validate_restricted_manifest_fields(self, manifest_rows,
            ic_form_mapping, restricted_bucket_list):
        """
        Entrypoint for validating a restricted manifest.
        Returns False if any check fails validation; otherwise returns True.
        """
        # Required manifest fields.
        # TODO The 4 columns listed are biccn (manifest v1.0). This WILL change
        # and need to be configurable when manifest v2.0 goes live.
        ic_form_field_name = "CA_IC_id"
        project_field_name = "BCDC_Project"
        collection_field_name = "BCDC_collection"
        ca_usage_field_name = "CA_usage"

        if self.has_missing_data_in_required_fields(manifest_rows,
                ic_form_field_name, collection_field_name, project_field_name,
                ca_usage_field_name):
            return False

        if self.has_multiple_values_in_restricted_columns(manifest_rows,
                ic_form_field_name, collection_field_name, project_field_name,
                ca_usage_field_name):
            self.set_state("INVALID")
            return False

        if self.has_incorrect_mapping_to_ic_form(manifest_rows,
                ic_form_mapping, ic_form_field_name, collection_field_name,
                project_field_name, ca_usage_field_name):
            self.set_state("INVALID")
            return False

        if self.has_no_release_bucket(manifest_rows, restricted_bucket_list,
                project_field_name, ca_usage_field_name):
            self.set_state("INVALID")
            return False

        self.set_state("VALID")
        return True

    def record_error(self, error) -> None:
        """
        Record error to easily retrievable property.
        """
        if isinstance(error, list):
            self.errors.extend(error)
        else:
            self.errors.append(error)
