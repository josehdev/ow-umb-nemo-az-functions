#!/usr/bin/env python

"""
bundle_entity.py

Functions and classes to aid in managing bundles
"""

import hashlib
import logging

from abc import ABC, abstractmethod
from pathlib import Path

# Chunking size for md5 computation
BLOCKSIZE = 65536

TYPE_TO_EXT = {
    "BAM": "bam.tar",
    "CELLHASH": "nuc_hash.tar",
    "CRAM": "cram.tar",
    "CSV": "csv.tar",
    "FASTQ": "fastq.tar",
    "FPKM": "fpkm_tracking.tar",
    "H5AD": "h5ad.tar",
    "MEX": "mex.tar.gz",
    "SNAP": "snap.tar.gz",
    "TAB_ANALYSIS": "tab.analysis.tar.gz",
    "TAB_COUNTS": "tab.counts.tar.gz",
    "TSV": "tsv.tar",
    "VCF": "vcf.tar"
}

# Misc functions (nothing yet)


# Bundles


class FileBundle(ABC):

    # States:
    # NOT BUNDLED ->  INVALID/VALID -> RELEASED
    # Final state is VALID when validating manifest contents
    # Final state when ingesting submitted files is RELEASED

    STATES = ["NOT BUNDLED", "INVALID", "VALID", "RELEASED"]

    def __init__(self, file_entity_list, file_type, file_prefix, is_validation) -> None:
        self.file_prefix = file_prefix

        self.state = "NOT BUNDLED"

        self.logger = None
        if is_validation:
            self.logger = self.setup_logger()
            self.logger.info("Creating an instance of file_bundle.{}.{}".format(
                self.get_class_name(), self.file_prefix))
        self.errors = []

        self.component_file_entities = file_entity_list
        self.version = 1    # Will be adjusted in 'determine_release_path'
        self.release_file = None
        self.size = None
        self.mtime = None
        self.md5 = None
        self.file_type = file_type
        self.bundle_extension = TYPE_TO_EXT[self.file_type]

        # Will store either "alignment", "sequence", or "derived"
        self.identifier_table = None
        self.file_identifier = None

        for f in self.component_file_entities:
            f.set_bundle(self)

    def adjust_bundle_version(self, release_dir) -> None:
        """Determine if bundled file exists in the output directory, and adjust version number if needed.

        Args:
            release_dir (Path): The Path object representation of the "release" directory path

        Returns:
            Nothing. Sets "version" object property for bundle entity and component file entities
        """
        if self.logger is not None:
            self.logger.info("Determining if bundle previously existed...")

        # Build a bundle pathname
        tar_base = self.file_prefix + "." + self.bundle_extension
        tar_to_check = release_dir.joinpath(tar_base)   # 'release_dir' is a pathlib.Path object

        version_counter = self.version  # Default 1

        # Keep checking versions of tar bundle until we find one that does not exist
        while(Path(tar_to_check).is_file()):
            version_counter += 1
            tar_base = self.file_prefix + ".v" + str(version_counter)
            new_tar_base = tar_base + "." + self.bundle_extension
            tar_to_check = release_dir.joinpath(new_tar_base)

        self.version = version_counter
        # The component files need to be kept on the same version as the bundle
        for f in self.component_file_entities:
            f.version = self.version

    def compute_md5(self) -> None:
        """
        Compute MD5 checksum for bundle

        Args:
            Nothing

        Returns:
            Nothing. Sets "md5" object property.
        """

        hasher = hashlib.md5()
        with open(self.release_file, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        self.md5 = hasher.hexdigest()

    def compute_mtime(self) -> None:
        """
        Compute mtime for bundle

        Args:
            Nothing

        Returns:
            Nothing. Sets "mtime" object property.
        """

        self.mtime = Path(self.release_file).stat().st_mtime

    def compute_size(self) -> None:
        """
        Compute size for bundle

        Args:
            Nothing

        Returns:
            Nothing. Sets "size" object property.
        """

        self.size = Path(self.release_file).stat().st_size

#
#   def create_file_identifier(self, conn):
#     """Create a bundle-based file identifier.  Goes to either "alignment", "derived", or "sequence"
#
#        Args:
#            conn (Connection) - SQLAlchemy MySQL engine connection
#
#        Returns:
#            string: The generated file identifier.  Also sets "file_identifier" property in the bundle entity
#        """
#
#        self.logger.info("Creating file identifier for {}".format(Path(self.release_file).name))
#
#        if self.identifier_table == "alignment":
#            self.file_identifier = mysql_query.new_alignment_identifier(conn)
#        elif self.identifier_table == "derived":
#            self.file_identifier = mysql_query.new_derived_identifier(conn)
#        elif self.identifier_table == "sequence":
#            self.file_identifier = mysql_query.new_sequence_identifier(conn)
#        else:
#            self.record_error("Identifier table type {} for bundle {} is not valid. Skipping".format(self.identifier_table, Path(self.release_file).name))
#            self.set_state("INVALID")
#        return self.file_identifier

    def create_https_url(self):
        """
        Create an HTTPS URL for the file.

        Args:
            Nothing

        Returns:
            string: An HTTPS url if the release file is public.  Otherwise the "release_file" property.
        """

        if "public" not in self.release_file:
            if self.logger is not None:
                self.logger.warning("File URI for {} will not have an automatically-created URL".format(self.release_file))
            return self.release_file.split('brain')[1]

        # In the past we have done http, but would rather use HTTPS.
        # If the certificate is down, then hitting these URLs will not work
        HTTPS_URL_PREFIX="https://data.nemoarchive.org"
        return self.release_file.replace("/local/projects-t3/NEMO/public/release/brain", HTTPS_URL_PREFIX)

    def determine_release_path(self) -> None:
        """
        Build release filepath for bundle file using the "validated_dir"
        property from a component file entity.

        Args:
            Nothing

        Returns:
            Nothing.  Sets "release_file" property for bundle entity
        """

        validated_dir = self.component_file_entities[0].validated_dir
        release_dir = Path(validated_dir.replace("validated", "release"))
        try:
            release_dir.mkdir(mode=0o775, parents=True, exist_ok=True)
        except Exception as e:
            self.record_error("Could not mkdir {}. Skipping".format(
                self.validated_dir))
            self.record_error(str(e))
            self.set_state("INVALID")
            return

        self.adjust_bundle_version(release_dir)

        release_basename = self.file_prefix + "." + self.bundle_extension
        if self.version > 1:
            release_basename = self.file_prefix + ".v" + str(self.version) + "." + self.bundle_extension
        self.release_file = str(release_dir / release_basename)

    def get_class_name(self):
        """Get and return class name of the current class."""

        return self.__class__.__name__

    def record_error(self, error) -> None:
        """Record error to log handler and to an easily retrievable property.

        Args:
            error (str) - Error message

        Returns:
            Nothing.  Sets "errors" property for bundle entity
        """

        self.errors.append(error)
        if self.logger is not None:
            self.logger.error(error)

    def set_released(self, compute_stats=False) -> None:
        """
        Set the file state to "RELEASED" and compute various metrics.

        Args:
            compute_stats (bool) - Optional.  If true, compute various metrics for the files

        Returns:
            Nothing.
        """

        self.set_state("RELEASED")

        if compute_stats:
            self.compute_md5()
            self.compute_mtime()
            self.compute_size()

    def set_state(self, new_state) -> None:
        """
        Set the file state to a new state

        Args:
            new_state (str) - State to set to.

        Returns:
            Nothing.  Sets "state" property of bundle entity
        """

        if new_state not in self.STATES:
            if self.logger is not None:
                self.logger.error(
                    "Tried to change state from {} to non-existing state {}".format(self.state, new_state))
        self.state = new_state

    def setup_logger(self):
        """
        Setup general logger.

        Args:
            Nothing

        Returns:
            Logger object
        """

        logger = logging.getLogger("bundle_entity.{}.{}".format(
            self.get_class_name(), Path(self.file_prefix).name))
        logger.setLevel(logging.DEBUG)
        return logger

    @abstractmethod
    def validate_contents(self):
        """
        Validate the bundle entity to ensure all requisite files are present.

        Args:
            Nothing

        Returns:
            Nothing
        """
        if self.logger is not None:
            self.logger.info("Validating file bundle {}".format(self.file_prefix))
        sample_ids = {f.sample_id for f in self.component_file_entities}
        if len(sample_ids) > 1:
            self.record_error("All of the files for bundle with prefix '{}' do not have the same sample ID in the manifest.".format(self.file_prefix))
            self.set_state("INVALID")
        validated_paths = {f.validated_dir for f in self.component_file_entities}
        if len(validated_paths) > 1:
            self.record_error("The files for the bundle with prefix '{}' do not have the same metadata for the path. \nThe columns Sub-program, Lab, Modality, Subspecimen_type, Technique, Species, and Data_type should all be the same.")
            self.set_state("INVALID")


class BamBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "alignment"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["BAM"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # BAM_IDX files are optional but recommended
        if "BAM_IDX" not in file_subtypes:
            if self.logger is not None:
                self.logger.warning(
                    "No BAM_IDX file found for prefix '{}'".format(self.file_prefix))

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class CellHashBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "sequence"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")

        if "CELLHASH_FASTQ_I2" in file_subtypes:
            for subtype in ["CELLHASH_FASTQ_I1", "CELLHASH_FASTQ_R1", "CELLHASH_FASTQ_R2"]:
                if subtype not in file_subtypes:
                    self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                        self.file_type, self.file_prefix, subtype))
                    self.set_state("INVALID")
        elif "CELLHASH_FASTQ_I1" in file_subtypes or "CELLHASH_FASTQ_R3" in file_subtypes:
            for subtype in ["CELLHASH_FASTQ_R1", "CELLHASH_FASTQ_R2"]:
                if subtype not in file_subtypes:
                    self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                        self.file_type, self.file_prefix, subtype))
                    self.set_state("INVALID")
        elif "CELLHASH_FASTQ_R2" in file_subtypes:
            for subtype in ["CELLHASH_FASTQ_R1"]:
                if subtype not in file_subtypes:
                    self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                        self.file_type, self.file_prefix, subtype))
                    self.set_state("INVALID")
        elif "CELLHASH_CSV" in file_subtypes:
            for subtype in ["CELLHASH_FASTQ_R1", "CELLHASH_FASTQ_R2"]:
                if subtype not in file_subtypes:
                    self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                        self.file_type, self.file_prefix, subtype))
                    self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class CramBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "alignment"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["CRAM"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # CRAM_IDX files are optional but recommended
        if "CRAM_IDX" not in file_subtypes:
            if self.logger is not None:
                self.logger.warning(
                    "No CRAM_IDX file found for prefix '{}'".format(self.file_prefix))

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class CSVBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["CSV"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class FastqBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "sequence"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")

        # FASTQ can come in various combinations we need to account for these
        # 10X FASTQ files must have I1 (will figure out later)
        # If I2, must have I1
        # If I1, must have R1 and R2
        # If R3, must R1 and R2 (R3 being read2 and R2 being barcodes)
        # If R2, must have R1
        # TODO: Ideally we determine specific FASTQ file combinations based on sequencing technology


        if self.component_file_entities[0].technique == "PacBio long read sequencing".lower() or self.component_file_entities[0].technique == "Oxford Nanopore long read sequencing".lower():
            if len(file_uniq_subtypes) !=1 and "PacBio long read sequencing".lower() in file_uniq_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Only one fastq is allowed for pacbio sequencing.")

        #only require read2 fastq file
        elif self.component_file_entities[0].technique.lower() == "sci-rna-seq3":
            if "FASTQ_INDEX1" in file_subtypes:
                for subtype in ["FASTQ_READ2", "FASTQ_READ1"]:
                    if subtype not in file_subtypes:
                        self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                            self.file_type, self.file_prefix, subtype))
                        self.set_state("INVALID")
            elif "FASTQ_READ1" in file_subtypes:
                    for subtype in ["FASTQ_READ2"]:
                        if subtype not in file_subtypes:
                            self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                                self.file_type, self.file_prefix, subtype))
                            self.set_state("INVALID")

        #standard fastq bundling
        else:
            if "FASTQ_INDEX2" in file_subtypes:
                for subtype in ["FASTQ_INDEX1", "FASTQ_READ1", "FASTQ_READ2"]:
                    if subtype not in file_subtypes:
                        self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                            self.file_type, self.file_prefix, subtype))
                        self.set_state("INVALID")
            elif "FASTQ_INDEX1" in file_subtypes or "FASTQ_READ3" in file_subtypes:
                for subtype in ["FASTQ_READ1", "FASTQ_READ2"]:
                    if subtype not in file_subtypes:
                        self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                            self.file_type, self.file_prefix, subtype))
                        self.set_state("INVALID")
            elif "FASTQ_READ2" in file_subtypes:
                for subtype in ["FASTQ_READ1"]:
                    if subtype not in file_subtypes:
                        self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                            self.file_type, self.file_prefix, subtype))
                        self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class FPKMBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["GENES_FPKM", "ISOFORMS_FPKM"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class H5ADBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")

        # H5AD is required.
        # H5AD_JSON files are optional but recommended
        for subtype in ["H5AD"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class MEXBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        if "MEX_PEAK" in file_subtypes:
            for subtype in ["MEX_BARCODES", "MEX_PEAK", "MEX_MTX"]:
                if subtype not in file_subtypes:
                    self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                        self.file_type, self.file_prefix, subtype))
                    self.set_state("INVALID")
        else: #handle the file without peak files, which replace genes.tsv
            for subtype in ["MEX_BARCODES", "MEX_GENES", "MEX_MTX"]:
                if subtype not in file_subtypes:
                    self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                        self.file_type, self.file_prefix, subtype))
                    self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class SnapBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["SNAP"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        if "SNAP_QC" not in file_subtypes:
            if self.logger is not None:
                self.logger.warning(
                    "No SNAP_QC file found for prefix '{}'".format(self.file_prefix))

        if self.state == "NOT_BUNDLED":
            self.set_state("VALID")


class TabAnalysisBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["TAB_ANALYSIS_COL", "TAB_ANALYSIS_ROW", "TAB_ANALYSIS_DIMRED"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class TabCountsBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")

        # TAB_COUNTS_COL, TAB_COUNTS_ROW, and TAB_COUNTS_MTX are required.
        # TAB_COUNTS_JSON files are optional but recommended
        for subtype in ["TAB_COUNTS_COL", "TAB_COUNTS_ROW", "TAB_COUNTS_MTX"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")


class TSVBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "derived"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["TSV"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # TSV_IDX files are optional but recommended
        if "TSV_IDX" not in file_subtypes:
            if self.logger is not None:
                self.logger.warning(
                    "No TSV_IDX file found for prefix '{}'".format(self.file_prefix))

        self.set_state("VALID")


class VcfBundle(FileBundle):
    def __init__(self, file_entity_list, file_type, file_prefix, is_validation=False) -> None:
        super().__init__(file_entity_list, file_type, file_prefix, is_validation)
        self.identifier_table = "alignment"

    def validate_contents(self):
        super().validate_contents()
        file_subtypes = [f.file_subtype for f in self.component_file_entities]
        file_uniq_subtypes = set(file_subtypes)
        if not len(file_subtypes) == len(file_uniq_subtypes):
            self.record_error("One of the {} files for file prefix '{}' has multiple files of the same subtype".format(
                self.file_type, self.file_prefix))
            self.set_state("INVALID")
        for subtype in ["VCF"]:
            if subtype not in file_subtypes:
                self.record_error("Cannot create {} bundle for prefix '{}': Missing {} subtype file.".format(
                    self.file_type, self.file_prefix, subtype))
                self.set_state("INVALID")

        # VCF_TBI files are optional but recommended
        if "VCF_TBI" not in file_subtypes:
            if self.logger is not None:
                self.logger.warning(
                    "No VCF_TBI file found for prefix '{}'".format(self.file_prefix))

        # Set to valid if validation checks passed
        if self.state == "NOT BUNDLED":
            self.set_state("VALID")

### Bundle entity stuff
BUNDLE_LOOKUP = {
    "BAM": BamBundle,
    "CELLHASH": CellHashBundle,
    "CRAM": CramBundle,
    "CSV": CSVBundle,
    "FASTQ": FastqBundle,
    "FPKM": FPKMBundle,
    "H5AD": H5ADBundle,
    "MEX": MEXBundle,
    "SNAP": SnapBundle,
    "TAB_ANALYSIS": TabAnalysisBundle,
    "TAB_COUNTS": TabCountsBundle,
    "TSV": TSVBundle,
    "VCF": VcfBundle
}

def bundle_entity_factory(file_list):
    """
    Create a new object from a bundle entity subclass based on the type of the
    files

    Args:
        file_list (list): List of the files that will be bundled together.  All
        should share the same file prefix

    Returns:
        object that is a subclass of BundleEntity

    Raises:
        KeyError: Component file types did not match a bundle entity subclass
        type (see BUNDLE_LOOKUP dict)
    """
    # All files should have the same prefix and same file type
    file_prefix = file_list[0].file_prefix

    # pop off final character
    if file_prefix.endswith('.'):
        file_prefix = file_prefix[:-1]

    file_type = file_list[0].file_type

    try:
        bundleClass = BUNDLE_LOOKUP[file_type]
        return bundleClass(file_list, file_type, file_prefix)
    except KeyError as ke:
        raise KeyError("Attempted to bundle an unsupported file type {} for files of prefix {}".format(
            file_type, file_prefix))
