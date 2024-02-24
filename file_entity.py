#!/usr/bin/env python3

"""
file_entity.py

Functions and classes to aid in managing submitted files
"""

import hashlib
import logging
import os, re, sys
import shutil
import gzip


from abc import ABC, abstractmethod
from pathlib import Path


# Chunking size for md5 computation
BLOCKSIZE = 65536

ASPERA_EXT = ".aspx"

# Misc functions


def gunzip_file(gzip_file):
    """Run "gunzip" on a file and return the extracted filename.

    Args:
        gzip_file (str): Filepath for a file with gzip compression

    Returns:
        str: Filepath for the uncompressed version of the file
    """

    # Return original file if it is not gzipped
    if not gzip_file.endswith(".gz"):
        return gzip_file
    (gunzip_file, _sep, _after) = gzip_file.rpartition(".gz")
    with gzip.open(gzip_file, 'rb') as f_in:
        with open(gunzip_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    # Now that file is extracted, remove original file
    os.remove(gzip_file)
    return gunzip_file


def gzip_file(gunzip_file):
    """Run "gzip" on a file and return the compressed filename.

    Args:
        gunzip_file (str): Filepath for a file that is uncompressed

    Returns:
        str: Filepath for the gzip-compressed version of the file
    """

    # Return original file if already gzipped
    if gunzip_file.endswith(".gz"):
        return gunzip_file
    gzip_file = gunzip_file + ".gz"
    with open(gunzip_file, 'rb') as f_in:
        with gzip.open(gzip_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    # Now that file is compressed, rmove original file
    os.remove(gunzip_file)
    return gzip_file


class FileEntity(ABC):

    # States:
    # NOT SUBMITTED -> TRANSFERRING -> SUBMITTED -> INVALID/VALID -> VALIDATED (if validated)
    # Final state is VALID when validating manifest contents
    # Final state when ingesting submitted files is VALIDATED

    STATES = ["NOT SUBMITTED", "TRANSFERRING",
              "SUBMITTED", "INVALID", "VALID", "VALIDATED"]

    def __init__(self, filepath, subtype, file_prefix, is_validation=False, **kwargs) -> None:
        if not kwargs:
            kwargs = {}

        self.filepath = filepath
        self.orig_filepath = filepath
        self.file_prefix = file_prefix
        self.technique = None
        if "Technique" in kwargs:
            self.technique = kwargs["Technique"]

        self.state = "NOT SUBMITTED"
        if not is_validation:
            self.logger = self.setup_logger
            self.logger.info("Creating an instance of file_entity.{}.{}".format(
                self.get_class_name(), Path(self.filepath).name))
        self.error = None

        self.validated_dir = None
        if "validated_dir" in kwargs:
            self.validated_dir = kwargs["validated_dir"]
        self.validated_file = None
        self.release_file = None
        self.sample_id = None
        self.version = 1    # Component files get version adjusted in bundle entity
        self.size = None
        self.mtime = None
        self.md5 = None
        # Will store either 'component' file identifiers or "derived"
        self.identifier_table = None
        self.file_identifier = None
        self.file_type = None
        self.file_subtype = subtype
        self.is_bundled = False
        self.bundle_entity = None

        # Kwargs assignment
        self.first_prefix_half = kwargs.get("first_prefix_half", None)
        self.second_prefix_half = kwargs.get("second_prefix_half", None)


    def compute_md5(self) -> None:
        """Compute md5 checksum for file

        Args:
            Nothing

        Returns:
            Nothing. Sets "md5" object property.
        """

        self.logger.info("Computing md5 of {}".format(self.filepath))
        hasher = hashlib.md5()
        with open(self.filepath, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        self.md5 = hasher.hexdigest()

    def compute_mtime(self) -> None:
        """Compute mtime for file

        Args:
            Nothing

        Returns:
            Nothing. Sets "mtime" object property.
        """

        self.logger.info("Computing mtime of {}".format(self.filepath))
        self.mtime = Path(self.filepath).stat().st_mtime

    def compute_size(self) -> None:
        """Compute size for file

        Args:
            Nothing

        Returns:
            Nothing. Sets "size" object property.
        """

        self.logger.info("Computing size of {}".format(self.filepath))
        self.size = Path(self.filepath).stat().st_size

    def create_gcp_url(self):
        """Create a GCP bucket URL for the file.

        Args:
            Nothing

        Returns:
            string: A GCP url if the validated file is public and has "biccn" in the directory path.  Otherwise the "validated_file" property.
        """

        # Note this will have to change for "other" and "biccc", and only works for "public" datasets

        if "biccn" not in self.validated_file or "public" not in self.validated_file:
            self.logger.warn("File URI for {} will not have an automatically-created GCP URL".format(self.validated_file))

        GCP_URL_PREFIX="gs://nemo-public/biccn_unbundled"
        return self.validated_file.replace("/local/projects-t3/NEMO/public/validated/brain/biccn", GCP_URL_PREFIX)

    def create_https_url(self):
        """Create an HTTPS URL for the file.

        Args:
            Nothing

        Returns:
            string: An HTTPS url if the release file is public.  Otherwise the "release_file" property.
        """

        if "public" not in self.release_file:
            self.logger.warn("File URI for {} will not have an automatically-created URL".format(self.filepath))

        # In the past we have done http, but would rather use HTTPS.  If the certificate is down, then hitting these URLs will not work
        HTTPS_URL_PREFIX="https://data.nemoarchive.org"
        return self.release_file.replace("/local/projects-t3/NEMO/public/release/brain", HTTPS_URL_PREFIX)

    def determine_release_path(self) -> None:
        """Use validated path to build release_path.

        Args:
            Nothing

        Returns:
            Nothing.  Sets "release_file" property for file entity
        """

        self.logger.info("Building release area filepath for {}".format(self.filepath))

        release_dir = Path(self.validated_dir.replace("validated", "release"))
        try:
            release_dir.mkdir(mode=0o775, parents=True, exist_ok=True)
        except Exception as e:
            self.record_error("Could not mkdir {}. Skipping".format(
                self.validated_dir))
            self.record_error(str(e))
            self.set_state("INVALID")
            return

        # NOTE: This path will be a "v1" of the final path
        # This property is only important for files copied from "validated" to "release"
        self.release_file = self.validated_file.replace("validated", "release")

    def get_class_name(self):
        """Get and return class name of the current class."""

        return self.__class__.__name__

    def get_modality(self):
        """Get and return modality if valid one is in filepath

        Args:
            Nothing

        Returns:
            string: The modality if found in filepath, otherwise "NA"
        """

        MODALITIES = ["epigenome", "projection", "transcriptome", "multimodal"]
        for modality in MODALITIES:
            if modality in self.filepath:
                return modality
        return "NA"

    def is_tar_member_extracted(self, tar_members) -> bool:
        """If file was a member of a tarball, it skips Aspera transfer.  Jump straight to "SUBMITED" state.

        Args:
            tar_members (list): List of all files in a tarball

        Returns:
            bool: True, if file from tarball has been extracted.  False, otherwise
        """

        # NOTE: Is this technically a subset of the "is_transfer_done" conditions?
        if self.filepath in tar_members and self.state == "NOT SUBMITTED":
            return True
        return False

    def is_transferring(self) -> bool:
        """Is the file currently transferring via Aspera?

        Args:
            Nothing

        Returns:
            bool: True, if file is still transferring from Aspera.  False, otherwise
        """

        if Path(self.filepath + ASPERA_EXT).is_file():
            return True
        return False

    def is_transfer_done(self) -> bool:
        """Has a transferred file finished (lost the Aspera extension file)?

        Args:
            Nothing

        Returns:
            bool: True, if file has finished transferring via Aspera.  False, otherwise
        """

        # Either go through transferring, or resuming an ingest that failed validation (only removed invalid file)
        if Path(self.filepath).is_file() and not Path(self.filepath + ASPERA_EXT).is_file():
            return True
        return False

    def move_to_validated(self) -> None:
        """Move submitted file to validated area, adjusting filename based on version number.

        Args:
            Nothing

        Returns:
            Nothing. The "validated_file" property is set for the file entity.
        """

        validated_dirpath = Path(self.validated_dir)
        try:
            validated_dirpath.mkdir(mode=0o775, parents=True, exist_ok=True)
        except Exception as e:
            self.record_error("Could not mkdir {}. Skipping".format(
                self.validated_dir))
            self.record_error(str(e))
            self.set_state("INVALID")
            return

        # Get the file extension.  Normally use file_prefix but if file_prefix originated from two halves, use the rightmost-half
        prefix_to_split_on = self.second_prefix_half if self.second_prefix_half else self.file_prefix
        (before, sep, extension) = Path(self.filepath).name.rpartition(prefix_to_split_on)

        # Get the filename w/o extension.
        validated_file_prefix = before + sep

        # Chop off odd characters from extension that would make the filename look weird
        unsightly_chars = "._-"
        for c in unsightly_chars:
            if extension.startswith(c):
                extension = extension[1:]
                break

        validated_basename = validated_file_prefix + "." + extension
        # Alter the file name based on versioning
        if self.version > 1:
            validated_basename = validated_file_prefix + ".v" + str(self.version) + "." + extension

        validated_file = str(validated_dirpath.joinpath(validated_basename))

        self.logger.info("Moving {} to validated area {}".format(
            self.filepath, validated_file))

        try:
            self.validated_file = shutil.move(self.filepath, validated_file)
            self.determine_release_path()
            self.set_state("VALIDATED")
        except PermissionError as e:
            self.record_error("Could not move {} to validated area due to permissions.".format(
                self.filepath))
            self.set_state("INVALID")
        except FileNotFoundError as e2:
            self.record_error("File {} is not present in submission area.  Maybe it was moved to validated area already?".format(
                self.filepath))
            self.set_state("INVALID")

    @abstractmethod
    def normalize_file(self) -> None:
        """Get all files into the same format, such as either gzip or gunzip.

        Args:
            Nothing

        Returns:
            Nothing
        """

        self.logger.info("Getting file {} into a standard format".format(self.filepath))

    def record_error(self, error) -> None:
        """Record error to log handler and to easily retrievable property.

        Args:
            error (str) - Error message

        Returns:
            Nothing.  Sets "errors" property for file entity
        """

        self.error = error
        self.logger.error(self.error)

    def set_bundle(self, bundle_entity) -> None:
        """Set BundleEntity to FileEntity object for quick access.

        Args:
            bundle_entity (BundleEntity obj) - BundleEntity subclass object

        Returns:
            Nothing.  Sets "bundle_entity" property for file entity
        """

        self.bundle_entity = bundle_entity

    def set_submitted(self, compute_stats=False) -> None:
        """Set the file state and compute various metrics.

        Args:
            compute_stats (bool) - Optional.  If true, compute various metrics for the files

        Returns:
            Nothing.
        """

        self.set_state("SUBMITTED")
        if compute_stats:
            self.compute_mtime()
            self.compute_md5()
            self.compute_size()

    def set_state(self, new_state) -> None:
        """Set the file state to a new state

        Args:
            new_state (str) - State to set to.

        Returns:
            Nothing.  Sets "state" property of file entity
        """

        if new_state not in self.STATES:
            self.record_error("Tried to change state from {} to non-existing state {}".format(
                self.state, new_state))
        self.state = new_state

    def setup_logger(self):
        """Setup general logger.

        Args:
            Nothing

        Returns:
            Logger object
        """

        logger = logging.getLogger("file_entity.{}.{}".format(
            self.get_class_name(), Path(self.filepath).name))
        logger.setLevel(logging.DEBUG)
        return logger

    def validate_md5(self, md5) -> None:
        """Validate computed md5 against the md5 argument (provided by manifest).

        Args:
            md5 (str): expected MD5 value.

        Returns:
            Nothing. Sets "state" property for file entity
        """

        self.logger.debug("EXPECTED - {}".format(md5))
        self.logger.debug("OBSERVED - {}".format(self.md5))
        if not self.md5 == md5:
            self.set_state("INVALID")
            self.record_error("MD5 of file {} does not match - Expected: {} and computed: {}".format(
                self.filepath, md5, self.md5))
            return
        self.set_state("VALID")


class BamFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "BAM"
        self.is_bundled = True
        self.identifier_table = "alignment_component"

    def normalize_file(self):
        return super().normalize_file()

class BedFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "BED"
        self.identifier_table = "derived"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gunzip_file(self.filepath)

class QbedFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "QBED"
        self.identifier_table = "derived"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gunzip_file(self.filepath)

class BigbedFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "BIGBED"
        self.identifier_table = "derived"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gunzip_file(self.filepath)

class BigwigFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "BIGWIG"
        self.identifier_table = "derived"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gunzip_file(self.filepath)

class CellHashFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "CELLHASH"
        self.is_bundled = True
        self.identifier_table = "sequence_component"

    def normalize_file(self):
        super().normalize_file()

class CramFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "CRAM"
        self.is_bundled = True
        self.identifier_table = "alignment_component"

    def normalize_file(self):
        return super().normalize_file()

class CSVFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "CSV"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gzip_file(self.filepath)

class FastqFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "FASTQ"
        self.is_bundled = True
        self.identifier_table = "sequence_component"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gzip_file(self.filepath)

class FPKMFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "FPKM"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        return super().normalize_file()

class H5File(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "H5"
        self.identifier_table = "derived"

    def normalize_file(self):
        return super().normalize_file()

class H5ADFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "H5AD"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        return super().normalize_file()

class MEXFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "MEX"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gunzip_file(self.filepath)

class SnapFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "SNAP"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        super().normalize_file()
        self.filepath = gzip_file(self.filepath)

class TabAnalysisFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "TAB_ANALYSIS"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        return super().normalize_file()

class TabCountsFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "TAB_COUNTS"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        return super().normalize_file()

class TSVFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "TSV"
        self.is_bundled = True
        self.identifier_table = "derived_component"

    def normalize_file(self):
        return super().normalize_file()

class LoomFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "LOOM"
        self.is_bundled = False
        self.identifier_table = "derived"

    def normalize_file(self):
        self.filepath = gzip_file(self.filepath)

class PlinkBedFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "PLINK_BED"
        self.datatype = ["align"]
        self.is_bundled = False
        self.identifier_table = "alignment"

    def normalize_file(self):
        return super().normalize_file()

class BimFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "BIM"
        self.datatype = ["align"]
        self.is_bundled = False
        self.identifier_table = "alignment"

    def normalize_file(self):
        return super().normalize_file()

class FamFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "FAM"
        self.datatype = ["align"]
        self.is_bundled = False
        self.identifier_table = "alignment"

    def normalize_file(self) -> None:
        return super().normalize_file()

class TextFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "TXT"
        self.datatype = ["other"]
        self.is_bundled = False
        self.identifier_table = "derived"

    def normalize_file(self) -> None:
        return super().normalize_file()

class RFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "R"
        self.datatype = ["other"]
        self.is_bundled = False
        self.identifier_table = "derived"

    def normalize_file(self) -> None:
        return super().normalize_file()

class VcfFile(FileEntity):
    def __init__(self, filepath, subtype, file_prefix, **kwargs) -> None:
        super().__init__(filepath, subtype, file_prefix, **kwargs)
        self.file_type = "VCF"
        self.datatype = ["align"]
        self.is_bundled = True
        self.identifier_table = "alignment"

    def normalize_file(self):
        return super().normalize_file()


# Patterns for the various filetypes
FASTQ_PTRN = r"\.f(ast)?q(\.gz)?$"

# Only capture FASTQ subtype R1/R2/I1/I2 regex group
# Must exclude the CELLHASH pattern
R1_PTRN = r"(?!_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))(-R1|_R1|\.R1|\.read1)(?:_[0-9]{3}|\.raw|\.trimmed|\.trimed)?\.f(?:ast)?q(?:\.gz)?$"
R2_PTRN = r"(?!_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))(-R2|_R2|\.R2|\.read2)(?:_[0-9]{3}|\.raw|\.trimmed|\.trimed)?\.f(?:ast)?q(?:\.gz)?$"
R3_PTRN = r"(?!_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))(-R3|_R3|\.R3|\.read3)(?:_[0-9]{3}|\.raw|\.trimmed|\.trimed)?\.f(?:ast)?q(?:\.gz)?$"

I1_PTRN = r"(?!_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))(-|_|\.)I1(?:_[0-9]{3}|\.raw|\.trimmed|\.trimed)?\.f(?:ast)?q(?:\.gz)?$"
I2_PTRN = r"(?!_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))(-|_|\.)I2(?:_[0-9]{3}|\.raw|\.trimmed|\.trimed)?\.f(?:ast)?q(?:\.gz)?$"

BAM_PTRN = r"\.bam$"
BAI_PTRN = r"\.bam\.bai$"

CRAM_PTRN = r"\.cram$"
CRAI_PTRN = r"\.cram\.crai$"

TSV_PTRN = r"(?<!barcodes)(?<!features)(?<!genes)\.tsv(\.gz)?$"
TSV_IDX_PTRN = r"\.tsv(\.gz)?\.(tbi|idx)$"

MTX_PTRN = r"\.?(matrix|_umi_counts)\.mtx(\.gz)?$"
BARCODES_TSV_PTRN = r"\.?(barcodes|_cell_annotations)\.tsv(\.gz)?$"
GENES_TSV_PTRN = r"\.?(features|genes|_gene_annotations)\.tsv(\.gz)?$"
PEAK_BED_PTRN = r"peaks\.bed(\.gz)?$"

CSV_PTRN = r"(?<!_nuc_hash)\.csv(\.gz)?$"

# Multiome Cell Hashing requires FASTQ R1, R2, and CSV
CELLHASH_CSV_PTRN = r"_nuc_hash\.csv(\.gz)?$"
CELLHASH_FASTQ_R1_PTRN = r"(_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))_R1(?:_[0-9]{3})?\.f(?:ast)?q(?:\.gz)?$"
CELLHASH_FASTQ_R2_PTRN = r"(_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))_R2(?:_[0-9]{3})?\.f(?:ast)?q(?:\.gz)?$"
CELLHASH_FASTQ_R3_PTRN = r"(_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))_R3(?:_[0-9]{3})?\.f(?:ast)?q(?:\.gz)?$"
CELLHASH_FASTQ_I1_PTRN = r"(_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))_I1(?:_[0-9]{3})?\.f(?:ast)?q(?:\.gz)?$"
CELLHASH_FASTQ_I2_PTRN = r"(_[Ss][0-9]{1,2}_[Ll](?:[0-9]{3}|[0-9]{1}-[0-9]{1}))_I2(?:_[0-9]{3})?\.f(?:ast)?q(?:\.gz)?$"

ISOFORMS_FPKM_PTRN = r"barcodes\.fpkm_tracking$"
GENES_FPKM_PTRN = r"genes\.fpkm_tracking$"

BED_PTRN = r"(?<!peaks)(?<!plink)\.bed(\.gz)?$"
QBED_PTRN = r"(?<!peaks)(?<!plink)\.qbed(\.gz)?$"
BIGBED_PTRN = r"(\.bb|\.bigBed|\.bigbed)(\.gz)?$"
BIGWIG_PTRN = r"(\.bw|\.bigWig|\.bigwig)(\.gz)?$"

COL_COUNTS_PTRN = r"COLmeta\.tab$"
ROW_COUNTS_PTRN = r"ROWmeta\.tab$"
MTX_COUNTS_PTRN = r"DataMTX\.tab$"
EXP_JSON_PTRN = r"EXPmeta\.json$"
COL_ANALYSIS_PTRN = r"_?COLmeta_DIMRED_?"
ROW_ANALYSIS_PTRN = r"_?ROWmeta_DIMRED_?"
DIMRED_PTRN = r"_?DIMREDmeta_?"

H5AD_PTRN = r"\.h5ad$"
JSON_PTRN = r"(?<!EXPmeta)\.json$"

H5_PTRN = r"\.h5$"

SNAP_PTRN = r"\.snap$"
SNAP_QC_PTRN = r"\.snap\.qc$"

PLINK_BED_PTRN = r"\.plink\.bed$"
BIM_PTRN = r"\.bim$"
FAM_PTRN = r"\.fam$"

LOOM_PTRN = r"\.loom$"

R_PTRN = r"(\.rds|\.rda|\.Rdata|\.Robj)$"

TXT_PTRN = r"\.txt$"

VCF_PTRN = r"\.vcf\.gz$"
VCF_TBI_PTRN = r"\.vcf\.gz\.tbi$"

# Using lookup table to quickly determine file type
# Source https://stackoverflow.com/questions/33343680/can-a-regular-expression-be-used-as-a-key-in-a-dictionary
FILE_TYPE_LOOKUPS = [
    (BAM_PTRN, "BAM")
    , (BAI_PTRN, "BAM_IDX")

    , (BED_PTRN, "BED")
    , (QBED_PTRN, "QBED")

    , (BIGBED_PTRN, "BIGBED")

    , (BIGWIG_PTRN, "BIGWIG")

    , (CRAM_PTRN, "CRAM")
    , (CRAI_PTRN, "CRAM_IDX")

    , (CSV_PTRN, "CSV")

    # 10X Multiome Cell Hash
    # Cell Hash FASTQs are determined in separate function
    , (CELLHASH_CSV_PTRN, "CELLHASH_CSV")

    , (FASTQ_PTRN, "FASTQ") # Will need to determine type in a separate function

    , (GENES_FPKM_PTRN, "GENES_FPKM")
    , (ISOFORMS_FPKM_PTRN, "ISOFORMS_FPKM")

    , (H5_PTRN, "H5")

    , (H5AD_PTRN, "H5AD")
    , (JSON_PTRN, "H5AD_JSON")

    , (LOOM_PTRN, "LOOM")

    , (BARCODES_TSV_PTRN, "MEX_BARCODES")
    , (GENES_TSV_PTRN, "MEX_GENES")
    , (MTX_PTRN, "MEX_MTX")
    , (PEAK_BED_PTRN, "MEX_PEAK")

    , (SNAP_PTRN, "SNAP")
    , (SNAP_QC_PTRN, "SNAP_QC")

    , (COL_ANALYSIS_PTRN, "TAB_ANALYSIS_COL")
    , (ROW_ANALYSIS_PTRN, "TAB_ANALYSIS_ROW")
    , (DIMRED_PTRN, "TAB_ANALYSIS_DIMRED")

    , (COL_COUNTS_PTRN, "TAB_COUNTS_COL")
    , (ROW_COUNTS_PTRN, "TAB_COUNTS_ROW")
    , (MTX_COUNTS_PTRN, "TAB_COUNTS_MTX")
    , (EXP_JSON_PTRN, "TAB_COUNTS_JSON")

    , (TSV_PTRN, "TSV")
    , (TSV_IDX_PTRN, "TSV_IDX")

    , (PLINK_BED_PTRN, "PLINK_BED")
    , (BIM_PTRN, "BIM")
    , (FAM_PTRN, "FAM")

    , (R_PTRN, "R")
    , (TXT_PTRN, "TXT")

    , (VCF_PTRN, "VCF")
    , (VCF_TBI_PTRN, "VCF_TBI")
]

CLASS_LOOKUP = {
    "BAM": BamFile
    , "BAM_IDX": BamFile

    , "BED": BedFile
    , "QBED": QbedFile

    , "BIGBED": BigbedFile

    , "BIGWIG": BigwigFile

    , "CRAM": CramFile
    , "CRAM_IDX": CramFile

    , "CSV": CSVFile

    # 10X Multiome Cell Hash
    , "CELLHASH_CSV": CellHashFile
    , "CELLHASH_FASTQ_R1": CellHashFile
    , "CELLHASH_FASTQ_R2": CellHashFile
    , "CELLHASH_FASTQ_R3": CellHashFile
    , "CELLHASH_FASTQ_I1": CellHashFile
    , "CELLHASH_FASTQ_I2": CellHashFile

    , "FASTQ_READ1": FastqFile
    , "FASTQ_READ2": FastqFile
    , "FASTQ_READ3": FastqFile
    , "FASTQ_INDEX1": FastqFile
    , "FASTQ_INDEX2": FastqFile
    , "PACBIO_FASTQ": FastqFile

    , "GENES_FPKM": FPKMFile
    , "ISOFORMS_FPKM": FPKMFile

    , "H5": H5File

    , "H5AD": H5ADFile
    , "H5AD_JSON": H5ADFile

    , "LOOM": LoomFile

    , "MEX_BARCODES": MEXFile
    , "MEX_GENES": MEXFile
    , "MEX_MTX": MEXFile
    , "MEX_PEAK": MEXFile

    , "SNAP": SnapFile
    , "SNAP_QC": SnapFile

    , "TAB_ANALYSIS_COL": TabAnalysisFile
    , "TAB_ANALYSIS_ROW": TabAnalysisFile
    , "TAB_ANALYSIS_DIMRED": TabAnalysisFile

    , "TAB_COUNTS_COL": TabCountsFile
    , "TAB_COUNTS_ROW": TabCountsFile
    , "TAB_COUNTS_MTX": TabCountsFile
    , "TAB_COUNTS_JSON": TabCountsFile

    , "TSV": TSVFile
    , "TSV_IDX": TSVFile

    , "PLINK_BED": PlinkBedFile
    , "BIM": BimFile
    , "FAM": FamFile

    , "TXT": TextFile
    , "R": RFile

    , "VCF": VcfFile
    , "VCF_TBI": VcfFile
}


def determine_fastq_component_filetype(filepath, **kwargs):
    """Determine component filetype of a FASTQ file by seeing what component files are present.

    Args:
        filepath (str): filepath for a file in the "incoming" area.
        kwargs (dict): keyword arguments.  Typically will be exactly the same as manifest row headers

    Returns:
        tuple: 2-element tuple consisting of the correct FASTQ pattern to use and an subtype category.

    """
    if not kwargs:
        kwargs = {}

    # R2 may be changed to "FASTQ_BARCODES" and R3 to "FASTQ_READ2" later on
    if "Technique" in kwargs and kwargs['Technique'].lower() == "10x genomics multiome-cell hashing;cell hashing":
        if re.search(CELLHASH_FASTQ_R1_PTRN, filepath):
            return (CELLHASH_FASTQ_R1_PTRN, "CELLHASH_FASTQ_R1")
        elif re.search(CELLHASH_FASTQ_R2_PTRN, filepath):
            return (CELLHASH_FASTQ_R2_PTRN, "CELLHASH_FASTQ_R2")
        elif re.search(CELLHASH_FASTQ_R3_PTRN, filepath):
            return (CELLHASH_FASTQ_R3_PTRN, "CELLHASH_FASTQ_R3")
        elif re.search(CELLHASH_FASTQ_I1_PTRN, filepath):
            return (CELLHASH_FASTQ_I1_PTRN, "CELLHASH_FASTQ_I1")
        elif re.search(CELLHASH_FASTQ_I2_PTRN, filepath):
            return (CELLHASH_FASTQ_I2_PTRN, "CELLHASH_FASTQ_I2")
        else:
            return (None, None)

    elif re.search(R1_PTRN, filepath):
        return (R1_PTRN, "FASTQ_READ1")
    elif re.search(R2_PTRN, filepath):
        return (R2_PTRN, "FASTQ_READ2")
    elif re.search(R3_PTRN, filepath):
        return (R3_PTRN, "FASTQ_READ3")
    elif re.search(I1_PTRN, filepath):
        return (I1_PTRN, "FASTQ_INDEX1")
    elif re.search(I2_PTRN, filepath):
        return (I2_PTRN, "FASTQ_INDEX2")
    elif "Technique" in kwargs and (kwargs['Technique'].lower() == "pacbio long read sequencing" or kwargs['Technique'].lower() == "Oxford Nanopore long read sequencing".lower()):
        # Pacbio reads have no discerning FASTQ patterns so checking by technique from manifest
        return (FASTQ_PTRN, "PACBIO_FASTQ")
    elif filepath.endswith("-R1.fq.gz"):
        # The following patterns are one-time exceptions for the Ecker group and will be removed in future submisssions
        return (R1_PTRN, "FASTQ_READ1")
    elif filepath.endswith("-R2.fq.gz"):
        return (R2_PTRN, "FASTQ_READ2")
    elif filepath.endswith("-R3.fq.gz"):
        return (R3_PTRN, "FASTQ_READ3")
    elif filepath.endswith("-I1.fq.gz"):
        return (I1_PTRN, "FASTQ_INDEX1")
    elif filepath.endswith("-I2.fq.gz"):
        return (I2_PTRN, "FASTQ_INDEX2")
    return (None, None)


def file_entity_factory(filepath, **kwargs):
    """Create a FileEntity sub-class object based on the identifying file pattern.

    Args:
        filepath (str): filepath for a file in the "incoming" area.
        kwargs (dict): keyword arguments.  Typically will be exactly the same as manifest row headers

    Returns:
        object that is a subclass of FileEntity

    Raises:
        ValueError: Cannot determine an appropriate file type for the filepath based on patterns found in the FILE_TYPE_LOOKUPS list.
    """
    found_type = None

    if not kwargs:
        kwargs = {}

    # Use known file pattern and secondary pattern to determine file type and subtype
    for pattern, file_format in FILE_TYPE_LOOKUPS:
        if re.search(pattern, filepath):
            found_type = file_format
            found_pattern = pattern
            if file_format == "FASTQ":
                (found_pattern, found_type) = determine_fastq_component_filetype(filepath, **kwargs)
                if not found_type:
                    raise ValueError(
                        "Cannot determine an appropriate sub-filetype for FASTQ file {}".format(filepath))
            fileclass = CLASS_LOOKUP[found_type]
            break

    if not (found_type and fileclass):
        raise ValueError(
            "Cannot determine an appropriate filetype for file {}".format(filepath))

    # File basename
    filename = Path(filepath).name

    # Get file_prefix by taking part of basename left of the filetype pattern
    file_prefix = re.split(found_pattern, filename)[0]

    # Catch files with no prefix. These file names are the same length of the found regex pattern.
    if len(file_prefix) == 0:
        raise ValueError(f"Cannot determine file prefix. File name is too short for file {filepath}")

    # CELLHASH fastq
    if found_pattern in [CELLHASH_FASTQ_R1_PTRN, CELLHASH_FASTQ_R2_PTRN, CELLHASH_FASTQ_R3_PTRN, CELLHASH_FASTQ_I1_PTRN, CELLHASH_FASTQ_I2_PTRN]:
        if re.search(CELLHASH_FASTQ_R1_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(CELLHASH_FASTQ_R1_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(CELLHASH_FASTQ_R1_PTRN, filename)[1]).stem
        elif re.search(CELLHASH_FASTQ_R2_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(CELLHASH_FASTQ_R2_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(CELLHASH_FASTQ_R2_PTRN, filename)[1]).stem
        elif re.search(CELLHASH_FASTQ_R3_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(CELLHASH_FASTQ_R3_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(CELLHASH_FASTQ_R3_PTRN, filename)[1]).stem
        elif re.search(CELLHASH_FASTQ_I1_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(CELLHASH_FASTQ_I1_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(CELLHASH_FASTQ_I1_PTRN, filename)[1]).stem
        elif re.search(CELLHASH_FASTQ_I2_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(CELLHASH_FASTQ_I2_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(CELLHASH_FASTQ_I2_PTRN, filename)[1]).stem
        else:
            kwargs["first_prefix_half"] = filename.rsplit('-', 1)[0]
            kwargs["second_prefix_half"] = ""

    # FASTQ and TABanalysis files may have basename like <prefix><subtype_ptrn><more_prefix>.<extension>
    # Want the final file_prefix to be <prefix>-<more_prefix>
    elif found_pattern in [R1_PTRN, R2_PTRN, R3_PTRN, I1_PTRN, I2_PTRN, COL_ANALYSIS_PTRN, ROW_ANALYSIS_PTRN, DIMRED_PTRN]:
        # FASTQ files
        if re.search(R1_PTRN, filename):
            # When splitting on the pattern, we want what is to the left and to the right of the subtype pattern
            kwargs["first_prefix_half"] = re.split(R1_PTRN, filename)[0]
            # NOTE: If any part of the pattern is enclosed in parentheses, that pattern match is also included in the list
            second_half = Path(re.split(R1_PTRN, filename)[2]).stem
            # With FASTQ files, we do not know how many suffixes are present, so take whatever was leftmost from the FASTQ pattern
            kwargs["second_prefix_half"] = re.split(FASTQ_PTRN, second_half)[0]
        elif re.search(R2_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(R2_PTRN, filename)[0]
            second_half = Path(re.split(R2_PTRN, filename)[2]).stem
            kwargs["second_prefix_half"] = re.split(FASTQ_PTRN, second_half)[0]
        elif re.search(R3_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(R3_PTRN, filename)[0]
            second_half = Path(re.split(R3_PTRN, filename)[2]).stem
            kwargs["second_prefix_half"] = re.split(FASTQ_PTRN, second_half)[0]
        elif re.search(I1_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(I1_PTRN, filename)[0]
            second_half = Path(re.split(I1_PTRN, filename)[2]).stem
            kwargs["second_prefix_half"] = re.split(FASTQ_PTRN, second_half)[0]
        elif re.search(I2_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(I2_PTRN, filename)[0]
            second_half = Path(re.split(I2_PTRN, filename)[2]).stem
            kwargs["second_prefix_half"] = re.split(FASTQ_PTRN, second_half)[0]

        # TABanalysis files
        elif re.search(COL_ANALYSIS_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(COL_ANALYSIS_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(COL_ANALYSIS_PTRN, filename)[1]).stem
        elif re.search(ROW_ANALYSIS_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(ROW_ANALYSIS_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(ROW_ANALYSIS_PTRN, filename)[1]).stem
        elif re.search(DIMRED_PTRN, filename):
            kwargs["first_prefix_half"] = re.split(DIMRED_PTRN, filename)[0]
            kwargs["second_prefix_half"] = Path(re.split(DIMRED_PTRN, filename)[1]).stem

        else:
            kwargs["first_prefix_half"] = filename.rsplit('-', 1)[0]
            kwargs["second_prefix_half"] = ""

        # Chop off unsightly characters at the beginning of the second prefix half to make the prefix look nice
        unsightly_chars = "._-"
        for i in unsightly_chars:
            if kwargs["second_prefix_half"].startswith(i):
                kwargs["second_prefix_half"] = kwargs["second_prefix_half"][:-1]
                break

        file_prefix = kwargs["first_prefix_half"] + "-" + kwargs["second_prefix_half"]

    # Chop off unsightly characters at the end of the file_prefix (just one)
    unsightly_chars = "._-"
    for i in unsightly_chars:
        if file_prefix.endswith(i):
            file_prefix = file_prefix[:-1]
            break

    return fileclass(filepath, found_type, file_prefix, **kwargs)
