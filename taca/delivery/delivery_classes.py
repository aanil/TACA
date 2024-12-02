"""Delivery classes for TACA."""

import glob
import logging
import os

from datetime import datetime
from taca.utils.config import CONFIG
from taca.utils.filesystem import do_symlink

logger = logging.getLogger(__name__)


def get_staging_object(project, project_dir, flowcells, samples):
    """Identify type of data and instantiate appropriate staging object."""
    if "ONT_TAR" in project_dir:
        return StageTar(project, project_dir, flowcells)
    elif "DATA" in project_dir:
        return StageData(project, project_dir, flowcells, samples)
    elif "ANALYSIS" in project_dir:
        return StageAnalysis(project, project_dir, flowcells, samples)


class Stage:
    """Defines a generic staging object."""

    def __init__(self, project, project_dir, flowcells=None, samples=None):
        self.project_id = project
        self.project_dir = project_dir
        self.flowcells = flowcells
        self.samples = samples
        self.staging_path = CONFIG.get("delivery").get("staging_path")

    def make_staging_path(self):
        """Create a staging dir."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        self.project_staging_path = os.path.join(
            self.staging_path, self.project_id, timestamp
        )
        try:
            os.makedirs(self.project_staging_path)
        except OSError as e:
            logger.error(
                f"An error occurred while setting up the staging directory {self.project_staging_path}. Aborting."
            )
            raise (e)


class StageTar(Stage):
    """Defines an object for staging files in ONT_TAR."""

    def __init__(self, project, project_dir, flowcells=None):
        super().__init__(project, project_dir, flowcells)

    def stage_data(self):
        """Stage tarball and md5sum-file to DELIVERY/PID/TIMESTAMP."""
        self.make_staging_path()
        if self.flowcells:
            fc_tarballs = []
            for fc in self.flowcells:
                fc_tarballs.append(os.path.join(self.project_dir, fc + ".tar"))
        else:
            fc_tarballs = glob.glob(
                os.path.join(self.project_dir, "*.tar")
            )  # TODO: make more accurate
        # future todo: look in DB for FCs that have already been delivered/are failed or aborted and exclude them unless listed in -f option
        for tarball_path in fc_tarballs:  # TODO: handle files/dirs that don't exist
            tarball_filename = os.path.basename(tarball_path)
            do_symlink(
                tarball_path, os.path.join(self.project_staging_path, tarball_filename)
            )
            do_symlink(
                tarball_path + ".md5",
                os.path.join(self.project_staging_path, tarball_filename + ".md5"),
            )
        # future todo: symlink reports
        logger.info(
            f"Successfully staged {self.project_id} to {self.project_staging_path}"
        )


class StageData(Stage):
    """Defines an object for staging files in DATA."""

    def __init__(self, project, project_dir, flowcells=None, samples=None):
        super().__init__(project, project_dir, flowcells, samples)

    def stage_data(self):
        """Stage relevant files in DATA."""
        # future todo: look for any aborted samples that should not be delivered and exclude aborted fc/samples and those not listed in -f and -s
        # Generate md5sums
        # Symlink all data to DELIVERY/PID/DDSID
        # Make xml files
        pass


class StageAnalysis(Stage):
    """Defines an object for staging files in ANALYSIS."""

    def __init__(self, project, project_dir, flowcells=None, samples=None):
        super().__init__(project, project_dir, flowcells, samples)

    def stage_data(self):
        """Stage relevant files in ANALYSIS."""
        # future todo: for BP, look for any aborted samples that should not be delivered and exclude aborted fc/samples and those not listed in -f and -s
        # Generate md5sums
        # Symlink all data to DELIVERY/PID/DDSID
        pass
