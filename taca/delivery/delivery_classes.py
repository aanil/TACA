"""Delivery classes for TACA."""

import logging

from taca.utils.config import CONFIG

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
    """Defines a generic staging object"""

    def __init__(self, project, project_dir, flowcells=None, samples=None):
        self.project_id = project
        self.project_dir = project_dir
        self.flowcells = flowcells
        self.samples = samples


class StageTar(Stage):
    """Defines an object for staging files in ONT_TAR."""

    def __init__(self, project, project_dir, flowcells=None):
        super().__init__(project, project_dir, flowcells)

    def stage_data(self):
        """Stage tarball and md5sum-file."""
        # TODO: Exclude fcs not listed in -f
        # Symlink all data to DELIVERY/PID/DDSID
        pass


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
