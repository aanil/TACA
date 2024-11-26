"""Flowcell classes for TACA."""

import logging
import os

from taca.utils.config import CONFIG
from taca.utils.misc import call_external_command_detached

logger = logging.getLogger(__name__)

def get_flowcell_type(flowcell):
    """Return flowcell type based on flowcell name"""
    pass

def instantiate_flowcell(flowcell, project):
    flowcell_type = get_flowcell_type(flowcell)

    if flowcell_type == "nanopore":
        return NanoporeFlowcell(flowcell=flowcell, project_id=project)
    elif flowcell_type == "illumina":
        return IlluminaFlowcell(flowcell=flowcell, project_id=project)
    elif flowcell_type == "element":
        return ElementFlowcell(flowcell=flowcell, project_id=project)
    else:
        logger.warning(
            f"Flowcell type could not be recognised for flowcell {flowcell}, skipping it."
        )
        return

class Flowcell:
    """Defines a generic Flowcell"""

    def __init__(self, flowcell, project_id):
        self.fc_id = flowcell
        self.project_id = project_id
        self.detsination_path = CONFIG.get("organise", None).get(
            self.fc_type + "_path", None
        )
        self.organised_project_dir = os.path.join(self.detsination_path, project_id)

    def create_org_dir(self):
        """Create a project directory that the data should be organised to."""
        if not os.path.exists(self.organised_project_dir):
            os.mkdir(self.organised_project_dir)
        return


class NanoporeFlowcell(Flowcell):
    """Defines a Nanopore Flowcell"""

    def __init__(self, flowcell, project_id):
        super().__init__(flowcell, project_id)
        self.fc_type = "nanopore"

    def organise_data(self):
        """Tarball data into ONT_TAR"""
        # generate tarball (detached process? how to signal when tarballing is done?)
        # future todo: also organise data in DATA for easier analysis
        tar_command = f'tar -cf {self.fc_id}'
        call_external_command_detached(tar_command)


class IlluminaFlowcell(Flowcell):
    """Defines a Illumina Flowcell"""

    def __init__(self, flowcell, project_id):
        super().__init__(flowcell, project_id)
        self.fc_type = "illumina"

    def organise_data(self):
        """Symlink data into DATA"""
        pass


class ElementFlowcell(Flowcell):
    """Defines a Element Flowcell"""

    def __init__(self, flowcell):
        super().__init__(flowcell)
        self.fc_type = "element"

    def organise_data(self):
        """Symlink data into DATA"""
        pass
