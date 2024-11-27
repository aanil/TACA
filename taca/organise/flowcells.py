"""Flowcell classes for TACA."""

import logging
import os
import re
import subprocess

from taca.utils import filesystem
from taca.utils.config import CONFIG

logger = logging.getLogger(__name__)


def get_flowcell_object(flowcell, project):
    if re.match(filesystem.RUN_RE_ONT, flowcell):
        return NanoporeFlowcell(flowcell=flowcell, project_id=project)
    elif re.match(filesystem.RUN_RE_ILLUMINA, flowcell):
        return IlluminaFlowcell(flowcell=flowcell, project_id=project)
    elif re.match(filesystem.RUN_RE_ELEMENT, flowcell):
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
        self.incoming_path = os.path.join(
            CONFIG.get("organise", None).get("incoming_path", None)
        )
        self.fc_path_incoming = os.path.join(self.incoming_path, self.fc_id)
        self.project_id = project_id

    def create_org_dir(self):
        """Create a project directory that the data should be organised to."""
        if not os.path.exists(self.organised_project_dir):
            os.mkdir(self.organised_project_dir)
        return


class NanoporeFlowcell(Flowcell):
    """Defines a Nanopore Flowcell"""

    def __init__(self, flowcell, project_id):
        super().__init__(flowcell, project_id)
        self.destination_path = CONFIG.get("organise", None).get("nanopore_path", None)
        self.organised_project_dir = os.path.join(self.destination_path, project_id)
        self.tar_file = self.fc_id + ".tar"
        self.tar_path = os.path.join(self.organised_project_dir, self.tar_file)
        self.md5_path = self.tar_path + ".md5"

    def organise_data(self):
        """Tarball data into ONT_TAR"""
        # future todo: also organise data in DATA for easier analysis
        with filesystem.chdir(self.incoming_path):
            try:
                tar_command = ["tar", "-cvf", self.tar_path, self.fc_id]
                subprocess.run(tar_command)  # TODO: catch out and err in files
            except subprocess.CalledProcessError as e:
                raise e
        with filesystem.chdir(self.organised_project_dir):
            try:
                md5_command = ["md5sum", self.tar_file]
                result = subprocess.run(
                    md5_command, capture_output=True, text=True
                )  # TODO: catch err in files
                with open(self.md5_path, "w") as md5_file:
                    md5_file.write(result.stdout)
            except subprocess.CalledProcessError as e:
                raise e
        # TODO: Add a timestamp to statusdb indicating when the FC was organised


class IlluminaFlowcell(Flowcell):
    """Defines a Illumina Flowcell"""

    def __init__(self, flowcell, project_id):
        super().__init__(flowcell, project_id)

    def organise_data(self):
        """Symlink data into DATA"""
        pass


class ElementFlowcell(Flowcell):
    """Defines a Element Flowcell"""

    def __init__(self, flowcell):
        super().__init__(flowcell)

    def organise_data(self):
        """Symlink data into DATA"""
        pass
