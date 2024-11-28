"""Delivery methods for TACA."""

import logging
import os
import sys

from taca.delivery.delivery_classes import get_staging_object
from taca.utils.config import CONFIG

logger = logging.getLogger(__name__)


def locate_data_dirs(project):
    """Locate all data locations of a given project."""
    data_locations = CONFIG.get("delivery").get("data_locations")
    project_locations = []
    logger.info(f"Locating {project} directories.")
    for location in data_locations:
        if os.path.isdir(os.path.join(location, project)):
            project_locations.append(os.path.join(location, project))
            logger.info(
                f"Located {project} directory in {os.path.basename(location)}, including it in staging."
            )
        else:
            logger.info(
                f"No directory found for {project} in {os.path.basename(location)}."
            )
    return project_locations


def get_dds_project(project):
    """Set up a DDS project"""
    pass


def stage(project, flowcells, samples):
    """Determine data type and stage accordingly."""
    project_data_dirs = locate_data_dirs(project)

    if not project_data_dirs:
        logger.warning(f"Could not find any data to stage for {project}. Exiting.")
        sys.exit()

    dds_project = get_dds_project(project)
    staging_path = CONFIG.get("delivery").get("staging_path")
    project_staging_path = os.path.join(staging_path, project, dds_project)
    try:
        os.makedirs(project_staging_path)
    except OSError as e:
        logger.error(
            f"An error occurred while setting up the staging directory {project_staging_path}. Aborting."
        )
        raise (e)

    for project_dir in project_data_dirs:
        staging_object = get_staging_object(project, project_dir, flowcells, samples)
        staging_object.stage_data()

    # Update statusdb with DDSID and status "staged" (project, FC or sample level? Maybe new delivery DB?)


def upload_to_dds(project, dds_id):
    "Upload staged data to DDS"
    pass


def release_dds_project(project, dds_id):
    "Release DDS project to user"
    pass
