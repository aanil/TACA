"""Delivery methods for TACA."""

import logging

logger = logging.getLogger(__name__)


def stage(project, flowcells, samples):
    """Determine data type and stage accordingly."""
    pass


def upload_to_dds(project, dds_id):
    "Upload staged data to DDS"
    pass


def release_dds_project(project, dds_id):
    "Release DDS project to user"
    pass
