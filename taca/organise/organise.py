"""Flowcell organisation methods for TACA."""

import logging

from taca.organise.flowcells import instantiate_flowcell

logger = logging.getLogger(__name__)


def organise_flowcell(flowcell, project):
    """Determine flowcell type and organise the data accordingly."""
    flowcell_object = instantiate_flowcell()

    flowcell_object.create_org_dir()
    flowcell_object.organise_data()

    logger.info(f"Finished organisation of flowcell {flowcell}.")
