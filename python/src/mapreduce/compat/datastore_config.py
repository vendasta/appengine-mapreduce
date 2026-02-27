"""Compatibility replacements for datastore_rpc.Configuration and related constants.

Replaces google.appengine.datastore.datastore_rpc usage.
"""

MAX_RPC_BYTES = 1048576


class Configuration:
    """Simple replacement for datastore_rpc.Configuration."""

    def __init__(self, deadline=None, force_writes=False):
        self.deadline = deadline
        self.force_writes = force_writes


def create_config(deadline=None, force_writes=False):
    """Replacement for datastore.CreateConfig."""
    return Configuration(deadline=deadline, force_writes=force_writes)
