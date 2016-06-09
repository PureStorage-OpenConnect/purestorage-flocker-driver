# Copyright Hybrid Logic Ltd.
# Copyright 2016 Pure Storage Inc.
# See LICENSE file for details.

"""
Pure Storage Test helpers for ``flocker.node.agents``.
"""

import os
import yaml
import socket
from uuid import uuid4

from twisted.trial.unittest import SkipTest

from purestorage_flasharray_flocker_driver import purestorage_blockdevice


def pure_client_from_environment():
    """
    Create a ``PureFlashArrayConfiguration`` by picking up parameters
    from environment

    :returns: ``PureFlashArrayConfiguration`` Object
    """
    agent_config_path = os.environ.get('AGENT_CONFIG_FILE')
    agent_default_path = '/etc/flocker/agent.yml'
    if agent_config_path is not None and os.path.exists(agent_config_path):
        config_file = open(agent_config_path)
    elif os.path.exists(agent_default_path):
        config_file = open(agent_default_path)
    else:
        raise SkipTest(
            'Supply the path to an Agent config file configured for Pure '
            'Storage using the AGENT_CONFIG_FILE environment variable. '
            'Or use the default /etc/flocker/agent.yml file location.'
            'See: https://docs.clusterhq.com/en/latest/ for more info'
            'on file format for agents.'
        )
    config = yaml.load(config_file.read())
    dataset = config['dataset']

    return purestorage_blockdevice.PureFlashArrayConfiguration(
        dataset.get('pure_ip'),
        dataset.get('pure_api_token'),
        dataset.get('pure_storage_protocol'),
        dataset.get('pure_manage_purity_hosts'),
        dataset.get('pure_chap_host_user'),
        dataset.get('pure_chap_host_password'),
        dataset.get('pure_verify_https'),
        dataset.get('pure_ssl_cert')
    )


def detach_destroy_volumes(api):
    """
    Detach and destroy all volumes known to this API.
    :param : api object
    """
    volumes = api.list_volumes()

    for volume in volumes:
        if volume.attached_to is not None:
            api.detach_volume(volume.blockdevice_id)
        api.destroy_volume(volume.blockdevice_id)


def build_test_device_api(test_case):
    """
    Return a ``Pure Storage Client`and register a ``test_case``
    cleanup callback to remove any volumes that are created during each test.
    :param test_case object
    """
    config = pure_client_from_environment()
    pure = purestorage_blockdevice.FlashArrayBlockDeviceAPI(
        cluster_id=unicode(uuid4()),
        configuration=config
    )
    test_case.addCleanup(detach_destroy_volumes, pure)

    return pure
