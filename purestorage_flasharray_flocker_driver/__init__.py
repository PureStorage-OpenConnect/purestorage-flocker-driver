# Copyright Hybrid Logic Ltd.
# Copyright 2016 Pure Storage Inc.
# See LICENSE file for details.

from flocker.node import BackendDescription, DeployerType
from purestorage_flasharray_flocker_driver import purestorage_blockdevice


def api_factory(cluster_id, **kwargs):
    return purestorage_blockdevice.pure_from_configuration(
        cluster_id=cluster_id,
        pure_ip=kwargs.get('pure_ip'),
        pure_api_token=kwargs.get('pure_api_token'),
        pure_storage_protocol=kwargs.get('pure_storage_protocol'),
        pure_manage_purity_hosts=kwargs.get('pure_manage_purity_hosts'),
        pure_chap_host_user=kwargs.get('pure_chap_host_user'),
        pure_chap_host_password=kwargs.get('pure_chap_host_password'),
        pure_verify_https=kwargs.get('pure_verify_https'),
        pure_ssl_cert=kwargs.get('pure_ssl_cert'),
    )


FLOCKER_BACKEND = BackendDescription(
    name=u"purestorage_flasharray_flocker_driver",
    needs_reactor=False,
    needs_cluster_id=True,
    api_factory=api_factory,
    deployer_type=DeployerType.block
)
