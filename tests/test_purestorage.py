# Copyright Hybrid Logic Ltd.
# Copyright 2015 Pure Storage Inc.
# See LICENSE file for details.

"""
Functional tests for
``PureStorageBlockDeviceAPI``
"""

import functools
import os
from uuid import uuid4

from twisted.trial.unittest import SynchronousTestCase, SkipTest

from flocker.node.agents.test.test_blockdevice import make_iblockdeviceapi_tests

from tests.utils import testtools_purestorage

PURE_ALLOCATION_UNIT = int(1024 * 1024)


def purestorageblockdeviceapi_for_test(test_case):
    """
    Create a ``PureStorageBlockDeviceAPI`` instance for use in tests.
    :returns: A ``PureCinderBlockDeviceAPI`` instance
    """
    user_id = os.getuid()
    if user_id != 0:
        raise SkipTest(
            "``PureStorageBlockDeviceAPI`` queries for iSCSI initiator name which is owned by root, "
            "Required UID: 0, Found UID: {!r}".format(user_id)
        )
    pure = testtools_purestorage.build_test_device_api(test_case)
    return pure

class PureStorageBlockDeviceAPIInterfaceTests(
    make_iblockdeviceapi_tests(
        blockdevice_api_factory=functools.partial(purestorageblockdeviceapi_for_test),
        minimum_allocatable_size=PURE_ALLOCATION_UNIT,
        device_allocation_unit=None,
        unknown_blockdevice_id_factory=lambda test: u"vol-00000000"
    )
):

    """
	Interface adherence Tests for ``PureStorageBlockDeviceAPI``
	"""
