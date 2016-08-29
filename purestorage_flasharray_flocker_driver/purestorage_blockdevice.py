# Copyright Hybrid Logic Ltd.
# Copyright 2016 Pure Storage Inc.
# See LICENSE file for details..

import base64
import json
import os
import re
import platform
import socket
import uuid

import eliot
from oslo_log import log as logging
import purestorage
from twisted.python import filepath
from zope.interface import implementer
from flocker.node.agents import blockdevice


# Eliot is transitioning away from the "Logger instances all over the place"
# approach.  And it's hard to put Logger instances on PRecord subclasses which
# we have a lot of.  So just use this global logger for now.
_logger = eliot.Logger()

class EliotOsloLogProxy(object):
    """Simple proxy to forward log messages to eliot.

    We will patch this in to be used instead of the oslo.log stuff that is
    currently relied upon for os-brick.
    """
    def __init__(self, logger, name):
        self._logger = logger
        self._name = name

    def _log(self, **msg):
        eliot.Message.new(source=self._name, **msg).write(self._logger)

    def _format(self, msg, args):
        return msg % args

    def log(self, level, msg, *args, **kwargs):
        if args and msg:
            msg = self._format(msg, *args)
        self._log(level=level, msg=msg)

    def debug(self, msg, *args, **kwargs):
        if args and msg:
            msg = self._format(msg, *args)
        self._log(debug=msg)

    def info(self, msg, *args, **kwargs):
        if args and msg:
            msg = self._format(msg, *args)
        self._log(info=msg)

    def warning(self, msg, *args, **kwargs):
        if args and msg:
            msg = self._format(msg, *args)
        self._log(warning=msg)

    def error(self, msg, *args, **kwargs):
        if args and msg:
            msg = self._format(msg, *args)
        self._log(error=msg)

    def critical(self, msg, *args, **kwargs):
        if args and msg:
            msg = self._format(msg, *args)
        self._log(critical=msg)

    def exception(self, msg, *args, **kwargs):
        eliot.write_traceback()
        if args and msg:
            msg = self._format(msg, *args)
        self._log(exception=msg)

    def __getattr__(self, name):
        """If we are called for anything else just ignore it..."""
        def stub_method(*args, **kwargs):
            self._log(warning='LOG called with invalid method.',
                      invalid_method=name,
                      args=str(args),
                      kwargs=str(kwargs))

        return stub_method


def get_logger_proxy(name, *args, **kwargs):
    return EliotOsloLogProxy(_logger, name)

# TODO(patrickeast): See if there is a better way to intercept the os-brick logging...
# Until then we are going to just patch the logging module that os_brick will
# try and use. This has to be done before we import the connector.
logging.getLogger = get_logger_proxy
from os_brick.initiator import connector

MiB = 1048576  # bytes

PURE_ALLOCATION_UNIT = 1 * MiB
PURE_BASE_PREFIX = 'flocker'

FIBRE_CHANNEL = 'FIBRE_CHANNEL'
ISCSI = 'ISCSI'

# Purity REST API Error message string matching helpers...
ERR_MSG_ALREADY_EXISTS = 'already exists'
ERR_MSG_NOT_EXIST = 'does not exist'
ERR_MSG_PENDING_ERADICATION = 'has been destroyed'
ERR_MSG_NOT_CONNECTED = 'is not connected'

class InvalidConfig(Exception):
    def __init__(self, reason):
        msg = 'Invalid configuration: {0}.'.format(reason)
        Exception.__init__(self, msg)

class UnmanagedPurityHostNotFoundException(Exception):
    def __init__(self):
        msg = 'Unable to find an existing Purity host with IQN or WWN for current host.'
        Exception.__init__(self, msg)

class UnknownStorageProtocolException(Exception):
    def __init__(self, protocol):
        msg = 'Unknown storage protocol "{0}".'.format(protocol)
        Exception.__init__(self, msg)


class PureFlashArrayConfiguration(object):
    def __init__(self, ip, api_token, storage_protocol,
                 manage_purity_hosts, chap_host_user,
                 chap_host_password, verify_https, ssl_cert):
        self.ip = ip
        self.api_token = api_token

        if storage_protocol is not None:
            self.storage_protocol = storage_protocol
        else:  # default
            self.storage_protocol = ISCSI

        if manage_purity_hosts is not None:
            self.manage_purity_hosts = manage_purity_hosts
        else:  # default
            self.manage_purity_hosts = True

        self.chap_host_user = chap_host_user
        self.chap_host_password = chap_host_password
        self.verify_https = verify_https
        self.ssl_cert = ssl_cert

    def __str__(self):
        return str({
            'ip': self.ip,
            'api_token': self.api_token,
            'storage_protocol': self.storage_protocol,
            'manage_purity_hosts': self.manage_purity_hosts,
            'chap_host_user': self.chap_host_user,
            'chap_host_password': self.chap_host_password,
            'verify_https': self.verify_https,
            'ssl_cert': self.ssl_cert
        })

@implementer(blockdevice.IBlockDeviceAPI)
@implementer(blockdevice.IProfiledBlockDeviceAPI)
class FlashArrayBlockDeviceAPI(object):
    """
    Implementation of the``IBlockDeviceAPI`` which creates volumes
    (devices) with Pure Storage FlashArray.
    """

    VERSION = '1.0.0'

    def __init__(self, configuration, cluster_id):
        """
       :param configuration: FlashArrayconfiguration
       :param cluster_id: Flocker cluster id
       :param hostname: Current hostname
       :param allocation_unit: allocation_unit
       """
        self._cluster_id = cluster_id
        self._hostname = unicode(socket.gethostname())
        self._conf = configuration


        eliot.Message.new(
            info='Initializing FlashArrayBlockDeviceAPI',
            cluster_id=self._cluster_id,
            hostname=self._hostname,
            config='PureFlashArrayConfiguration = {0}'.format(self._conf)
        ).write(_logger)

        self._validate_config()  # Will raise exception if something is missing

        self._full_vol_prefix = '{0}-{1}'.format(PURE_BASE_PREFIX,
                                                 self._cluster_id)

        # TODO(patrickeast): remove this hack...  we trim to allow for
        #  '-<vol id>' postfix in the name without going over the 63 char
        # limit on volumes. We need to have the full dataset_id in them, but
        # can skimp on the cluster_id and hope we don't get two with the same
        # starting uuid. Ideally we store metadata somewhere so we have both
        # full paths available so we can correctly list_volumes later.
        self._vol_prefix = self._full_vol_prefix[:26] + '-'

        ua = '{cls}/{version} (flocker; {protocol}; {sys} {sys_version};)'.format(
            cls=self.__class__.__name__,
            version=self.VERSION,
            protocol=self._conf.storage_protocol,
            sys=platform.system(),
            sys_version=platform.version()
        )
        self._array = purestorage.FlashArray(self._conf.ip,
                                             api_token=self._conf.api_token,
                                             verify_https=self._conf.verify_https,
                                             ssl_cert=self._conf.ssl_cert,
                                             user_agent=ua)

        self._connector = connector.InitiatorConnector.factory(
            self._conf.storage_protocol,
            None,
            use_multipath=True,
        )
        self._initiator_info = self._get_initiator_info()
        eliot.Message.new(info='Found initiator info: ' + str(self._initiator_info)).write(_logger)

        self._purity_hostname = self._ensure_purity_host()
        eliot.Message.new(info='Using Purity host: ' + str(self._purity_hostname)).write(_logger)

        self._volume_path_cache = {}

    def _validate_config(self):
        if not self._conf.ip:
            raise InvalidConfig('Missing required config parameter pure_ip')

        if not self._conf.api_token:
            raise InvalidConfig('Missing required config parameter pure_api_token')

        if not self._conf.storage_protocol in [ISCSI, FIBRE_CHANNEL]:
            raise InvalidConfig('Storage protocol {} is not a valid option.'
                                .format(self._conf.storage_protocol))

        if ((self._conf.chap_host_user and not self._conf.chap_host_password)
                or (self._conf.chap_host_password and not self._conf.chap_host_user)):
            raise InvalidConfig('CHAP support requires both pure_chap_host_user'
                                'and pure_chap_host_password.')

        if self._conf.ssl_cert and not self._verify_https:
            eliot.Message.new(warning='pure_ssl_cert specified but '
                                      'pure_verify_https is disabled. Requests '
                                      'are not being validated with certificate!')

    @staticmethod
    def _get_initiator_info():
        info = connector.get_connector_properties(None, None, True, True)
        if not 'wwpns' in info:
            info['wwpns'] = []

        if not 'initiator' in info:
            info['initiator'] = []
        elif not isinstance(info['initiator'], list):
            info['initiator'] = [info['initiator']]

        return info

    def _get_managed_purity_hostname(self):
        return '{0}-{1}'.format(PURE_BASE_PREFIX, self._hostname)

    def _find_purity_host(self):
        hosts = self._array.list_hosts()
        purity_host = None
        managed_hostname = self._get_managed_purity_hostname()
        for host in hosts:
            # If there is one with our specific node name... take it, but
            # only if there isn't another one with the right iqn/wwns
            if host['name'] == managed_hostname:
                purity_host = host

            if self._conf.storage_protocol == FIBRE_CHANNEL:
                purity_wwpns = [wwpn.lower() for wwpn in host['wwn']]
                for wwn in self._initiator_info['wwpns']:
                    if wwn.lower() in purity_wwpns:
                        return host
            eliot.Message.new(debug='Looking at host: ' + str(host)).write(_logger)
            if self._conf.storage_protocol == ISCSI:
                for iqn in self._initiator_info['initiator']:
                    eliot.Message.new(debug='Checking for iqn {0} in {1}'.
                                      format(iqn, host['iqn'])).write(_logger)
                    if iqn in host['iqn']:
                        return host

        return purity_host

    def _ensure_purity_host(self):
        """Ensure that a Purity host exists for this compute instance.

        If configured to manage the host we will create one as needed and/or
        modify the host to to ensure that it has this initiators iqn/wwns and
        CHAP credentials setup.

        If not configured to manage the host we will just try and find a host
        to use, and if that fails log a message and raise exception.
        """
        purity_host = self._find_purity_host()

        if not self._conf.manage_purity_hosts:
            if purity_host:
                return purity_host['name']
            else:
                eliot.Message.new(Error='Unable to find purity host with '
                                  'iqn or wwn for initiator.').write(_logger)
                raise UnmanagedPurityHostNotFoundException()

        if not purity_host:
            purity_host = self._array.create_host(
                self._get_managed_purity_hostname(),
                wwnlist=self._initiator_info['wwpns'],
                iqnlist=self._initiator_info['initiator']
            )
        else:
            # Make sure the wwns/iqns are setup for the host
            if self._conf.storage_protocol == FIBRE_CHANNEL:
                wwnlist = []
                purity_wwpns = [wwpn.lower() for wwpn in purity_host['wwn']]
                for wwpn in self._initiator_info['wwpns']:
                    if not wwpn.lower() in purity_wwpns:
                        wwnlist.append(wwpn)

                self._array.set_host(
                    purity_host['name'],
                    addwwnlist=wwnlist,
                )
            elif self._conf.storage_protocol == ISCSI:
                iqnlist = []
                for iqn in self._initiator_info['initiator']:
                    if not iqn in purity_host['iqn']:
                        iqnlist.append(self._initiator_info['initiator'])

                self._array.set_host(
                    purity_host['name'],
                    addiqnlist=iqnlist
                )

                if self._conf.chap_host_user and self._conf.chap_host_password:
                    self._array.set_host(
                        purity_host['name'],
                        host_user=self._conf.chap_host_user,
                        host_password=self._conf.chap_host_password
                    )

        return purity_host['name']

    @staticmethod
    def _round_to_mib(bytes):
        return int(math.ceil(float(bytes) / MiB))

    def _vol_name_from_dataset_id(self, dataset_id):
        return self._vol_prefix + str(dataset_id)

    def _dataset_id_from_vol_name(self, vol_name):
        return uuid.UUID(vol_name[len(self._vol_prefix):])

    def _connect_volume(self, vol_name):
        """Connect the volume object to our Purity host.

        We need to return a dictionary with target information that will be
        consumed by os-brick.
        """
        try:
            connection = self._array.connect_host(self._purity_hostname, vol_name)
        except purestorage.PureHTTPError as err:
            if err.code == 400 and ERR_MSG_ALREADY_EXISTS in err.text:
                raise blockdevice.AlreadyAttachedVolume(vol_name)
            elif err.code == 400 and ERR_MSG_NOT_EXIST in err.text:
                raise blockdevice.UnknownVolume(vol_name)
            else:
                raise
        return self._format_connection_info(connection)

    def _disconnect_volume(self, vol_name):
        try:
            self._array.disconnect_host(self._purity_hostname, vol_name)
        except purestorage.PureHTTPError as err:
            if err.code == 400 and ERR_MSG_NOT_CONNECTED in err.text:
                raise blockdevice.UnattachedVolume(vol_name)
            elif err.code == 400 and ERR_MSG_NOT_EXIST in err.text:
                raise blockdevice.UnknownVolume(vol_name)
            else:
                raise

    def _get_target_info(self, vol_name):
        """Build a dictionary of information about the target.

        :param vol_name:
        :return: dictionary containing the following info:

        iSCSI:
            target_portal(s) - ip and optional port
            target_iqn(s) - iSCSI Qualified Name
            target_lun(s) - LUN id of the volume
        FC:
            target_wwn - World Wide Name
            target_lun - LUN id of the volume

        ALL:
            volume - A dictionary representation of the Purity volume object
        """
        conn_info = {}
        try:
            connected_hosts = self._array.list_volume_private_connections(vol_name)
        except purestorage.PureHTTPError as err:
            if err.code == 400 and ERR_MSG_NOT_EXIST in err.text:
                raise blockdevice.UnknownVolume(vol_name)
            else:
                raise
        for host_info in connected_hosts:
            if host_info["host"] == self._purity_hostname:
                conn_info = host_info
                break

        if not conn_info:
            raise blockdevice.UnattachedVolume(vol_name)

        return self._format_connection_info(conn_info)

    def _get_target_iscsi_ports(self):
        """Return list of iSCSI-enabled port descriptions."""
        ports = self._array.list_ports()
        iscsi_ports = [port for port in ports if port['iqn']]
        return iscsi_ports

    def _get_target_wwns(self):
        """Return list of wwns from the array"""
        ports = self._array.list_ports()
        return [port["wwn"] for port in ports if port["wwn"]]

    def _format_connection_info(self, purity_connection_info):
        props = {}

        if self._conf.storage_protocol == ISCSI:
            props['target_discovered'] = False

            target_ports = self._get_target_iscsi_ports()

            port_iter = iter(target_ports)
            target_luns = []
            target_iqns = []
            target_portals = []
            for port in port_iter:
                target_luns.append(purity_connection_info['lun'])
                target_iqns.append(port['iqn'])
                target_portals.append(port['portal'])

            # If we have multiple ports always report them.
            if target_luns and target_iqns and target_portals:
                props['target_luns'] = target_luns
                props['target_iqns'] = target_iqns
                props['target_portals'] = target_portals

            if self._conf.chap_host_password and self._conf.chap_host_user:
                props['auth_method'] = 'CHAP'
                props['auth_username'] = self._conf.chap_host_user
                props['auth_password'] = self._conf.chap_host_password

        elif self._conf.storage_protocol == FIBRE_CHANNEL:
            props['target_discovered'] = True
            props['target_lun'] = purity_connection_info['lun']
            props['target_wwn'] = self._get_target_wwns()
        else:
            raise UnknownStorageProtocolException(self._conf.storage_protocol)

        return props

    def compute_instance_id(self):
        """
        :return: Compute instance id
        """
        return self._purity_hostname

    def allocation_unit(self):
        """
        Return allocation unit
        """
        return PURE_ALLOCATION_UNIT

    def create_volume(self, dataset_id, size):
        """
        Create a volume of specified size on the Pure Storage FlashArray.
        The size shall be rounded off to 1MB, as Pure Storage creates
        volumes of these sizes.

        See ``IBlockDeviceAPI.create_volume`` for parameter and return type
        documentation.
        """

        vol_name = self._vol_name_from_dataset_id(dataset_id)
        volume = blockdevice.BlockDeviceVolume(
            blockdevice_id=unicode(vol_name),
            size=size,
            attached_to=None,
            dataset_id=dataset_id,
        )
        eliot.Message.new(Info="Creating Volume: " + vol_name).write(_logger)
        self._array.create_volume(vol_name, size)
        return volume

    def create_volume_with_profile(self, dataset_id, size, profile_name):
        """Create a new volume on the array.
        :param dataset_id: The Flocker dataset ID for the volume.
        :param size: The size of the new volume in bytes.
        :param profile_name: The name of the storage profile for
                             this volume.
        :return: A ``BlockDeviceVolume``
        """
        # We only have one type of volume: fast
        return self.create_volume(dataset_id, size)

    def destroy_volume(self, blockdevice_id):
        """
        Destroy an existing volume.
        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :return: ``None``
        """
        try:
            eliot.Message.new(Info="Destroying Volume" + str(blockdevice_id)).write(_logger)
            try:
                self._disconnect_volume(blockdevice_id)
            except blockdevice.UnattachedVolume:
                # Don't worry if it is not connected, normally it won't be
                pass
            self._array.destroy_volume(blockdevice_id)
        except purestorage.PureHTTPError as err:
            if (err.code == 400 and
                    (ERR_MSG_NOT_EXIST in err.text
                     or ERR_MSG_PENDING_ERADICATION in err.text)):
                raise blockdevice.UnknownVolume(blockdevice_id)
            else:
                eliot.Message.new(Error="Failed to delete volume for dataset "
                                  + str(blockdevice_id),
                            Exception=err).write(_logger)
                raise

    def attach_volume(self, blockdevice_id, attach_to):
        """
        Attach ``blockdevice_id`` to the node indicated by ``attach_to``.
        :param unicode blockdevice_id: The unique identifier for the block
            device being attached.
        :param unicode attach_to: An identifier like the one returned by the
            ``compute_instance_id`` method indicating the node to which to
            attach the volume.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises AlreadyAttachedVolume: If the supplied ``blockdevice_id`` is
            already attached.
        :returns: A ``BlockDeviceVolume`` with a ``attached_to`` attribute set
            to ``attach_to``.
        """

        eliot.Message.new(Info="Attaching volume %s to %s" %
                               (blockdevice_id, attach_to)).write(_logger)
        # Connect the volume internally in Purity so it is exposed for the
        # initiator.
        target_info = self._connect_volume(blockdevice_id)

        # Do initiator connection steps to attach and discover the device.
        self._connector.connect_volume(target_info)

        volume = self._array.get_volume(blockdevice_id)
        eliot.Message.new(Info="Finished attaching volume" + str(blockdevice_id)).write(_logger)


        return blockdevice.BlockDeviceVolume(
            blockdevice_id=volume['name'],
            size=volume['size'],
            attached_to=attach_to,
            dataset_id=self._dataset_id_from_vol_name(volume['name'])
        )

    def detach_volume(self, blockdevice_id):
        """
        Detach ``blockdevice_id`` from whatever host it is attached to.
        :param unicode blockdevice_id: The unique identifier for the block
            device being detached.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to anything.
        :returns: ``None``
        """
        eliot.Message.new(Info="Detaching volume" + str(blockdevice_id)).write(_logger)
        target_info = self._get_target_info(blockdevice_id)

        # Disconnect on the initiator first
        self._connector.disconnect_volume(target_info, None)

        # Now disconnect internally in Purity
        self._disconnect_volume(blockdevice_id)
        eliot.Message.new(Info="Finished detaching volume" + str(blockdevice_id)).write(_logger)

    def list_volumes(self):
        """
        Return ``BlockDeviceVolume`` instances for all managed volumes.
        """
        volumes = []
        pure_vols = self._array.list_volumes()
        for vol in pure_vols:
            name = vol['name']
            if name.startswith(self._vol_prefix):
                eliot.Message.new(Info="Found Purity volume managed by flocker " + str(vol)).write(_logger)
                attached_to = None
                try:
                    host_connections = self._array.list_volume_private_connections(name)
                except purestorage.PureHTTPError as err:
                    if err.code == 400 and ERR_MSG_NOT_EXIST in err.text:
                        break  # Carry on... nothing to see here...
                    else:
                        raise
                for connection in host_connections:
                    # Look for one thats our host, if not we'll take anything
                    # else that is connected. It *should* only ever be one
                    # host, but just in case we loop through them all...
                    if connection['host'] == self._purity_hostname:
                        # Make sure there is a path on the system, meaning
                        # it is fully attached.
                        try:
                            self.get_device_path(name)
                        except blockdevice.UnattachedVolume:
                            pass
                        else:
                            attached_to = self._purity_hostname
                            break
                    else:
                        attached_to = connection['host']
                eliot.Message.new(Info="Volume %s attached_to = %s" % (vol['name'], attached_to)).write(_logger)

                volumes.append(blockdevice.BlockDeviceVolume(
                    blockdevice_id=name,
                    size=vol['size'],
                    attached_to=attached_to,
                    dataset_id=self._dataset_id_from_vol_name(name),
                ))
        return volumes

    def get_device_path(self, blockdevice_id):
        """
        Return the device path that has been allocated to the block device on
        the host to which it is currently attached.
        Returning the wrong value here can lead to data loss or corruption
        if a container is started with an unexpected volume. Make very
        sure you are returning the correct result.
        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :returns: A ``FilePath`` for the device.
        """
        eliot.Message.new(Info="Looking for a volume path for {0}"
                          .format(blockdevice_id)).write(_logger)

        if blockdevice_id in self._volume_path_cache:
            path = self._volume_path_cache[blockdevice_id]
            eliot.Message.new(Info="Found volume path for {0} in cache at {1}"
                          .format(blockdevice_id, path)).write(_logger)
            return filepath.FilePath(path)

        target_info = self._get_target_info(blockdevice_id)

        host_devices = self._connector.get_volume_paths(target_info)
        eliot.Message.new(Info="Found volume paths for {0} at {1}"
                          .format(blockdevice_id, host_devices)).write(_logger)

        valid_device = None
        if host_devices:
            valid_device = next(dev for dev in host_devices if os.path.exists(dev))

        if not valid_device:
            raise blockdevice.UnattachedVolume(blockdevice_id)

        # Slow way, check the output of multipath -l, and scan through the devices
        # TODO: check if friendly names are off and just use /dev/mapper/<WWN>
        valid_device = os.path.realpath(valid_device)
        mpath_info = self._connector._linuxscsi.find_multipath_device(valid_device)
        path = mpath_info['device']

        eliot.Message.new(Info="Using volume path for {0} at {1}"
                          .format(blockdevice_id, path)).write(_logger)

        self._volume_path_cache[blockdevice_id] = path
        return filepath.FilePath(path)


def pure_from_configuration(cluster_id, pure_ip, pure_api_token,
                            pure_storage_protocol, pure_manage_purity_hosts,
                            pure_chap_host_user, pure_chap_host_password,
                            pure_verify_https, pure_ssl_cert):
    """
    :param cluster_id: Flocker cluster id.
    :param pure_ip: Management IP Address for the Array
    :param pure_api_token: API Token for management REST API calls.
    :return: FlashArrayBlockDeviceAPI object
    """
    return FlashArrayBlockDeviceAPI(
        configuration=PureFlashArrayConfiguration(
            pure_ip,
            pure_api_token,
            pure_storage_protocol,
            pure_manage_purity_hosts,
            pure_chap_host_user,
            pure_chap_host_password,
            pure_verify_https,
            pure_ssl_cert
        ),
        cluster_id=cluster_id,
    )
