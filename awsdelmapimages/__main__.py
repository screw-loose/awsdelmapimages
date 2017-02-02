from __future__ import absolute_import

# standard.
import json
import logging
from os import environ
from subprocess import check_call

# project.
from awsdelmapimages.delete_map_images import delete_map_images


_LOG_FILE_NAME = '/var/log/glassnetic.log'
_logger = logging.getLogger()


def copy_log(bucket, version, suffix):
    suffix = "" if suffix is None else '_' + suffix
    _logger.info('copying GLASSNETIC log file.')
    args = ['aws', 's3', 'cp', _LOG_FILE_NAME,
            's3://{}/maps/tasks/delete_map_images_{}{}.log'.format(bucket, version, suffix)]
    check_call(args)

    _logger.info('copying CLOUD-INIT log file.')
    args = ['aws', 's3', 'cp', '/var/log/cloud-init-output.log',
            's3://{}/maps/tasks/delete_map_images_{}_cloud_output.log'.format(bucket, version)]
    check_call(args)


def main():
    logging.basicConfig(level=logging.DEBUG, filename=_LOG_FILE_NAME)

    suffix = None
    config = dict(bucket='glsmap', version='ERROR')
    try:
        cfg_str = environ.get('SCRIPT_CONFIG', "{}")
        _logger.info('config string: {}'.format(cfg_str))

        errors = 0
        cfg = json.loads(cfg_str)
        for k in ('region', 'bucket', 'version', 'max_zoom'):
            if k not in cfg:
                _logger.info('** CONFIG ERROR: missing "{}"'.format(k))
                errors += 1

        if errors == 0:
            _logger.info('config: {}'.format(cfg))
            config = cfg
            delete_map_images(config)
    except Exception:
        _logger.exception('EXCEPTION')
        suffix = 'ERROR'

    # flush logs, then copy the log to S3 for review.
    logging.shutdown()
    copy_log(config['bucket'], config['version'], suffix)


if __name__ == '__main__':
    main()
