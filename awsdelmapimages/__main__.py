from __future__ import absolute_import

# standard.
import json
import logging
from os import environ
from subprocess import check_call
import traceback

# project.
from awsdelmapimages.delete_map_images import delete_map_images


def copy_log(bucket, version, suffix):
    suffix = "" if suffix is None else '_' + suffix
    print('copying: /var/log/cloud-init-output.log')
    args = ['aws', 's3', 'cp', '/var/log/cloud-init-output.log',
            's3://{}/maps/tasks/delete_map_images_{}{}.log'.format(bucket, version, suffix)]
    check_call(args)


def main():
    suffix = None
    config = dict(bucket='glsmap', version='ERROR')
    try:
        cfg_str = environ.get('SCRIPT_CONFIG', "{}")
        print('config string: {}'.format(cfg_str))

        errors = 0
        cfg = json.loads(cfg_str)
        for k in ('region', 'bucket', 'version', 'max_zoom'):
            if k not in cfg:
                print('** CONFIG ERROR: missing "{}"'.format(k))
                errors += 1

        if errors == 0:
            print('config: {}'.format(cfg))
            config = cfg
            delete_map_images(config)
    except Exception:
        traceback.print_exc()
        suffix = 'ERROR'

    # flush logs, then copy the log to S3 for review.
    logging.shutdown()
    copy_log(config['bucket'], config['version'], suffix)


if __name__ == '__main__':
    main()
