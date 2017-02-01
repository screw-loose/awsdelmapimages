from __future__ import absolute_import

# standard.
import json
from os import environ
from subprocess import check_call
import traceback

# project.
from awsdelmapimages.delete_map_images import delete_map_images


def copy_log(bucket, version):
    print('copying: /var/log/cloud-init-output.log')
    args = ['aws', 's3', 'cp', '/var/log/cloud-init-output.log',
            's3://{}/maps/tasks/delete_map_images_{}.log'.format(bucket, version)]
    check_call(args)


def main():
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

    # copy the log to S3 for review.
    copy_log(config['bucket'], config['version'])


if __name__ == '__main__':
    main()
