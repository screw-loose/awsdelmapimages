from __future__ import absolute_import


import logging
from multiprocessing.dummy import Pool
from subprocess import check_output
import boto3


# module logger.
_logger = logging.getLogger()

# private data; also inherited by child processes.
_cli = None
_bucket = None
_version = None
_max_zoom = None


def delete_map_images(config):
    global _cli
    global _bucket
    global _version
    global _max_zoom

    # store config values in global context for child processes.
    _cli = boto3.client('s3', region_name=config['region'])
    _bucket = config['bucket']
    _version = config['version']
    _max_zoom = config['max_zoom']

    # delete map tiles images; this generally takes awhile.
    _logger.info('==================== START TILE DELETION:')
    pool = Pool(4)
    pool.map(_delete_by_index, range(2 ** _max_zoom), chunksize=8)
    _logger.info('==================== END TILE DELETION.')

    # delete product images; this is pretty quick.
    _logger.info('==================== DELETING PRODUCT IMAGE FILES:')
    args = ['aws', 's3', 'rm', '--recursive', '--only-show-errors',
            's3://{bucket}/maps/{version}/'.format(bucket=_bucket, version=_version)]
    _logger.info(check_output(args))
    _logger.info('==================== FINISHED: DELETED IMAGES FOR MAP VERSION "{}".'.format(_version))


def _generate_keys_for(y_hash, version, max_zoom):
    hash = hex(y_hash)[2:][::-1]
    key_list = []
    for z in xrange(1, max_zoom+1):
        for x in xrange(2**z):
            for y in xrange(2**z):
                if y == y_hash:
                    key_list.append('maps/tiles/{hash}/{version}/{zoom}/tile_{x}_{y}.png'
                                    .format(hash=hash, version=version, zoom=z, x=x, y=y))

    return hash, key_list


def _delete_keys(s3_cli, hash, bucket, keys):
    index = 0
    err_keys = []
    while index < len(keys):
        count = min(1000, len(keys)-index)
        objects = map(lambda k: dict(Key=k), keys[index:index+count])
        index += count
        result = s3_cli.delete_objects(Bucket=bucket, Delete=dict(Objects=objects))
        _logger.info('hash=0x{}: {} (of {})'.format(hash, len(result.get('Deleted', [])), len(objects)))
        for error in result.get('Errors', []):
            err_keys.append(error['Key'])
            _logger.info('{code}: {key}'.format(code=error['Code'], key=error['Key']))

    if err_keys:
        # recursive call to retry failed keys.
        _logger.info('** {} ERRORS; retrying'.format(len(err_keys)))
        _delete_keys(s3_cli, bucket, err_keys)


def _delete_by_index(index):
    hash, keys = _generate_keys_for(index, _version, _max_zoom)
    _logger.info('========== hash=0x{} ({}) [START] =========='.format(hash, index))
    _delete_keys(_cli, hash, _bucket, keys)
    _logger.info('========== hash=0x{} ({}) [END] =========='.format(hash, index))
