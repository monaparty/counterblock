"""
Implements IPFS support as a counterblock plugin

Python 3.x since v1.4.0
"""
import logging

import ipfshttpclient

from counterblock.lib import config

from counterblock.lib.modules import IPFS_PRIORITY_WATCH_PINS, assets, betting
from counterblock.lib.processor import BlockProcessor, StartUpProcessor, CaughtUpProcessor, RollbackProcessor, API, start_task

logger = logging.getLogger(__name__)

STATUS_VALIDATING = 'validating' # Getting hash. The first state
STATUS_AVAILABLE = 'available'   # Cached the hash. The content is avalable somewhere on the IPFS
STATUS_PINNED = 'pinned'         # Pinned the hash on this node. This state is optional.
STATUS_INVALID = 'invalid'        # The content isn't acceptable.

TYPE_IMAGE = 'image'
TYPE_ASSET_INFO = 'asset_info'
TYPE_FEED = 'feed'

def upsert_hash(type, url, status = STATUS_VALIDATING):
    if url is not None and url.startswith('ipfs://'):
        hash = url.split('/')[2]
        config.mongo_db.ipfs_hash.update({
            'type': type,
            'hash': hash,
            'url': url
        },{
            'type': type,
            'hash': hash,
            'url': url,
            'status': status
        }, upsert = True)
        logger.info('status changed, %s,%s,%s,%s' % (type, hash, url, status))
    else:
        logger.error('Unxepected URL passed to upsert_hash: %s' % url)

def invalidate_hash(url):
    if url is not None and url.startswith('ipfs://'):
        hash = url.split('/')[2]
        config.mongo_db.ipfs_hash.update({
            'hash': hash,
        },{
            '$set': {
                'status': STATUS_INVALID
            }
        }, upsert = False)
        logger.info('status changed, %s,%s,%s,%s' % (type, hash, url, status))
    else:
        logger.error('Unxepected URL passed to upsert_hash: %s' % url)

def watch_image(url):
    upsert_hash(TYPE_IMAGE, url)

def watch_asset_info(url):
    upsert_hash(TYPE_ASSET_INFO, url)

def watch_feed(url):
    upsert_hash(TYPE_FEED, url)

def try_pin():
    logger.info('started: try_pin.')

    status_availables = list(config.mongo_db.ipfs_hash.find({ 'status': STATUS_AVAILABLE }))
    with ipfshttpclient.connect(config.IPFS_API_MULTIADDR) as api:
        for o in status_availables:
            try:
                    api.pin.add(o['hash'])

                    config.mongo_db.ipfs_hash.update({
                            'type': o['type'],
                            'hash': o['hash']
                        },{
                            '$set': {
                                'status': STATUS_PINNED
                            }
                        })
                    logger.info('pinned: %s %s' % (o['type'], o['hash']))
            except Exception as e:
                logger.warning('failed to pin (%s %s): %s' % (o['type'], o['hash'], e))
    start_task(try_pin, delay = 5 * 60) # 5 minutes.

def recover_fetches():
    logger.info('started: recover_fetches.')

    status_validatings = list(config.mongo_db.ipfs_hash.find({ 'status': STATUS_VALIDATING }))
    need_refetch_assets = False
    need_refetch_feeds = False

    for o in status_validatings:
        query_asset_info = None
        query_feed = None
        type = o['type']
        if type == TYPE_IMAGE:
            query_asset_info = {'info_status': 'error'}
            query_feed = {'info_status': 'error'}
            need_refetch_assets = True
            need_refetsh_feeds = True
        elif type == TYPE_ASSET_INFO:
            query_asset_info = {'info_url': o['url'], 'info_status': 'error'}
            need_refetch_assets = True
        elif type == TYPE_FEED:
            query_feed = {'info_url': o['url'], 'info_status': 'error'}
            need_refetch_feeds = True
        else:
            raise Exception('Unknown type: %s' % type)

        if query_asset_info is not None:
            config.mongo_db.asset_extended_info.update(
                query_asset_info,
                {'$set': {
                    'info_status': 'needfetch',
                    'fetch_info_retry': 0,  # retry ASSET_MAX_RETRY times to fetch info from info_url
                    'info_data': {},
                    'errors': []
                }}, upsert = False)
        else:
            pass

        if query_feed is not None:
            config.mongo_db.feeds.update(
                query_feed,
                {'$set': {
                    'info_status': 'needfetch',
                    'fetch_info_retry': 0,  # retry ASSET_MAX_RETRY times to fetch info from info_url
                    'info_data': {},
                    'errors': []
                }}, upsert = False)
        else:
            pass

    start_task(recover_fetches, delay = 5 * 60) # 5 minutes.

def validate_availability():
    logger.info('started: validate_availability.')

    def check_image(image_url):
        if image_url and image_url.startswith('ipfs://'):
            upsert_hash(TYPE_IMAGE, image_url, STATUS_AVAILABLE)
        else:
            pass

    def validate_asset_extended_info(o):
        valid_info = config.mongo_db.asset_extended_info.find_one({
            'info_url': o['url'],
            'info_status': 'valid'
        })
        if valid_info is None:
            return
        else:
            pass

        config.mongo_db.ipfs_hash.update(o, {
            '$set': {
                'status': STATUS_AVAILABLE
            }
        })
        if valid_info.get('info_data'):
            check_image(valid_info['info_data'].get('image'))
        else:
            pass

    def validate_feeds(o):
        valid_info = config.mongo_db.feeds.find_one({
            'info_url': o['url'],
            'info_status': 'valid'
        })
        if valid_info is None:
            return
        else:
            pass

        config.mongo_db.ipfs_hash.update(o, {
            '$set': {
                'status': STATUS_AVAILABLE
            }
        })
        if valid_info.get('info_data'):
            info_data = valid_info['info_data']
            check_image(info_data.get('image'))
            operator = info_data.get('operator')
            if operator:
                check_image(operator.get('image'))
            else:
                pass
            targets = info_data.get('targets')
            for target in targets:
                check_image(target.get('image'))

    status_validatings = list(config.mongo_db.ipfs_hash.find({ 'status': STATUS_VALIDATING }))
    for o in status_validatings:
        if o['type'] == TYPE_ASSET_INFO:
            validate_asset_extended_info(o)
        elif o['type'] == TYPE_FEED:
            validate_feeds(o)

    start_task(validate_availability, delay = 5 * 60) # 5 minutes.

@StartUpProcessor.subscribe()
def init():
    config.mongo_db.ipfs_hash.ensure_index('type')
    config.mongo_db.ipfs_hash.ensure_index('hash')
    config.mongo_db.ipfs_hash.ensure_index('url')
    config.mongo_db.ipfs_hash.ensure_index('status')

@CaughtUpProcessor.subscribe()
def start_tasks():
    start_task(validate_availability)
    start_task(recover_fetches)
    if config.IPFS_ALLOW_PIN:
        start_task(try_pin)
    else:
        logger.warning('This node is disabled to pin IPFS contents.')

@RollbackProcessor.subscribe()
def process_rollback(max_block_index):
    if not max_block_index:  # full reparse
        config.mongo_db.ipfs_hash.drop()
    else:
        pass # anyway hash will be required again after transactions are re-blocked.
