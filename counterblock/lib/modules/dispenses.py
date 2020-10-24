import logging

from counterblock.lib import util ,blockchain
from counterblock.lib.modules import assets
from counterblock.lib.processor import API

logger = logging.getLogger(__name__)

@API.add_method
def get_dispenses(asset = None, address = None, transaction = None, offset = 0, limit = 500):

    if limit > 500:
        limit = 500
    elif limit < 0:
        raise Exception('limit must be positive')

    target_query = None
    if address != None:
        target_query = 'source = "{}"'.format(address)
    elif asset != None:
        if target_query != None:
            raise Exception('Both address and asset specified')
        else:
            target_query = 'asset = "{}"'.format(asset)
    elif transaction != None:
        if target_query != None:
            if address != None:
                raise Exception('Both address and transaction specified')
            else:
                raise Exception('Both asset and transaction specified')
        else:
            try:
                int(transaction)
                target_query = 'tx_index = {}'.format(str(transaction))
            except ValueError:
                target_query = 'tx_hash = "{}"'.format(transaction)
    else:
        raise Exception('address, asset or block must be set.')

    sql = '''select dispenses.*, blocks.block_time as timestamp from dispenses
            inner join blocks on dispenses.block_index = blocks.block_index
            and dispenses.{} order by block_index DESC
            limit {} offset {}'''.format(target_query, str(limit), str(offset))
    logger.error(sql)

    data_body = util.call_jsonrpc_api('sql', {'query': sql}, abort_on_error=True)['result']

    logger.error(data_body)
    for x in data_body:
        dispenser_query = {
            "query": 'select * from dispensers where tx_hash = "{}"'.format(x['dispenser_tx_hash'])
        }
        dispenser_body = util.call_jsonrpc_api("sql", dispenser_query, abort_on_error=True)['result'][0]
        logger.error(dispenser_body)
        give_quantity = dispenser_body['give_quantity']
        asset_info = assets.get_assets_info([x['asset']])[0]

        x['btc_quantity'] = '{:.8f}'.format(blockchain.normalize_quantity(x['must_give'] * x['satoshirate'], True))
        x['quantity'] = '{:.8f}'.format(blockchain.normalize_quantity(give_quantity,asset_info['divisible']))
        x['asset_longname'] = asset_info['asset_longname']
        x['dispenser'] = x['dispenser_tx_hash']
        del x['dispenser_tx_hash']
        x['give_quantity'] = give_quantity

    return {"data": data_body, "total": len(data_body) }
