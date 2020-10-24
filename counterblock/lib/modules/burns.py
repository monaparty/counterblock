import logging

from counterblock.lib import util ,blockchain
from counterblock.lib.processor import API

logger = logging.getLogger(__name__)

@API.add_method
def get_burns(address = None, block = None, offset = 0, limit = 500):

    if limit > 500:
        limit = 500
    elif limit < 0:
        raise Exception('limit must be positive')

    if address != None and block != None:
        raise Exception('Canot set address and block at same time.')
    if address == None and block == None:
        raise Exception('address or block must be set.')
    target_query = 'source = "{}"'.format(address) if address != None else 'block_index = ' + str(block)

    sql = ('''select burns.*, blocks.block_time as timestamp from burns
            inner join blocks on burns.block_index = blocks.block_index
            and burns.{} order by block_index DESC limit {} offset {}'''
          ).format(target_query, str(limit), str(offset))

    data_body = util.call_jsonrpc_api("sql", {"query": sql}, abort_on_error=True)["result"]

    for x in data_body:
        x['burned'] = '{:.8f}'.format(blockchain.normalize_quantity(x['burned'], True))
        x['earned'] = '{:.8f}'.format(blockchain.normalize_quantity(x['earned'], True))

    return {
        "data": data_body,
        "total": len(data_body)
    }
