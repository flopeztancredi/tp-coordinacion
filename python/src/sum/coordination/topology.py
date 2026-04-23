import hashlib

import config


def sum_routing_key(sum_id):
    return f"{config.SUM_PREFIX}_{sum_id}"


def sum_routing_keys():
    return [sum_routing_key(sum_id) for sum_id in range(config.SUM_AMOUNT)]


def aggregation_routing_key(aggregation_id):
    return f"{config.AGGREGATION_PREFIX}_{aggregation_id}"


def aggregation_id_for(fruit):
    digest = hashlib.md5(fruit.encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % config.AGGREGATION_AMOUNT
