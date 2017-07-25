import json


def demo_transform1(rdd, selcolumn='A'):
    """input rdd and information"""

    return rdd.map(json.loads) \
              .filter(lambda x: x.get(selcolumn, False))
