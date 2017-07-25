from argparse import ArgumentParser
import importlib
import json


def encode_json(indict):
    """This encodes a json blob to be redshift tolerant"""

    def rekey_json(v):
        """This takes in a an element and changes the python string formatting to
            make the output immediately usable to redshift"""

        if v is None:
            return "null"
        if isinstance(v, unicode):
            return '"' + repr(v)[2:-1]\
                    .replace('\\', '\\\\')\
                    .replace('\"', '\\\"')\
                    + '"'
        elif isinstance(v, str):
            return '"' + repr(v)\
                    .replace('\\', '\\\\')\
                    .replace('\"', '\\\"')\
                    + '"'
        elif isinstance(v, bool):
            return str(v).lower()
        else:
            return str(v)

    return '{' + ','.join(['"{0}": {1}'.format(k.replace('"', ''), rekey_json(v))
                            for k, v in indict.iteritems()]
                          ) + '}'

class ArgHandler(ArgumentParser):
    """Generic wrapper for handling arguments"""

    def __init__(self, **kwargs):

        ArgumentParser.__init__(self, **kwargs)

        self.add_argument('--dest', 
                          help='path for finding data in S3',
                          default=None)

        self.add_argument('--config', 
                          help='path to json config',
                          default='demo1.json')

        self.args = self.parse_args()

        self.__checks()


    def get_dest(self):
        return self.args.dest.rstrip('/')


    def __checks(self):
        """Internal checks for consistency"""
        pass


def init_rdd(datapath):
    """take in spark path for data, return rdd"""

    return sc.textFile(datapath)


def get_config_rdds(rdd_list):
    """take a list of configuration RDDs, return list of RDDs"""

    return dict((rdditem['name'], init_rdd(rdditem['source'])) for rdditem in rdd_list)


def get_function_kwargs(config):
    """input: config dictionary from JSON file
        return: kwargs for function to call

        input a list of spark RDDs from the config file,
        add optional kwargs from the config file on update
    """
    

    # first get the rdds from the config file
    func_kwargs = get_config_rdds(config['rdds'])
    func_kwargs.update(config.get('function_kwargs', {}))

    return func_kwargs


if __name__ == '__main__':

    arghandler = ArgHandler()
    print arghandler.args

    with open(arghandler.args.config, 'r') as jsonfile:
        config = json.load(jsonfile)

    try:
        from pyspark import SparkContext
        sc = SparkContext()
    except:
        print 'epic fail'
        import sys
        sys.exit(1)

    # load module and function defined in config file
    mod = importlib.import_module('plugins.{}'.format(config['module']))
    func = getattr(mod, config['function'])


    # first get the rdds from the config file
    func_kwargs = get_function_kwargs(config)

    # return RDD with some operations happening with N RDDs
    ret = func(**func_kwargs)

    print ret.take(10)

