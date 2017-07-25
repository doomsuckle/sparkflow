
import json
import random
import uuid

def gen():
    """
    generate some sample data at random
    """
    
    col_den = random.sample(list('ABC'), random.randint(0, 1))
    if not col_den:
        floatval = random.gauss(0, 1)
    elif col_den[0] == 'A':
        floatval = random.gauss(4, 2)
    elif col_den[0] == 'B':
        floatval = random.gauss(-4, 1)
    elif col_den[0] == 'C':
        floatval = random.gauss(4, 2) + random.gauss(-4, 1)


    colres = None
    if col_den:
        colres = col_den[0]


    return {
        'uuid': uuid.uuid4().urn.split(':')[-1],
        'A': 'A' in col_den,
        'B': 'B' in col_den,
        'C': 'C' in col_den,
        'colres': colres,
        'floatval': floatval
    }


def main():

    ngen = 10000
    nfiles = 5

    for jfile in xrange(1, nfiles+1):
        soutfile = 'data/demo1/file_{:04d}.txt'.format(jfile)
        print soutfile

        with open(soutfile, 'w') as outfile:
            
            for jgen in xrange(ngen):
                if jgen > 0:
                    outfile.write('\n')
                outfile.write(json.dumps(gen()))


if __name__ == '__main__':

    main()


