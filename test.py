import platform
import sys
from pprint import pprint
from asammdf import MDF

mdf=MDF('data/Acura_M52_NB_Comfort.MF4')
print(mdf.version)

pprint("python=" + sys.version)
pprint("os=" + platform.platform())

try:
    import numpy
    pprint("numpy=" + numpy.__version__)
except ImportError:
    pass

try:
    import asammdf
    pprint("asammdf=" + asammdf.__version__)
except ImportError:
    pass