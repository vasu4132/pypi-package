"""
    DSXE Framework

    This package is used to "run" DSXE related tasks.

    optional arguments:
      -h, --help            show this help message and exit
      --consoledebug        enable the log messages to be sent to the console AND to the file
      --filter              filter console output (may be comma-delimited)
"""

__author__ = 'shughson@cisco.com'

from mypackage.api.common import API
from mypackage.contacthub.common import CONTACTHUB
from mypackage.dataload.common import DATALOAD
from mypackage.gcp.common import GCP
from mypackage.utils.common import UTILS


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
