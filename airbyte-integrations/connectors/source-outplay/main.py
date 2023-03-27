#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_outplay import SourceOutplay
from source_outplay.debug import turn_on_debug_requests

if __name__ == "__main__":
    source = SourceOutplay()
    # turn_on_debug_requests()
    launch(source, sys.argv[1:])
