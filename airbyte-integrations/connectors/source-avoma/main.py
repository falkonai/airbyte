#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_avoma import SourceAvoma

if __name__ == "__main__":
    source = SourceAvoma()
    launch(source, sys.argv[1:])
