#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_mexos_low_level import SourceMexosLowLevel

if __name__ == "__main__":
    source = SourceMexosLowLevel()
    launch(source, sys.argv[1:])
