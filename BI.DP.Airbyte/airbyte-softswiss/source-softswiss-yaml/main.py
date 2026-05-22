#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_softswiss_yaml import SourceSoftswissYaml

if __name__ == "__main__":
    source = SourceSoftswissYaml()
    launch(source, sys.argv[1:])
