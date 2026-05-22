#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_ego_yaml import SourceEgoYaml

if __name__ == "__main__":
    source = SourceEgoYaml()
    launch(source, sys.argv[1:])
