#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_smartico_yaml import SourceSmarticoYaml

if __name__ == "__main__":
    source = SourceSmarticoYaml()
    launch(source, sys.argv[1:])
