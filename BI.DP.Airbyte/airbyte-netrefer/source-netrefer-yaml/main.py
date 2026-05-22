#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_netrefer_yaml import SourceNetreferYaml

if __name__ == "__main__":
    source = SourceNetreferYaml()
    launch(source, sys.argv[1:])
