#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_referon_yaml import SourceReferonYaml

if __name__ == "__main__":
    source = SourceReferonYaml()
    launch(source, sys.argv[1:])
