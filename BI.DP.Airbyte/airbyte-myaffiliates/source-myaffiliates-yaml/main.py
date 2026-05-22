#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_myaffiliates_yaml import SourceMyaffiliatesYaml

if __name__ == "__main__":
    source = SourceMyaffiliatesYaml()
    launch(source, sys.argv[1:])
