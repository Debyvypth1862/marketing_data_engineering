#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys
# import os
from airbyte_cdk.entrypoint import launch
from source_cellxpert_yaml import SourceCellxpertYaml

# os.environ['APISIX_ENABLED'] = 'false'

if __name__ == "__main__":
    source = SourceCellxpertYaml()
    launch(source, sys.argv[1:])
