#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_buffalo_partner_yaml import SourceBuffaloPartnerYaml

if __name__ == "__main__":
    source = SourceBuffaloPartnerYaml()
    launch(source, sys.argv[1:])
