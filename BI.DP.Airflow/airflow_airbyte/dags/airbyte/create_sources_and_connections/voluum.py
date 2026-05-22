from airflow.models import Variable


class Voluum:
    source_name = Variable.get('voluum_sourceName')
    source_definition_id = Variable.get('voluum_source_definationid')
    client_id = Variable.get('client_id')
    access_key_id = Variable.get('access_key_id')
    access_key = Variable.get('access_key')

    def __init__(self, start_date, loopback_days, affiliate_network_report_recovery_dates, 
                 campaign_report_recovery_dates, conversions_recovery_dates, flow_report_recovery_dates, 
                 lander_report_recovery_dates, offer_report_recovery_dates, traffic_source_report_recovery_dates, 
                 is_recovery) -> None:
        self.start_date = start_date
        self.loopback_days = loopback_days
        self.affiliate_network_report_recovery_dates = affiliate_network_report_recovery_dates
        self.campaign_report_recovery_dates = campaign_report_recovery_dates
        self.conversions_recovery_dates = conversions_recovery_dates
        self.flow_report_recovery_dates = flow_report_recovery_dates
        self.lander_report_recovery_dates = lander_report_recovery_dates
        self.offer_report_recovery_dates = offer_report_recovery_dates
        self.traffic_source_report_recovery_dates = traffic_source_report_recovery_dates
        self.is_recovery = is_recovery

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "client_id": Voluum.client_id,
                "access_key_id": Voluum.access_key_id,
                "access_key": Voluum.access_key,
                "start_date": self.start_date,
                "loopback_days": self.loopback_days,
                "affiliate_network_report_recovery_dates": self.affiliate_network_report_recovery_dates,
                "campaign_report_recovery_dates": self.campaign_report_recovery_dates,
                "conversions_recovery_dates": self.conversions_recovery_dates,
                "flow_report_recovery_dates": self.flow_report_recovery_dates,
                "lander_report_recovery_dates": self.lander_report_recovery_dates,
                "offer_report_recovery_dates": self.offer_report_recovery_dates,
                "traffic_source_report_recovery_dates": self.traffic_source_report_recovery_dates,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Voluum.source_name,
            "sourceDefinitionId": Voluum.source_definition_id,
            "workspaceId": airbyte_workspace_id
        }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "user": user,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "conversions",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ip": {
                                        "type": "string"
                                    },
                                    "os": {
                                        "type": "string"
                                    },
                                    "isp": {
                                        "type": "string"
                                    },
                                    "city": {
                                        "type": "string"
                                    },
                                    "cost": {
                                        "type": "number"
                                    },
                                    "type": {
                                        "type": "string"
                                    },
                                    "brand": {
                                        "type": "string"
                                    },
                                    "model": {
                                        "type": "string"
                                    },
                                    "device": {
                                        "type": "string"
                                    },
                                    "pathId": {
                                        "type": "string"
                                    },
                                    "region": {
                                        "type": "string"
                                    },
                                    "browser": {
                                        "type": "string"
                                    },
                                    "clickId": {
                                        "type": "string"
                                    },
                                    "offerId": {
                                        "type": "string"
                                    },
                                    "revenue": {
                                        "type": "number"
                                    },
                                    "landerId": {
                                        "type": "string"
                                    },
                                    "referrer": {
                                        "type": "string"
                                    },
                                    "offerName": {
                                        "type": "string"
                                    },
                                    "osVersion": {
                                        "type": "string"
                                    },
                                    "userAgent": {
                                        "type": "string"
                                    },
                                    "campaignId": {
                                        "type": "string"
                                    },
                                    "deviceName": {
                                        "type": "string"
                                    },
                                    "externalId": {
                                        "type": "string"
                                    },
                                    "landerName": {
                                        "type": "string"
                                    },
                                    "costSources": {
                                        "type": "array"
                                    },
                                    "countryCode": {
                                        "type": "string"
                                    },
                                    "countryName": {
                                        "type": "string"
                                    },
                                    "subLanderId": {
                                        "type": "string"
                                    },
                                    "campaignName": {
                                        "type": "string"
                                    },
                                    "mobileCarrier": {
                                        "type": "string"
                                    },
                                    "transactionId": {
                                        "type": "string"
                                    },
                                    "browserVersion": {
                                        "type": "string"
                                    },
                                    "connectionType": {
                                        "type": "string"
                                    },
                                    "conversionType": {
                                        "type": "string"
                                    },
                                    "postbackParam1": {
                                        "type": "string"
                                    },
                                    "postbackParam2": {
                                        "type": "string"
                                    },
                                    "postbackParam3": {
                                        "type": "string"
                                    },
                                    "postbackParam4": {
                                        "type": "string"
                                    },
                                    "postbackParam5": {
                                        "type": "string"
                                    },
                                    "visitTimestamp": {
                                        "type": "string"
                                    },
                                    "customVariable1": {
                                        "type": "string"
                                    },
                                    "customVariable2": {
                                        "type": "string"
                                    },
                                    "customVariable3": {
                                        "type": "string"
                                    },
                                    "customVariable4": {
                                        "type": "string"
                                    },
                                    "customVariable5": {
                                        "type": "string"
                                    },
                                    "customVariable6": {
                                        "type": "string"
                                    },
                                    "customVariable7": {
                                        "type": "string"
                                    },
                                    "customVariable8": {
                                        "type": "string"
                                    },
                                    "customVariable9": {
                                        "type": "string"
                                    },
                                    "trafficSourceId": {
                                        "type": "string"
                                    },
                                    "conversionTypeId": {
                                        "type": "number"
                                    },
                                    "customVariable10": {
                                        "type": "string"
                                    },
                                    "postbackTimestamp": {
                                        "type": "string"
                                    },
                                    "trafficSourceName": {
                                        "type": "string"
                                    },
                                    "affiliateNetworkId": {
                                        "type": "string"
                                    },
                                    "connectionTypeName": {
                                        "type": "string"
                                    },
                                    "outgoingPostbackUrl": {
                                        "type": "string"
                                    },
                                    "affiliateNetworkName": {
                                        "type": "string"
                                    },
                                    "allConversionsRevenue": {
                                        "type": "number"
                                    },
                                    "revenueInOriginalCurrency": {
                                        "type": "number"
                                    },
                                    "conversionOriginalCurrency": {
                                        "type": "string"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh",
                                "incremental"
                            ],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": [
                                "postbackTimestamp"
                            ],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "clickId"
                                ],
                                [
                                    "conversionTypeId"
                                ],
                                [
                                    "postbackTimestamp"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": [
                                "postbackTimestamp"
                            ],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "clickId"
                                ],
                                [
                                    "conversionTypeId"
                                ],
                                [
                                    "postbackTimestamp"
                                ]
                            ],
                            "aliasName": "conversions",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "offers",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "id": {
                                        "type": "string"
                                    },
                                    "url": {
                                        "type": "string"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "tags": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "payout": {
                                        "type": "object",
                                        "properties": {
                                            "type": {
                                                "type": "string"
                                            },
                                            "value": {
                                                "type": "number"
                                            },
                                            "geoPayouts": {
                                                "type": "array"
                                            }
                                        }
                                    },
                                    "country": {
                                        "type": "object",
                                        "properties": {
                                            "code": {
                                                "type": "string"
                                            },
                                            "name": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "workspace": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "createdTime": {
                                        "type": "string"
                                    },
                                    "marketplace": {
                                        "type": "boolean"
                                    },
                                    "namePostfix": {
                                        "type": "string"
                                    },
                                    "updatedTime": {
                                        "type": "string"
                                    },
                                    "currencyCode": {
                                        "type": "string"
                                    },
                                    "allowedActions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "affiliateNetwork": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "capConfiguration": {
                                        "type": "object",
                                        "properties": {
                                            "limit": {
                                                "type": "number"
                                            },
                                            "capType": {
                                                "type": "string"
                                            },
                                            "enabled": {
                                                "type": "boolean"
                                            },
                                            "timeUnit": {
                                                "type": "string"
                                            },
                                            "timezone": {
                                                "type": "object",
                                                "properties": {
                                                    "code": {
                                                        "type": "string"
                                                    }
                                                }
                                            },
                                            "replacementOfferId": {
                                                "type": "object",
                                                "properties": {
                                                    "id": {
                                                        "type": "string"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "preferredTrackingDomain": {
                                        "type": "string"
                                    },
                                    "conversionTrackingMethod": {
                                        "type": "string"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "id"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "id"
                                ]
                            ],
                            "aliasName": "offers",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "campaigns",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "id": {
                                        "type": "string"
                                    },
                                    "url": {
                                        "type": "string"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "tags": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "basic": {
                                        "type": "boolean"
                                    },
                                    "country": {
                                        "type": "object",
                                        "properties": {
                                            "code": {
                                                "type": "string"
                                            },
                                            "name": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "costModel": {
                                        "type": "object",
                                        "properties": {
                                            "type": {
                                                "type": "string"
                                            },
                                            "value": {
                                                "type": "number"
                                            },
                                            "trafficLossRatio": {
                                                "type": "number"
                                            },
                                            "trafficLossEnabled": {
                                                "type": "boolean"
                                            },
                                            "customConversionCostConfiguration": {
                                                "type": "object",
                                                "properties": {
                                                    "customConversionCosts": {
                                                        "type": "array"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "workspace": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "createdTime": {
                                        "type": "string"
                                    },
                                    "namePostfix": {
                                        "type": "string"
                                    },
                                    "trafficType": {
                                        "type": "string"
                                    },
                                    "updatedTime": {
                                        "type": "string"
                                    },
                                    "revenueModel": {
                                        "type": "object",
                                        "properties": {
                                            "type": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "impressionUrl": {
                                        "type": "string"
                                    },
                                    "trafficSource": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "allowedActions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "directTracking": {
                                        "type": "boolean"
                                    },
                                    "redirectTarget": {
                                        "type": "object",
                                        "properties": {
                                            "flow": {
                                                "type": "object",
                                                "properties": {
                                                    "id": {
                                                        "type": "string"
                                                    }
                                                }
                                            },
                                            "inlineFlow": {
                                                "type": "object",
                                                "properties": {
                                                    "name": {
                                                        "type": "string"
                                                    },
                                                    "deleted": {
                                                        "type": "boolean"
                                                    },
                                                    "countries": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "object",
                                                            "properties": {
                                                                "code": {
                                                                    "type": "string"
                                                                },
                                                                "name": {
                                                                    "type": "string"
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "workspace": {
                                                        "type": "object",
                                                        "properties": {
                                                            "id": {
                                                                "type": "string"
                                                            }
                                                        }
                                                    },
                                                    "defaultPaths": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "object",
                                                            "properties": {
                                                                "id": {
                                                                    "type": "string"
                                                                },
                                                                "name": {
                                                                    "type": "string"
                                                                },
                                                                "active": {
                                                                    "type": "boolean"
                                                                },
                                                                "offers": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "type": "object",
                                                                        "properties": {
                                                                            "offer": {
                                                                                "type": "object",
                                                                                "properties": {
                                                                                    "id": {
                                                                                        "type": "string"
                                                                                    }
                                                                                }
                                                                            },
                                                                            "weight": {
                                                                                "type": "number"
                                                                            }
                                                                        }
                                                                    }
                                                                },
                                                                "weight": {
                                                                    "type": "number"
                                                                },
                                                                "landers": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "type": "object",
                                                                        "properties": {
                                                                            "lander": {
                                                                                "type": "object",
                                                                                "properties": {
                                                                                    "id": {
                                                                                        "type": "string"
                                                                                    }
                                                                                }
                                                                            },
                                                                            "weight": {
                                                                                "type": "number"
                                                                            },
                                                                            "sublanders": {
                                                                                "type": "array"
                                                                            }
                                                                        }
                                                                    }
                                                                },
                                                                "listicle": {
                                                                    "type": "boolean"
                                                                },
                                                                "autoOptimized": {
                                                                    "type": "boolean"
                                                                },
                                                                "smartRotation": {
                                                                    "type": "boolean"
                                                                },
                                                                "offerRedirectMode": {
                                                                    "type": "string"
                                                                },
                                                                "realtimeRoutingApiState": {
                                                                    "type": "string"
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "allowedActions": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "string"
                                                        }
                                                    },
                                                    "realtimeRoutingApi": {
                                                        "type": "string"
                                                    },
                                                    "conditionalPathsGroups": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "object",
                                                            "properties": {
                                                                "id": {
                                                                    "type": "string"
                                                                },
                                                                "name": {
                                                                    "type": "string"
                                                                },
                                                                "paths": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "type": "object",
                                                                        "properties": {
                                                                            "id": {
                                                                                "type": "string"
                                                                            },
                                                                            "name": {
                                                                                "type": "string"
                                                                            },
                                                                            "active": {
                                                                                "type": "boolean"
                                                                            },
                                                                            "offers": {
                                                                                "type": "array",
                                                                                "items": {
                                                                                    "type": "object",
                                                                                    "properties": {
                                                                                        "offer": {
                                                                                            "type": "object",
                                                                                            "properties": {
                                                                                                "id": {
                                                                                                    "type": "string"
                                                                                                }
                                                                                            }
                                                                                        },
                                                                                        "weight": {
                                                                                            "type": "number"
                                                                                        }
                                                                                    }
                                                                                }
                                                                            },
                                                                            "weight": {
                                                                                "type": "number"
                                                                            },
                                                                            "landers": {
                                                                                "type": "array",
                                                                                "items": {
                                                                                    "type": "object",
                                                                                    "properties": {
                                                                                        "lander": {
                                                                                            "type": "object",
                                                                                            "properties": {
                                                                                                "id": {
                                                                                                    "type": "string"
                                                                                                }
                                                                                            }
                                                                                        },
                                                                                        "weight": {
                                                                                            "type": "number"
                                                                                        },
                                                                                        "sublanders": {
                                                                                            "type": "array"
                                                                                        }
                                                                                    }
                                                                                }
                                                                            },
                                                                            "listicle": {
                                                                                "type": "boolean"
                                                                            },
                                                                            "autoOptimized": {
                                                                                "type": "boolean"
                                                                            },
                                                                            "smartRotation": {
                                                                                "type": "boolean"
                                                                            },
                                                                            "offerRedirectMode": {
                                                                                "type": "string"
                                                                            },
                                                                            "realtimeRoutingApiState": {
                                                                                "type": "string"
                                                                            }
                                                                        }
                                                                    }
                                                                },
                                                                "active": {
                                                                    "type": "boolean"
                                                                },
                                                                "conditions": {
                                                                    "type": "object",
                                                                    "properties": {
                                                                        "region": {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "names": {
                                                                                    "type": "array",
                                                                                    "items": {
                                                                                        "type": "string"
                                                                                    }
                                                                                },
                                                                                "predicate": {
                                                                                    "type": "string"
                                                                                }
                                                                            }
                                                                        },
                                                                        "country": {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "countries": {
                                                                                    "type": "array",
                                                                                    "items": {
                                                                                        "type": "object",
                                                                                        "properties": {
                                                                                            "code": {
                                                                                                "type": "string"
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                },
                                                                                "predicate": {
                                                                                    "type": "string"
                                                                                }
                                                                            }
                                                                        },
                                                                        "deviceType": {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "predicate": {
                                                                                    "type": "string"
                                                                                },
                                                                                "deviceTypes": {
                                                                                    "type": "array",
                                                                                    "items": {
                                                                                        "type": "string"
                                                                                    }
                                                                                }
                                                                            }
                                                                        },
                                                                        "operatingSystem": {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "predicate": {
                                                                                    "type": "string"
                                                                                },
                                                                                "operatingSystems": {
                                                                                    "type": "array",
                                                                                    "items": {
                                                                                        "type": "string"
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                },
                                                                "smartRotation": {
                                                                    "type": "boolean"
                                                                },
                                                                "automationConfiguration": {
                                                                    "type": "object",
                                                                    "properties": {
                                                                        "autoOptimized": {
                                                                            "type": "boolean"
                                                                        },
                                                                        "calculationMethod": {
                                                                            "type": "string"
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "automationConfiguration": {
                                                        "type": "object",
                                                        "properties": {
                                                            "autoOptimized": {
                                                                "type": "boolean"
                                                            },
                                                            "calculationMethod": {
                                                                "type": "string"
                                                            }
                                                        }
                                                    },
                                                    "defaultOfferRedirectMode": {
                                                        "type": "string"
                                                    },
                                                    "defaultPathsSmartRotation": {
                                                        "type": "boolean"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "directTrackingOfferId": {
                                        "type": "string"
                                    },
                                    "directTrackingLanderId": {
                                        "type": "string"
                                    },
                                    "preferredTrackingDomain": {
                                        "type": "string"
                                    },
                                    "customPostbacksConfiguration": {
                                        "type": "object",
                                        "properties": {
                                            "customConversionPostbacks": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "name": {
                                                            "type": "string"
                                                        },
                                                        "index": {
                                                            "type": "number"
                                                        },
                                                        "customPostbackUrl": {
                                                            "type": "string"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "id"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "id"
                                ]
                            ],
                            "aliasName": "campaigns",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "affiliate_networks",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "id": {
                                        "type": "string"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "workspace": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "createdTime": {
                                        "type": "string"
                                    },
                                    "postbackUrl": {
                                        "type": "string"
                                    },
                                    "updatedTime": {
                                        "type": "string"
                                    },
                                    "currencyCode": {
                                        "type": "string"
                                    },
                                    "allowedActions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "postbackPayoutToken": {
                                        "type": "string"
                                    },
                                    "postbackClickIdToken": {
                                        "type": "string"
                                    },
                                    "postbackEventTypeToken": {
                                        "type": "string"
                                    },
                                    "appendClickIdToOfferUrl": {
                                        "type": "boolean"
                                    },
                                    "preferredTrackingDomain": {
                                        "type": "string"
                                    },
                                    "conversionTrackingMethod": {
                                        "type": "string"
                                    },
                                    "offerUrlClickIdParameter": {
                                        "type": "string"
                                    },
                                    "duplicatedPostbackIsUpsell": {
                                        "type": "boolean"
                                    },
                                    "postbackTransactionIdToken": {
                                        "type": "string"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "id"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "id"
                                ]
                            ],
                            "aliasName": "affiliate_networks",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "landers",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "id": {
                                        "type": "string"
                                    },
                                    "url": {
                                        "type": "string"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "tags": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "country": {
                                        "type": "object",
                                        "properties": {
                                            "code": {
                                                "type": "string"
                                            },
                                            "name": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "workspace": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "landerType": {
                                        "type": "string"
                                    },
                                    "createdTime": {
                                        "type": "string"
                                    },
                                    "namePostfix": {
                                        "type": "string"
                                    },
                                    "updatedTime": {
                                        "type": "string"
                                    },
                                    "allowedActions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "numberOfOffers": {
                                        "type": "number"
                                    },
                                    "preferredTrackingDomain": {
                                        "type": "string"
                                    },
                                    "shouldHaveTrackingScript": {
                                        "type": "boolean"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "id"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "id"
                                ]
                            ],
                            "aliasName": "landers",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "flows",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "id": {
                                        "type": "string"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "countries": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "code": {
                                                    "type": "string"
                                                },
                                                "name": {
                                                    "type": "string"
                                                }
                                            }
                                        }
                                    },
                                    "workspace": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "createdTime": {
                                        "type": "string"
                                    },
                                    "updatedTime": {
                                        "type": "string"
                                    },
                                    "defaultPaths": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {
                                                    "type": "string"
                                                },
                                                "name": {
                                                    "type": "string"
                                                },
                                                "active": {
                                                    "type": "boolean"
                                                },
                                                "offers": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "offer": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "id": {
                                                                        "type": "string"
                                                                    }
                                                                }
                                                            },
                                                            "weight": {
                                                                "type": "number"
                                                            }
                                                        }
                                                    }
                                                },
                                                "weight": {
                                                    "type": "number"
                                                },
                                                "landers": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "lander": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "id": {
                                                                        "type": "string"
                                                                    }
                                                                }
                                                            },
                                                            "weight": {
                                                                "type": "number"
                                                            },
                                                            "sublanders": {
                                                                "type": "array"
                                                            }
                                                        }
                                                    }
                                                },
                                                "listicle": {
                                                    "type": "boolean"
                                                },
                                                "autoOptimized": {
                                                    "type": "boolean"
                                                },
                                                "smartRotation": {
                                                    "type": "boolean"
                                                },
                                                "offerRedirectMode": {
                                                    "type": "string"
                                                },
                                                "realtimeRoutingApiState": {
                                                    "type": "string"
                                                }
                                            }
                                        }
                                    },
                                    "allowedActions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "realtimeRoutingApi": {
                                        "type": "string"
                                    },
                                    "conditionalPathsGroups": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {
                                                    "type": "string"
                                                },
                                                "paths": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "id": {
                                                                "type": "string"
                                                            },
                                                            "name": {
                                                                "type": "string"
                                                            },
                                                            "active": {
                                                                "type": "boolean"
                                                            },
                                                            "offers": {
                                                                "type": "array",
                                                                "items": {
                                                                    "type": "object",
                                                                    "properties": {
                                                                        "offer": {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "id": {
                                                                                    "type": "string"
                                                                                }
                                                                            }
                                                                        },
                                                                        "weight": {
                                                                            "type": "number"
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            "weight": {
                                                                "type": "number"
                                                            },
                                                            "landers": {
                                                                "type": "array",
                                                                "items": {
                                                                    "type": "object",
                                                                    "properties": {
                                                                        "lander": {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "id": {
                                                                                    "type": "string"
                                                                                }
                                                                            }
                                                                        },
                                                                        "weight": {
                                                                            "type": "number"
                                                                        },
                                                                        "sublanders": {
                                                                            "type": "array"
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            "listicle": {
                                                                "type": "boolean"
                                                            },
                                                            "autoOptimized": {
                                                                "type": "boolean"
                                                            },
                                                            "smartRotation": {
                                                                "type": "boolean"
                                                            },
                                                            "offerRedirectMode": {
                                                                "type": "string"
                                                            },
                                                            "realtimeRoutingApiState": {
                                                                "type": "string"
                                                            }
                                                        }
                                                    }
                                                },
                                                "active": {
                                                    "type": "boolean"
                                                },
                                                "conditions": {
                                                    "type": "object",
                                                    "properties": {
                                                        "city": {
                                                            "type": "object",
                                                            "properties": {
                                                                "names": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "type": "string"
                                                                    }
                                                                },
                                                                "predicate": {
                                                                    "type": "string"
                                                                }
                                                            }
                                                        },
                                                        "region": {
                                                            "type": "object",
                                                            "properties": {
                                                                "names": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "type": "string"
                                                                    }
                                                                },
                                                                "predicate": {
                                                                    "type": "string"
                                                                }
                                                            }
                                                        },
                                                        "country": {
                                                            "type": "object",
                                                            "properties": {
                                                                "countries": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "type": "object",
                                                                        "properties": {
                                                                            "code": {
                                                                                "type": "string"
                                                                            }
                                                                        }
                                                                    }
                                                                },
                                                                "predicate": {
                                                                    "type": "string"
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "smartRotation": {
                                                    "type": "boolean"
                                                }
                                            }
                                        }
                                    },
                                    "defaultOfferRedirectMode": {
                                        "type": "string"
                                    },
                                    "defaultPathsSmartRotation": {
                                        "type": "boolean"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "id"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "id"
                                ]
                            ],
                            "aliasName": "flows",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "traffic_sources",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "id": {
                                        "type": "string"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "type": {
                                        "type": "string"
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "workspace": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "templateId": {
                                        "type": "string"
                                    },
                                    "createdTime": {
                                        "type": "string"
                                    },
                                    "externalIds": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "parameter": {
                                                    "type": "string"
                                                },
                                                "placeholder": {
                                                    "type": "string"
                                                },
                                                "excludeFromCampaignUrl": {
                                                    "type": "boolean"
                                                }
                                            }
                                        }
                                    },
                                    "postbackUrl": {
                                        "type": "string"
                                    },
                                    "updatedTime": {
                                        "type": "string"
                                    },
                                    "costVariable": {
                                        "type": "object",
                                        "properties": {
                                            "parameter": {
                                                "type": "string"
                                            },
                                            "placeholder": {
                                                "type": "string"
                                            },
                                            "excludeFromCampaignUrl": {
                                                "type": "boolean"
                                            }
                                        }
                                    },
                                    "currencyCode": {
                                        "type": "string"
                                    },
                                    "allowedActions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "directTracking": {
                                        "type": "boolean"
                                    },
                                    "predefinedType": {
                                        "type": "string"
                                    },
                                    "clickIdVariable": {
                                        "type": "object",
                                        "properties": {
                                            "parameter": {
                                                "type": "string"
                                            },
                                            "placeholder": {
                                                "type": "string"
                                            },
                                            "excludeFromCampaignUrl": {
                                                "type": "boolean"
                                            }
                                        }
                                    },
                                    "customVariables": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {
                                                    "type": "string"
                                                },
                                                "index": {
                                                    "type": "number"
                                                },
                                                "parameter": {
                                                    "type": "string"
                                                },
                                                "placeholder": {
                                                    "type": "string"
                                                },
                                                "trackedInReports": {
                                                    "type": "boolean"
                                                }
                                            }
                                        }
                                    },
                                    "pixelRedirectUrl": {
                                        "type": "string"
                                    },
                                    "impressionSpecific": {
                                        "type": "boolean"
                                    },
                                    "limitedGeoTracking": {
                                        "type": "boolean"
                                    },
                                    "skipSendingPostback": {
                                        "type": "boolean"
                                    },
                                    "customPostbacksConfiguration": {
                                        "type": "object",
                                        "properties": {
                                            "customConversionPostbacks": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "name": {
                                                            "type": "string"
                                                        },
                                                        "index": {
                                                            "type": "number"
                                                        },
                                                        "customPostbackUrl": {
                                                            "type": "string"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "id"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "id"
                                ]
                            ],
                            "aliasName": "traffic_sources",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "campaign_report",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ap": {
                                        "type": "number"
                                    },
                                    "cr": {
                                        "type": "number"
                                    },
                                    "cv": {
                                        "type": "number"
                                    },
                                    "CVR": {
                                        "type": "number"
                                    },
                                    "cpv": {
                                        "type": "number"
                                    },
                                    "ctr": {
                                        "type": "number"
                                    },
                                    "epc": {
                                        "type": "number"
                                    },
                                    "epv": {
                                        "type": "number"
                                    },
                                    "roi": {
                                        "type": "number"
                                    },
                                    "rpm": {
                                        "type": "number"
                                    },
                                    "cost": {
                                        "type": "number"
                                    },
                                    "date": {
                                        "type": "string"
                                    },
                                    "ecpa": {
                                        "type": "number"
                                    },
                                    "ecpc": {
                                        "type": "number"
                                    },
                                    "ecpm": {
                                        "type": "number"
                                    },
                                    "ictr": {
                                        "type": "number"
                                    },
                                    "mtti": {
                                        "type": "number"
                                    },
                                    "clicks": {
                                        "type": "number"
                                    },
                                    "errors": {
                                        "type": "number"
                                    },
                                    "payout": {
                                        "type": [
                                            "null",
                                            "number"
                                        ]
                                    },
                                    "profit": {
                                        "type": "number"
                                    },
                                    "visits": {
                                        "type": "number"
                                    },
                                    "actions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "created": {
                                        "type": "string"
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "offerId": {
                                        "type": "string"
                                    },
                                    "revenue": {
                                        "type": "number"
                                    },
                                    "offerUrl": {
                                        "type": "string"
                                    },
                                    "offerName": {
                                        "type": "string"
                                    },
                                    "conversions": {
                                        "type": "number"
                                    },
                                    "costSources": {
                                        "type": [
                                            "array",
                                            "null"
                                        ],
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "impressions": {
                                        "type": "number"
                                    },
                                    "offerCountry": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "uniqueClicks": {
                                        "type": "number"
                                    },
                                    "uniqueVisits": {
                                        "type": "number"
                                    },
                                    "customRevenue1": {
                                        "type": "number"
                                    },
                                    "customRevenue2": {
                                        "type": "number"
                                    },
                                    "customRevenue3": {
                                        "type": "number"
                                    },
                                    "customRevenue4": {
                                        "type": "number"
                                    },
                                    "customRevenue5": {
                                        "type": "number"
                                    },
                                    "customRevenue6": {
                                        "type": "number"
                                    },
                                    "customRevenue7": {
                                        "type": "number"
                                    },
                                    "customRevenue8": {
                                        "type": "number"
                                    },
                                    "customRevenue9": {
                                        "type": "number"
                                    },
                                    "customRevenue10": {
                                        "type": "number"
                                    },
                                    "customRevenue11": {
                                        "type": "number"
                                    },
                                    "offerWorkspaceId": {
                                        "type": "string"
                                    },
                                    "suspiciousClicks": {
                                        "type": "number"
                                    },
                                    "suspiciousVisits": {
                                        "type": "number"
                                    },
                                    "customConversions1": {
                                        "type": "number"
                                    },
                                    "customConversions2": {
                                        "type": "number"
                                    },
                                    "customConversions3": {
                                        "type": "number"
                                    },
                                    "customConversions4": {
                                        "type": "number"
                                    },
                                    "customConversions5": {
                                        "type": "number"
                                    },
                                    "customConversions6": {
                                        "type": "number"
                                    },
                                    "customConversions7": {
                                        "type": "number"
                                    },
                                    "customConversions8": {
                                        "type": "number"
                                    },
                                    "customConversions9": {
                                        "type": "number"
                                    },
                                    "offerWorkspaceName": {
                                        "type": "string"
                                    },
                                    "customConversions10": {
                                        "type": "number"
                                    },
                                    "customConversions11": {
                                        "type": "number"
                                    },
                                    "offerPayoutCurrency": {
                                        "type": "string"
                                    },
                                    "timeToInstallRange0": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange1": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange2": {
                                        "type": "number"
                                    },
                                    "suspiciousClicksPercentage": {
                                        "type": "number"
                                    },
                                    "suspiciousVisitsPercentage": {
                                        "type": "number"
                                    },
                                    "offerPayoutInOriginalCurrency": {
                                        "type": [
                                            "null",
                                            "number"
                                        ]
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh",
                                "incremental"
                            ],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": [
                                "date"
                            ],
                            "sourceDefinedPrimaryKey": []
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": [
                                "date"
                            ],
                            "destinationSyncMode": "append",
                            "primaryKey": [],
                            "aliasName": "campaign_report",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "offer_report",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ap": {
                                        "type": "number"
                                    },
                                    "cr": {
                                        "type": "number"
                                    },
                                    "cv": {
                                        "type": "number"
                                    },
                                    "CVR": {
                                        "type": "number"
                                    },
                                    "cpv": {
                                        "type": "number"
                                    },
                                    "ctr": {
                                        "type": "number"
                                    },
                                    "epc": {
                                        "type": "number"
                                    },
                                    "epv": {
                                        "type": "number"
                                    },
                                    "roi": {
                                        "type": "number"
                                    },
                                    "rpm": {
                                        "type": "number"
                                    },
                                    "cost": {
                                        "type": "number"
                                    },
                                    "date": {
                                        "type": "string"
                                    },
                                    "ecpa": {
                                        "type": "number"
                                    },
                                    "ecpc": {
                                        "type": "number"
                                    },
                                    "ecpm": {
                                        "type": "number"
                                    },
                                    "ictr": {
                                        "type": "number"
                                    },
                                    "mtti": {
                                        "type": "number"
                                    },
                                    "clicks": {
                                        "type": "number"
                                    },
                                    "errors": {
                                        "type": "number"
                                    },
                                    "payout": {
                                        "type": [
                                            "null",
                                            "number"
                                        ]
                                    },
                                    "profit": {
                                        "type": "number"
                                    },
                                    "visits": {
                                        "type": "number"
                                    },
                                    "actions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "created": {
                                        "type": "string"
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "offerId": {
                                        "type": "string"
                                    },
                                    "revenue": {
                                        "type": "number"
                                    },
                                    "offerUrl": {
                                        "type": "string"
                                    },
                                    "offerName": {
                                        "type": "string"
                                    },
                                    "conversions": {
                                        "type": "number"
                                    },
                                    "costSources": {
                                        "type": [
                                            "array",
                                            "null"
                                        ],
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "impressions": {
                                        "type": "number"
                                    },
                                    "offerCountry": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "uniqueClicks": {
                                        "type": "number"
                                    },
                                    "uniqueVisits": {
                                        "type": "number"
                                    },
                                    "customRevenue1": {
                                        "type": "number"
                                    },
                                    "customRevenue2": {
                                        "type": "number"
                                    },
                                    "customRevenue3": {
                                        "type": "number"
                                    },
                                    "customRevenue4": {
                                        "type": "number"
                                    },
                                    "customRevenue5": {
                                        "type": "number"
                                    },
                                    "customRevenue6": {
                                        "type": "number"
                                    },
                                    "customRevenue7": {
                                        "type": "number"
                                    },
                                    "customRevenue8": {
                                        "type": "number"
                                    },
                                    "customRevenue9": {
                                        "type": "number"
                                    },
                                    "customRevenue10": {
                                        "type": "number"
                                    },
                                    "customRevenue11": {
                                        "type": "number"
                                    },
                                    "offerWorkspaceId": {
                                        "type": "string"
                                    },
                                    "suspiciousClicks": {
                                        "type": "number"
                                    },
                                    "suspiciousVisits": {
                                        "type": "number"
                                    },
                                    "customConversions1": {
                                        "type": "number"
                                    },
                                    "customConversions2": {
                                        "type": "number"
                                    },
                                    "customConversions3": {
                                        "type": "number"
                                    },
                                    "customConversions4": {
                                        "type": "number"
                                    },
                                    "customConversions5": {
                                        "type": "number"
                                    },
                                    "customConversions6": {
                                        "type": "number"
                                    },
                                    "customConversions7": {
                                        "type": "number"
                                    },
                                    "customConversions8": {
                                        "type": "number"
                                    },
                                    "customConversions9": {
                                        "type": "number"
                                    },
                                    "offerWorkspaceName": {
                                        "type": "string"
                                    },
                                    "customConversions10": {
                                        "type": "number"
                                    },
                                    "customConversions11": {
                                        "type": "number"
                                    },
                                    "offerPayoutCurrency": {
                                        "type": "string"
                                    },
                                    "timeToInstallRange0": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange1": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange2": {
                                        "type": "number"
                                    },
                                    "suspiciousClicksPercentage": {
                                        "type": "number"
                                    },
                                    "suspiciousVisitsPercentage": {
                                        "type": "number"
                                    },
                                    "offerPayoutInOriginalCurrency": {
                                        "type": [
                                            "null",
                                            "number"
                                        ]
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh",
                                "incremental"
                            ],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": [
                                "date"
                            ],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "offerId"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": [
                                "date"
                            ],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "offerId"
                                ]
                            ],
                            "aliasName": "offer_report",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "lander_report",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ap": {
                                        "type": "number"
                                    },
                                    "cr": {
                                        "type": "number"
                                    },
                                    "cv": {
                                        "type": "number"
                                    },
                                    "CVR": {
                                        "type": "number"
                                    },
                                    "cpv": {
                                        "type": "number"
                                    },
                                    "ctr": {
                                        "type": "number"
                                    },
                                    "epc": {
                                        "type": "number"
                                    },
                                    "epv": {
                                        "type": "number"
                                    },
                                    "roi": {
                                        "type": "number"
                                    },
                                    "rpm": {
                                        "type": "number"
                                    },
                                    "cost": {
                                        "type": "number"
                                    },
                                    "date": {
                                        "type": "string"
                                    },
                                    "ecpa": {
                                        "type": "number"
                                    },
                                    "ecpc": {
                                        "type": "number"
                                    },
                                    "ecpm": {
                                        "type": "number"
                                    },
                                    "ictr": {
                                        "type": "number"
                                    },
                                    "mtti": {
                                        "type": "number"
                                    },
                                    "clicks": {
                                        "type": "number"
                                    },
                                    "errors": {
                                        "type": "number"
                                    },
                                    "profit": {
                                        "type": "number"
                                    },
                                    "visits": {
                                        "type": "number"
                                    },
                                    "actions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "created": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "revenue": {
                                        "type": "number"
                                    },
                                    "landerId": {
                                        "type": "string"
                                    },
                                    "readOnly": {
                                        "type": [
                                            "boolean",
                                            "null"
                                        ]
                                    },
                                    "landerUrl": {
                                        "type": "string"
                                    },
                                    "landerName": {
                                        "type": "string"
                                    },
                                    "conversions": {
                                        "type": "number"
                                    },
                                    "costSources": {
                                        "type": [
                                            "array",
                                            "null"
                                        ],
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "impressions": {
                                        "type": "number"
                                    },
                                    "uniqueClicks": {
                                        "type": "number"
                                    },
                                    "uniqueVisits": {
                                        "type": "number"
                                    },
                                    "landerCountry": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customRevenue1": {
                                        "type": "number"
                                    },
                                    "customRevenue2": {
                                        "type": "number"
                                    },
                                    "customRevenue3": {
                                        "type": "number"
                                    },
                                    "customRevenue4": {
                                        "type": "number"
                                    },
                                    "customRevenue5": {
                                        "type": "number"
                                    },
                                    "customRevenue6": {
                                        "type": "number"
                                    },
                                    "customRevenue7": {
                                        "type": "number"
                                    },
                                    "customRevenue8": {
                                        "type": "number"
                                    },
                                    "customRevenue9": {
                                        "type": "number"
                                    },
                                    "numberOfOffers": {
                                        "type": "number"
                                    },
                                    "customRevenue10": {
                                        "type": "number"
                                    },
                                    "customRevenue11": {
                                        "type": "number"
                                    },
                                    "suspiciousClicks": {
                                        "type": "number"
                                    },
                                    "suspiciousVisits": {
                                        "type": "number"
                                    },
                                    "landerWorkspaceId": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customConversions1": {
                                        "type": "number"
                                    },
                                    "customConversions2": {
                                        "type": "number"
                                    },
                                    "customConversions3": {
                                        "type": "number"
                                    },
                                    "customConversions4": {
                                        "type": "number"
                                    },
                                    "customConversions5": {
                                        "type": "number"
                                    },
                                    "customConversions6": {
                                        "type": "number"
                                    },
                                    "customConversions7": {
                                        "type": "number"
                                    },
                                    "customConversions8": {
                                        "type": "number"
                                    },
                                    "customConversions9": {
                                        "type": "number"
                                    },
                                    "customConversions10": {
                                        "type": "number"
                                    },
                                    "customConversions11": {
                                        "type": "number"
                                    },
                                    "landerWorkspaceName": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "timeToInstallRange0": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange1": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange2": {
                                        "type": "number"
                                    },
                                    "suspiciousClicksPercentage": {
                                        "type": "number"
                                    },
                                    "suspiciousVisitsPercentage": {
                                        "type": "number"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh",
                                "incremental"
                            ],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": [
                                "date"
                            ],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "landerId"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": [
                                "date"
                            ],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "landerId"
                                ]
                            ],
                            "aliasName": "lander_report",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "flow_report",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ap": {
                                        "type": "number"
                                    },
                                    "cr": {
                                        "type": "number"
                                    },
                                    "cv": {
                                        "type": "number"
                                    },
                                    "CVR": {
                                        "type": "number"
                                    },
                                    "cpv": {
                                        "type": "number"
                                    },
                                    "ctr": {
                                        "type": "number"
                                    },
                                    "epc": {
                                        "type": "number"
                                    },
                                    "epv": {
                                        "type": "number"
                                    },
                                    "roi": {
                                        "type": "number"
                                    },
                                    "rpm": {
                                        "type": "number"
                                    },
                                    "cost": {
                                        "type": "number"
                                    },
                                    "date": {
                                        "type": "string"
                                    },
                                    "ecpa": {
                                        "type": "number"
                                    },
                                    "ecpc": {
                                        "type": "number"
                                    },
                                    "ecpm": {
                                        "type": "number"
                                    },
                                    "ictr": {
                                        "type": "number"
                                    },
                                    "mtti": {
                                        "type": "number"
                                    },
                                    "clicks": {
                                        "type": "number"
                                    },
                                    "errors": {
                                        "type": "number"
                                    },
                                    "flowId": {
                                        "type": "string"
                                    },
                                    "profit": {
                                        "type": "number"
                                    },
                                    "visits": {
                                        "type": "number"
                                    },
                                    "actions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "revenue": {
                                        "type": "number"
                                    },
                                    "flowName": {
                                        "type": "string"
                                    },
                                    "readOnly": {
                                        "type": [
                                            "boolean",
                                            "null"
                                        ]
                                    },
                                    "conversions": {
                                        "type": "number"
                                    },
                                    "costSources": {
                                        "type": [
                                            "array",
                                            "null"
                                        ],
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "impressions": {
                                        "type": "number"
                                    },
                                    "uniqueClicks": {
                                        "type": "number"
                                    },
                                    "uniqueVisits": {
                                        "type": "number"
                                    },
                                    "customRevenue1": {
                                        "type": "number"
                                    },
                                    "customRevenue2": {
                                        "type": "number"
                                    },
                                    "customRevenue3": {
                                        "type": "number"
                                    },
                                    "customRevenue4": {
                                        "type": "number"
                                    },
                                    "customRevenue5": {
                                        "type": "number"
                                    },
                                    "customRevenue6": {
                                        "type": "number"
                                    },
                                    "customRevenue7": {
                                        "type": "number"
                                    },
                                    "customRevenue8": {
                                        "type": "number"
                                    },
                                    "customRevenue9": {
                                        "type": "number"
                                    },
                                    "customRevenue10": {
                                        "type": "number"
                                    },
                                    "customRevenue11": {
                                        "type": "number"
                                    },
                                    "suspiciousClicks": {
                                        "type": "number"
                                    },
                                    "suspiciousVisits": {
                                        "type": "number"
                                    },
                                    "flowWorkspaceName": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customConversions1": {
                                        "type": "number"
                                    },
                                    "customConversions2": {
                                        "type": "number"
                                    },
                                    "customConversions3": {
                                        "type": "number"
                                    },
                                    "customConversions4": {
                                        "type": "number"
                                    },
                                    "customConversions5": {
                                        "type": "number"
                                    },
                                    "customConversions6": {
                                        "type": "number"
                                    },
                                    "customConversions7": {
                                        "type": "number"
                                    },
                                    "customConversions8": {
                                        "type": "number"
                                    },
                                    "customConversions9": {
                                        "type": "number"
                                    },
                                    "customConversions10": {
                                        "type": "number"
                                    },
                                    "customConversions11": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange0": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange1": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange2": {
                                        "type": "number"
                                    },
                                    "suspiciousClicksPercentage": {
                                        "type": "number"
                                    },
                                    "suspiciousVisitsPercentage": {
                                        "type": "number"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh",
                                "incremental"
                            ],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": [
                                "date"
                            ],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "flowId"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": [
                                "date"
                            ],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "flowId"
                                ]
                            ],
                            "aliasName": "flow_report",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "traffic_source_report",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ap": {
                                        "type": "number"
                                    },
                                    "cr": {
                                        "type": "number"
                                    },
                                    "cv": {
                                        "type": "number"
                                    },
                                    "CVR": {
                                        "type": "number"
                                    },
                                    "cpv": {
                                        "type": "number"
                                    },
                                    "ctr": {
                                        "type": "number"
                                    },
                                    "epc": {
                                        "type": "number"
                                    },
                                    "epv": {
                                        "type": "number"
                                    },
                                    "roi": {
                                        "type": "number"
                                    },
                                    "rpm": {
                                        "type": "number"
                                    },
                                    "cost": {
                                        "type": "number"
                                    },
                                    "date": {
                                        "type": "string"
                                    },
                                    "ecpa": {
                                        "type": "number"
                                    },
                                    "ecpc": {
                                        "type": "number"
                                    },
                                    "ecpm": {
                                        "type": "number"
                                    },
                                    "ictr": {
                                        "type": "number"
                                    },
                                    "mtti": {
                                        "type": "number"
                                    },
                                    "clicks": {
                                        "type": "number"
                                    },
                                    "errors": {
                                        "type": "number"
                                    },
                                    "profit": {
                                        "type": "number"
                                    },
                                    "visits": {
                                        "type": "number"
                                    },
                                    "actions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "created": {
                                        "type": "string"
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "revenue": {
                                        "type": "number"
                                    },
                                    "readOnly": {
                                        "type": "boolean"
                                    },
                                    "conversions": {
                                        "type": "number"
                                    },
                                    "costSources": {
                                        "type": [
                                            "array",
                                            "null"
                                        ],
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "impressions": {
                                        "type": "number"
                                    },
                                    "postbackUrl": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "costArgument": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "uniqueClicks": {
                                        "type": "number"
                                    },
                                    "uniqueVisits": {
                                        "type": "number"
                                    },
                                    "customRevenue1": {
                                        "type": "number"
                                    },
                                    "customRevenue2": {
                                        "type": "number"
                                    },
                                    "customRevenue3": {
                                        "type": "number"
                                    },
                                    "customRevenue4": {
                                        "type": "number"
                                    },
                                    "customRevenue5": {
                                        "type": "number"
                                    },
                                    "customRevenue6": {
                                        "type": "number"
                                    },
                                    "customRevenue7": {
                                        "type": "number"
                                    },
                                    "customRevenue8": {
                                        "type": "number"
                                    },
                                    "customRevenue9": {
                                        "type": "number"
                                    },
                                    "clickIdArgument": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customRevenue10": {
                                        "type": "number"
                                    },
                                    "customRevenue11": {
                                        "type": "number"
                                    },
                                    "trafficSourceId": {
                                        "type": "string"
                                    },
                                    "suspiciousClicks": {
                                        "type": "number"
                                    },
                                    "suspiciousVisits": {
                                        "type": "number"
                                    },
                                    "trafficSourceName": {
                                        "type": "string"
                                    },
                                    "customConversions1": {
                                        "type": "number"
                                    },
                                    "customConversions2": {
                                        "type": "number"
                                    },
                                    "customConversions3": {
                                        "type": "number"
                                    },
                                    "customConversions4": {
                                        "type": "number"
                                    },
                                    "customConversions5": {
                                        "type": "number"
                                    },
                                    "customConversions6": {
                                        "type": "number"
                                    },
                                    "customConversions7": {
                                        "type": "number"
                                    },
                                    "customConversions8": {
                                        "type": "number"
                                    },
                                    "customConversions9": {
                                        "type": "number"
                                    },
                                    "customVariable1-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable2-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable3-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable4-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable5-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable6-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable7-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable8-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customVariable9-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "customConversions10": {
                                        "type": "number"
                                    },
                                    "customConversions11": {
                                        "type": "number"
                                    },
                                    "customVariable10-TS": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "timeToInstallRange0": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange1": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange2": {
                                        "type": "number"
                                    },
                                    "trafficSourceWorkspaceId": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "suspiciousClicksPercentage": {
                                        "type": "number"
                                    },
                                    "suspiciousVisitsPercentage": {
                                        "type": "number"
                                    },
                                    "trafficSourceWorkspaceName": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh",
                                "incremental"
                            ],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": [
                                "date"
                            ],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "trafficSourceId"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": [
                                "date"
                            ],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "trafficSourceId"
                                ]
                            ],
                            "aliasName": "traffic_source_report",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "affiliate_network_report",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "ap": {
                                        "type": "number"
                                    },
                                    "cr": {
                                        "type": "number"
                                    },
                                    "cv": {
                                        "type": "number"
                                    },
                                    "CVR": {
                                        "type": "number"
                                    },
                                    "cpv": {
                                        "type": "number"
                                    },
                                    "ctr": {
                                        "type": "number"
                                    },
                                    "epc": {
                                        "type": "number"
                                    },
                                    "epv": {
                                        "type": "number"
                                    },
                                    "roi": {
                                        "type": "number"
                                    },
                                    "rpm": {
                                        "type": "number"
                                    },
                                    "cost": {
                                        "type": "number"
                                    },
                                    "date": {
                                        "type": "string"
                                    },
                                    "ecpa": {
                                        "type": "number"
                                    },
                                    "ecpc": {
                                        "type": "number"
                                    },
                                    "ecpm": {
                                        "type": "number"
                                    },
                                    "ictr": {
                                        "type": "number"
                                    },
                                    "mtti": {
                                        "type": "number"
                                    },
                                    "clicks": {
                                        "type": "number"
                                    },
                                    "errors": {
                                        "type": "number"
                                    },
                                    "profit": {
                                        "type": "number"
                                    },
                                    "visits": {
                                        "type": "number"
                                    },
                                    "actions": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "created": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "deleted": {
                                        "type": "boolean"
                                    },
                                    "revenue": {
                                        "type": "number"
                                    },
                                    "conversions": {
                                        "type": "number"
                                    },
                                    "costSources": {
                                        "type": [
                                            "array",
                                            "null"
                                        ],
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "impressions": {
                                        "type": "number"
                                    },
                                    "uniqueClicks": {
                                        "type": "number"
                                    },
                                    "uniqueVisits": {
                                        "type": "number"
                                    },
                                    "customRevenue1": {
                                        "type": "number"
                                    },
                                    "customRevenue2": {
                                        "type": "number"
                                    },
                                    "customRevenue3": {
                                        "type": "number"
                                    },
                                    "customRevenue4": {
                                        "type": "number"
                                    },
                                    "customRevenue5": {
                                        "type": "number"
                                    },
                                    "customRevenue6": {
                                        "type": "number"
                                    },
                                    "customRevenue7": {
                                        "type": "number"
                                    },
                                    "customRevenue8": {
                                        "type": "number"
                                    },
                                    "customRevenue9": {
                                        "type": "number"
                                    },
                                    "customRevenue10": {
                                        "type": "number"
                                    },
                                    "customRevenue11": {
                                        "type": "number"
                                    },
                                    "suspiciousClicks": {
                                        "type": "number"
                                    },
                                    "suspiciousVisits": {
                                        "type": "number"
                                    },
                                    "affiliateNetworkId": {
                                        "type": "string"
                                    },
                                    "customConversions1": {
                                        "type": "number"
                                    },
                                    "customConversions2": {
                                        "type": "number"
                                    },
                                    "customConversions3": {
                                        "type": "number"
                                    },
                                    "customConversions4": {
                                        "type": "number"
                                    },
                                    "customConversions5": {
                                        "type": "number"
                                    },
                                    "customConversions6": {
                                        "type": "number"
                                    },
                                    "customConversions7": {
                                        "type": "number"
                                    },
                                    "customConversions8": {
                                        "type": "number"
                                    },
                                    "customConversions9": {
                                        "type": "number"
                                    },
                                    "customConversions10": {
                                        "type": "number"
                                    },
                                    "customConversions11": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange0": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange1": {
                                        "type": "number"
                                    },
                                    "timeToInstallRange2": {
                                        "type": "number"
                                    },
                                    "affiliateNetworkName": {
                                        "type": "string"
                                    },
                                    "appendClickIdToOfferUrl": {
                                        "type": "boolean"
                                    },
                                    "onlyWhitelistedPostbackIps": {
                                        "type": "boolean"
                                    },
                                    "suspiciousClicksPercentage": {
                                        "type": "number"
                                    },
                                    "suspiciousVisitsPercentage": {
                                        "type": "number"
                                    },
                                    "affiliateNetworkWorkspaceId": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    },
                                    "affiliateNetworkWorkspaceName": {
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh",
                                "incremental"
                            ],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": [
                                "date"
                            ],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "affiliateNetworkId"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": [
                                "date"
                            ],
                            "destinationSyncMode": "append",
                            "primaryKey": [
                                [
                                    "date"
                                ],
                                [
                                    "affiliateNetworkId"
                                ]
                            ],
                            "aliasName": "affiliate_network_report",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "workspaces",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/schema#",
                                "properties": {
                                    "id": {
                                        "type": "string"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "memberships": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "role": {
                                                    "type": "string"
                                                },
                                                "email": {
                                                    "type": "string"
                                                },
                                                "userId": {
                                                    "type": "string"
                                                },
                                                "created": {
                                                    "type": "string"
                                                },
                                                "lastName": {
                                                    "type": "string"
                                                },
                                                "firstName": {
                                                    "type": "string"
                                                },
                                                "workspaces": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "id": {
                                                                "type": "string"
                                                            },
                                                            "name": {
                                                                "type": "string"
                                                            }
                                                        }
                                                    }
                                                },
                                                "restrictedColumns": {
                                                    "type": "array"
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [
                                [
                                    "id"
                                ]
                            ]
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "overwrite",
                            "primaryKey": [
                                [
                                    "id"
                                ]
                            ],
                            "aliasName": "workspaces",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    }
                ]
            },

            "nonBreakingChangesPreference": "propagate_fully",
            "sourceId": source_id,
            "destinationId": destination_id,
            "workspaceId": workspace_id,
            "status": "active",
            "scheduleType": "manual",
            "dataResidency": "auto",
            "name": name,
            "namespaceDefinition": "customformat",
            "namespaceFormat": namespace_format
        }

        return payload

    def update_source_payload(self, name, source_id, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "client_id": Voluum.client_id,
                "access_key_id": Voluum.access_key_id,
                "access_key": Voluum.access_key,
                "start_date": self.start_date,
                "loopback_days": self.loopback_days,
                "affiliate_network_report_recovery_dates": self.affiliate_network_report_recovery_dates,
                "campaign_report_recovery_dates": self.campaign_report_recovery_dates,
                "conversions_recovery_dates": self.conversions_recovery_dates,
                "flow_report_recovery_dates": self.flow_report_recovery_dates,
                "lander_report_recovery_dates": self.lander_report_recovery_dates,
                "offer_report_recovery_dates": self.offer_report_recovery_dates,
                "traffic_source_report_recovery_dates": self.traffic_source_report_recovery_dates,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Voluum.source_name,
            "sourceDefinitionId": Voluum.source_definition_id,
            "sourceId": source_id,
            "workspaceId": airbyte_workspace_id
        }
