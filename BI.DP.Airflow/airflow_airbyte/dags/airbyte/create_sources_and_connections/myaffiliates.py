from airflow.models import Variable


class MyAffiliates:
    source_name = Variable.get('myaffiliates_sourceName'),
    source_definition_id = Variable.get('myaffiliates_source_definationid')

    def __init__(self, base_url, client_id, start_date, client_secret, tracker_login_id, loopback_days, recovery_dates,
                 is_recovery) -> None:
        self.base_url = base_url
        self.client_id = client_id
        self.start_date = start_date
        self.client_secret = client_secret
        self.tracker_login_id = tracker_login_id
        self.loopback_days = loopback_days
        self.recovery_dates = recovery_dates
        self.is_recovery = is_recovery

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "base_url": self.base_url,
                "client_id": self.client_id,
                "start_date": self.start_date,
                "client_secret": self.client_secret,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": self.loopback_days,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": MyAffiliates.source_name,
            "sourceDefinitionId": MyAffiliates.source_definition_id,
            "workspaceId": airbyte_workspace_id
        }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "user": user,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "customer_report_stream",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "properties": {
                                    "date": {
                                        "type": ["null", "string"]
                                    },
                                    "tracker_login_id": {
                                        "type": ["null", "string"]
                                    },
                                    "Channel": {
                                        "type": ["null", "string"]
                                    },
                                    "Pay period": {
                                        "type": ["null", "string"]
                                    },
                                    "Customer group": {
                                        "type": ["null", "string"]
                                    },
                                    "Customer": {
                                        "type": ["null", "string"]
                                    },
                                    "Payload": {
                                        "type": ["null", "string"]
                                    },
                                    "Campaign group": {
                                        "type": ["null", "string"]
                                    },
                                    "Campaign": {
                                        "type": ["null", "string"]
                                    },
                                    "Media": {
                                        "type": ["null", "string"]
                                    },
                                    "Impressions": {
                                        "type": ["null", "string"]
                                    },
                                    "Clicks": {
                                        "type": ["null", "string"]
                                    },
                                    "Signups": {
                                        "type": ["null", "string"]
                                    },
                                    "FTD": {
                                        "type": ["null", "string"]
                                    },
                                    "Deposits": {
                                        "type": ["null", "string"]
                                    },
                                    "NDC": {
                                        "type": ["null", "string"]
                                    },
                                    "Bonuses": {
                                        "type": ["null", "string"]
                                    },
                                    "Qualified Players": {
                                        "type": ["null", "string"]
                                    },

                                    "Admin fee": {
                                        "type": ["null", "string"]
                                    },

                                    "Total Net Revenue": {
                                        "type": ["null", "string"]
                                    },

                                    "Income": {
                                        "type": ["null", "string"]
                                    }

                                }
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": ["date"],
                            "sourceDefinedPrimaryKey": [["date"], ["tracker_login_id"], ["Payload"]]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["date"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["date"], ["tracker_login_id"], ["Payload"]],
                            "aliasName": "customer_report_stream",
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

            "name": name,
            "namespaceDefinition": "customformat",
            "namespaceFormat": namespace_format
        }
        return payload

    def update_source_payload(self, name, source_id, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "base_url": self.base_url,
                "client_id": self.client_id,
                "start_date": self.start_date,
                "client_secret": self.client_secret,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": "2",
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": MyAffiliates.source_name,
            "sourceDefinitionId": MyAffiliates.source_definition_id,
            "sourceId": source_id,
            "workspaceId": airbyte_workspace_id
        }