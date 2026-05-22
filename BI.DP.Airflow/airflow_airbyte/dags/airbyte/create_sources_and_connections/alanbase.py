from airflow.models import Variable


class Alanbase:
    source_name = Variable.get('alanbase_sourceName'),
    source_definition_id = Variable.get('alanbase_source_definationid')

    def __init__(self, base_url, date_from, api_key, tracker_login_id, loopback_days,
                 recovery_dates_common_statistic, recovery_dates_conversions,
                 is_recovery) -> None:
        self.base_url = base_url
        self.date_from = date_from
        self.api_key = api_key
        self.tracker_login_id = tracker_login_id
        self.loopback_days = loopback_days
        self.recovery_dates_common_statistic = recovery_dates_common_statistic
        self.recovery_dates_conversions = recovery_dates_conversions
        self.is_recovery = is_recovery

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "base_url": self.base_url,
                "date_from": self.date_from,
                "api_key": self.api_key,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": self.loopback_days,
                "recovery_dates_common_statistic": self.recovery_dates_common_statistic,
                "recovery_dates_conversions": self.recovery_dates_conversions,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Alanbase.source_name,
            "sourceDefinitionId": Alanbase.source_definition_id,
            "workspaceId": airbyte_workspace_id
        }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "user": user,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "common_statistic",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "properties": {
                                    "data": {
                                        "type": ["null", "object"]
                                    },
                                    "date": {
                                        "type": ["null", "string"]
                                    },
                                    "tracker_login_id": {
                                        "type": ["null", "string"]
                                    }
                                }
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": ["date"],
                            "sourceDefinedPrimaryKey": [["date"], ["tracker_login_id"]]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["date"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["date"], ["tracker_login_id"]],
                            "aliasName": "common_statistic",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "conversions",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "properties": {
                                    "data": {
                                        "type": ["null", "object"]
                                    },
                                    "date": {
                                        "type": ["null", "string"]
                                    },
                                    "tracker_login_id": {
                                        "type": ["null", "string"]
                                    }
                                }
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": ["date"],
                            "sourceDefinedPrimaryKey": [["date"], ["tracker_login_id"]]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["date"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["date"], ["tracker_login_id"]],
                            "aliasName": "conversions",
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
                "base_url": self.base_url,
                "date_from": self.date_from,
                "api_key": self.api_key,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": self.loopback_days,
                "recovery_dates_common_statistic": self.recovery_dates_common_statistic,
                "recovery_dates_conversions": self.recovery_dates_conversions,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Alanbase.source_name,
            "sourceDefinitionId": Alanbase.source_definition_id,
            "sourceId": source_id,
            "workspaceId": airbyte_workspace_id
        }
