from airflow.models import Variable


class Sweep:
    source_name = Variable.get('sweep_sourceName'),
    source_definition_id = Variable.get('sweep_source_definationid')

    def __init__(self, base_url, from_date, api_password, api_username, tracker_login_id, loopback_days,
                 recovery_dates_dynamic_variables_report_stream, recovery_dates_registration_report_stream, recovery_dates_ftd_registration_report_stream,
                 is_recovery) -> None:
        self.base_url = base_url
        self.fromdate = from_date
        self.api_password = api_password
        self.api_username = api_username
        self.tracker_login_id = tracker_login_id
        self.loopback_days = loopback_days
        self.recovery_dates_dynamic_variables_report_stream = recovery_dates_dynamic_variables_report_stream
        self.recovery_dates_registration_report_stream = recovery_dates_registration_report_stream
        self.recovery_dates_ftd_registration_report_stream = recovery_dates_ftd_registration_report_stream
        self.is_recovery = is_recovery

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "base_url": self.base_url,
                "fromdate": self.fromdate,
                "api_password": self.api_password,
                "api_username": self.api_username,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": self.loopback_days,
                "recovery_dates_dynamic_variables_report_stream": self.recovery_dates_dynamic_variables_report_stream,
                "recovery_dates_registration_report_stream": self.recovery_dates_registration_report_stream,
                "recovery_dates_ftd_registration_report_stream": self.recovery_dates_ftd_registration_report_stream, 
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Sweep.source_name,
            "sourceDefinitionId": Sweep.source_definition_id,
            "workspaceId": airbyte_workspace_id
        }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "user": user,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "dynamic_variables_report_stream",
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
                            "aliasName": "dynamic_variables_report_stream",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "registration_report_stream",
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
                            "aliasName": "registration_report_stream",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    },
                    {
                        "stream": {
                            "name": "ftd_registration_report_stream",
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
                            "aliasName": "ftd_registration_report_stream",
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
                "fromdate": self.fromdate,
                "api_password": self.api_password,
                "api_username": self.api_username,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": self.loopback_days,
                "recovery_dates_dynamic_variables_report_stream": self.recovery_dates_dynamic_variables_report_stream,
                "recovery_dates_registration_report_stream": self.recovery_dates_registration_report_stream,
                "recovery_dates_ftd_registration_report_stream": self.recovery_dates_ftd_registration_report_stream,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Sweep.source_name,
            "sourceDefinitionId": Sweep.source_definition_id,
            "sourceId": source_id,
            "workspaceId": airbyte_workspace_id
        }
