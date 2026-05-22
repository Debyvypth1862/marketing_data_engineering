from airflow.models import Variable


class BuffaloPartner:
    source_name = Variable.get('buffalopartner_sourceName'),
    source_definition_id = Variable.get('buffalopartner_source_definationid')
    
    def __init__(self, username, apikey, start_date, end_date, base_url, loopback_days, tracker_login_id, 
                 recovery_dates, is_recovery) -> None:
        self.username = username
        self.apikey = apikey
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = base_url
        self.loopback_days = loopback_days
        self.tracker_login_id = tracker_login_id
        self.recovery_dates = recovery_dates
        self.is_recovery = is_recovery
    
    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "username": self.username,
                "apikey": self.apikey,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "base_url": self.base_url,
                "loopback_days": self.loopback_days,
                "tracker_login_id": self.tracker_login_id,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": BuffaloPartner.source_name,
            "sourceDefinitionId": BuffaloPartner.source_definition_id,
            "workspaceId": airbyte_workspace_id
        }
    
    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "rev_share_report_stream",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "properties": {
                                    "data": {"type": ["null", "object"]},
                                    "date": {"type": ["null", "string"]},
                                    "tracker_login_id": {"type": ["null", "string"]}
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
                            "aliasName": "rev_share_report_stream",
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
            "sourceUpdate": {
                "connectionConfiguration": {
                    "username": self.username,
                    "apikey": self.apikey,
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                    "base_url": self.base_url,
                    "loopback_days": self.loopback_days,
                    "tracker_login_id": self.tracker_login_id,
                    "recovery_dates": self.recovery_dates,
                    "is_recovery": self.is_recovery
                },
                "name": name,
                "sourceName": BuffaloPartner.source_name,
                "sourceDefinitionId": BuffaloPartner.source_definition_id,
                "sourceId": source_id,
                "workspaceId": airbyte_workspace_id
            }
        }
