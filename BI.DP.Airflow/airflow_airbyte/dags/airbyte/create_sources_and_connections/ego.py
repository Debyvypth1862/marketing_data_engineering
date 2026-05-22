from airflow.models import Variable


class Ego:
    source_name = Variable.get('ego_sourceName')
    source_definition_id = Variable.get('ego_source_definationid')
    reports = Variable.get('ego_reports')

    def __init__(self, api_password, base_url, username, start_date, tracker_login_id, loopback_days, recovery_dates,
                 is_recovery) -> None:
        self.api_password = api_password
        self.base_url = base_url
        self.username = username
        self.start_date = start_date
        self.tracker_login_id = tracker_login_id
        self.loopback_days = loopback_days
        self.recovery_dates = recovery_dates
        self.is_recovery = is_recovery

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "password": self.api_password,
                "base_url": self.base_url,
                "username": self.username,
                "start_date": self.start_date,
                "tracker_login_id": self.tracker_login_id,
                "reports": Ego.reports,
                "loopback_days": self.loopback_days,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Ego.source_name,
            "sourceDefinitionId": Ego.source_definition_id,
            "workspaceId": airbyte_workspace_id
        }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "user": user,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "brand_report_stream",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "properties": {
                                    "data": {"type": ["null", "object"]},
                                    "date": {"type": ["null", "string"]},
                                    "report": {"type": ["null", "string"]},
                                    "tracker_login_id": {"type": ["null", "string"]}
                                }
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": ["date"],
                            "sourceDefinedPrimaryKey": [["date"], ["tracker_login_id"], ["report"]]
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["date"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["date"], ["tracker_login_id"], ["report"]],
                            "aliasName": "brand_report_stream",
                            "selected": True,
                            "fieldSelectionEnabled": False
                        }
                    }
                ]
            },
            "nonBreakingChangesPreference": "ignore",
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
                "password": self.api_password,
                "base_url": self.base_url,
                "username": self.username,
                "start_date": self.start_date,
                "tracker_login_id": self.tracker_login_id,
                "reports": Ego.reports,
                "loopback_days": self.loopback_days,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery
            },
            "name": name,
            "sourceName": Ego.source_name,
            "sourceDefinitionId": Ego.source_definition_id,
            "sourceId": source_id,
            "workspaceId": airbyte_workspace_id
        }
