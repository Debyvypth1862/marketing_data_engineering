from airflow.models import Variable


class IncomeAccess:
    source_name = Variable.get("incomeaccess_sourceName")
    source_definition_id = Variable.get("incomeaccess_source_definationid")

    def __init__(self, api_key, base_url, start_date, tracker_login_id, loopback_days, recovery_dates, 
                 is_recovery) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.start_date = start_date
        self.tracker_login_id = tracker_login_id
        self.loopback_days = loopback_days
        self.recovery_dates = recovery_dates
        self.is_recovery = is_recovery

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "api_key": self.api_key,
                "base_url": self.base_url,
                "start_date": self.start_date,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": self.loopback_days,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery,
            },
            "name": name,
            "sourceName": IncomeAccess.source_name,
            "sourceDefinitionId": IncomeAccess.source_definition_id,
            "workspaceId": airbyte_workspace_id,
        }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "user": user,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "account_report_stream",
                            "jsonSchema": {
                                "type": "object",
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "properties": {
                                    "date": {"type": ["null", "string"]},
                                    "tracker_login_id": {"type": ["null", "string"]},
                                    "row": {"type": ["null", "object"]},
                                    "reportname": {"type": ["null", "string"]},
                                    "reportstartdate": {"type": ["null", "string"]},
                                    "reportenddate": {"type": ["null", "string"]},
                                    "reportmerchantid": {"type": ["null", "string"]},
                                    "reportdisplayby": {"type": ["null", "string"]},
                                },
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": ["date"],
                            "sourceDefinedPrimaryKey": [["date"], ["tracker_login_id"]],
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["date"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["date"], ["tracker_login_id"]],
                            "aliasName": "account_report_stream",
                            "selected": True,
                            "fieldSelectionEnabled": False,
                        },
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
            "namespaceFormat": namespace_format,
        }
        return payload

    def update_source_payload(self, name, source_id, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "api_key": self.api_key,
                "base_url": self.base_url,
                "start_date": self.start_date,
                "tracker_login_id": self.tracker_login_id,
                "loopback_days": self.loopback_days,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery,
            },
            "name": name,
            "sourceName": IncomeAccess.source_name,
            "sourceDefinitionId": IncomeAccess.source_definition_id,
            "sourceId": source_id,
            "workspaceId": airbyte_workspace_id,
        }
