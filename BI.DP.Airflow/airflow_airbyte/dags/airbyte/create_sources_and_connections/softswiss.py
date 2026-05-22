from airflow.models import Variable


class Softswiss:
    source_name = (Variable.get("softswiss_sourceName"),)
    source_definition_id = Variable.get("softswiss_source_definationid")

    def __init__( self, base_url, statistic_token, start_date, exchange_rates_date, tracker_login_id, step, 
                  loopback_days, dynamic_tag, recovery_dates, is_recovery) -> None:
        self.base_url = base_url
        self.statistic_token = statistic_token
        self.start_date = start_date
        self.exchange_rates_date = exchange_rates_date
        self.tracker_login_id = tracker_login_id
        self.step = step
        self.loopback_days = loopback_days
        self.dynamic_tag = dynamic_tag
        self.recovery_dates = recovery_dates
        self.is_recovery = is_recovery

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
            "connectionConfiguration": {
                "base_url": self.base_url,
                "statistic_token": self.statistic_token,
                "exchange_rates_date": self.exchange_rates_date,
                "start_date": self.start_date,
                "tracker_login_id": self.tracker_login_id,
                "step": self.step,
                "loopback_days": self.loopback_days,
                "dynamic_tag": self.dynamic_tag,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery,
            },
            "name": name,
            "sourceName": Softswiss.source_name,
            "sourceDefinitionId": Softswiss.source_definition_id,
            "workspaceId": airbyte_workspace_id,
        }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload = {
            "user": user,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "activity_report",
                            "jsonSchema": {
                                "type": "object",
                                "title": "Generated schema for Root",
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "required": [
                                    "report_type",
                                    "rows",
                                    "totals",
                                    "relations",
                                    "current_page",
                                    "total_pages",
                                    "current_per_page",
                                    "total_count",
                                ],
                                "properties": {
                                    "date": {"type": "string"},
                                    "rows": {
                                        "type": "object",
                                        "required": ["data"],
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "required": [
                                                            "name",
                                                            "value",
                                                            "type",
                                                        ],
                                                        "properties": {
                                                            "name": {"type": "string"},
                                                            "type": {"type": "string"},
                                                            "value": {},
                                                        },
                                                    },
                                                },
                                            }
                                        },
                                    },
                                    "totals": {
                                        "type": "object",
                                        "required": ["data"],
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "required": [
                                                            "name",
                                                            "value",
                                                            "type",
                                                        ],
                                                        "properties": {
                                                            "name": {"type": "string"},
                                                            "type": {"type": "string"},
                                                            "value": {},
                                                        },
                                                    },
                                                },
                                            }
                                        },
                                    },
                                    "end_date": {"type": "string"},
                                    "relations": {
                                        "type": "object",
                                        "required": [
                                            "campaigns",
                                            "promos",
                                            "brands",
                                            "partners",
                                            "dedicated_operators",
                                        ],
                                        "properties": {
                                            "brands": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "required": ["id", "name"],
                                                    "properties": {
                                                        "id": {"type": "number"},
                                                        "name": {"type": "string"},
                                                    },
                                                },
                                            },
                                            "promos": {"type": "array", "items": {}},
                                            "partners": {"type": "array", "items": {}},
                                            "campaigns": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "required": [
                                                        "id",
                                                        "name",
                                                        "strategy",
                                                    ],
                                                    "properties": {
                                                        "id": {"type": "number"},
                                                        "name": {"type": "string"},
                                                        "strategy": {"type": "string"},
                                                    },
                                                },
                                            },
                                            "dedicated_operators": {
                                                "type": "array",
                                                "items": {},
                                            },
                                        },
                                    },
                                    "start_date": {"type": "string"},
                                    "report_type": {"type": "string"},
                                    "total_count": {"type": "number"},
                                    "total_pages": {"type": "number"},
                                    "current_page": {"type": "number"},
                                    "current_per_page": {"type": "number"},
                                    "tracker_login_id": {"type": "number"},
                                },
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": ["date"],
                            "sourceDefinedPrimaryKey": [["tracker_login_id"], ["date"]],
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["date"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["tracker_login_id"], ["date"]],
                            "aliasName": "activity_report",
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
                "base_url": self.base_url,
                "statistic_token": self.statistic_token,
                "exchange_rates_date": self.exchange_rates_date,
                "start_date": self.start_date,
                "tracker_login_id": self.tracker_login_id,
                "step": self.step,
                "loopback_days": self.loopback_days,
                "dynamic_tag": self.dynamic_tag,
                "recovery_dates": self.recovery_dates,
                "is_recovery": self.is_recovery,
            },
            "name": name,
            "sourceName": Softswiss.source_name,
            "sourceDefinitionId": Softswiss.source_definition_id,
            "sourceId": source_id,
            "workspaceId": airbyte_workspace_id,
        }
