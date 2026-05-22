from airflow.models import Variable


class GoogleAnalytics4: 
    source_name = Variable.get('ga4_sourceName')
    source_definition_id = Variable.get('ga4_source_definationid')
 
    def __init__(self, property_id, start_date, is_recovery, pages_recovery_dates, devices_recovery_dates,
                 locations_recovery_dates, traffic_sources_recovery_dates, website_overview_recovery_dates,
                 daily_active_users_recovery_dates, weekly_active_users_recovery_dates,
                 four_weekly_active_users_recovery_dates) -> None:
        self.property_id = property_id
        self.start_date = start_date
        self.is_recovery = is_recovery
        self.pages_recovery_dates=pages_recovery_dates
        self.devices_recovery_dates=devices_recovery_dates
        self.locations_recovery_dates=locations_recovery_dates
        self.traffic_sources_recovery_dates=traffic_sources_recovery_dates
        self.website_overview_recovery_dates=website_overview_recovery_dates
        self.daily_active_users_recovery_dates=daily_active_users_recovery_dates
        self.weekly_active_users_recovery_dates=weekly_active_users_recovery_dates
        self.four_weekly_active_users_recovery_dates=four_weekly_active_users_recovery_dates

    def create_source_payload(self, name, airbyte_workspace_id):
        return {
                "connectionConfiguration": {
                    "credentials": {
                    "auth_type": "Service",
                        "credentials_json": "<YOUR_GCP_SERVICE_ACCOUNT_JSON>"
                            },
                    "property_id": self.property_id,
                    "window_in_days": 1,
                    "date_ranges_start_date": self.start_date,
                    "is_recovery": self.is_recovery,
                    "pages_recovery_dates": self.pages_recovery_dates,
                    "devices_recovery_dates": self.devices_recovery_dates,
                    "locations_recovery_dates": self.locations_recovery_dates,
                    "traffic_sources_recovery_dates": self.traffic_sources_recovery_dates,
                    "website_overview_recovery_dates": self.website_overview_recovery_dates,
                    "daily_active_users_recovery_dates": self.daily_active_users_recovery_dates,
                    "weekly_active_users_recovery_dates": self.weekly_active_users_recovery_dates,
                    "four_weekly_active_users_recovery_dates": self.four_weekly_active_users_recovery_dates
                },
                "name": name,
                "sourceName": GoogleAnalytics4.source_name,
                "sourceDefinitionId": GoogleAnalytics4.source_definition_id,
                "workspaceId": airbyte_workspace_id
                }

    def create_connection_payload(name, namespace_format, source_id, destination_id, user, workspace_id):
        payload= {
                    "name": name ,
                    "namespaceDefinition": "customformat",
                    "namespaceFormat": namespace_format,
                    "sourceId": source_id,
                    "destinationId": destination_id,
                    "syncCatalog": {
                        "streams": [
                            {
                                "stream": {
                                    "name": "daily_active_users",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "active1DayUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of distinct active users on your site or app within a 1 day period. The 1 day period includes the last day in the report's date range. Note: this is the same as Active Users."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "daily_active_users",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            },
                            {
                                "stream": {
                                    "name": "weekly_active_users",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "active7DayUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of distinct active users on your site or app within a 7 day period. The 7 day period includes the last day in the report's date range."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "weekly_active_users",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            },
                            {
                                "stream": {
                                    "name": "four_weekly_active_users",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "active28DayUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of distinct active users on your site or app within a 28 day period. The 28 day period includes the last day in the report's date range."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "four_weekly_active_users",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            },
                            {
                                "stream": {
                                    "name": "devices",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "browser": {
                                                "type": "string",
                                                "description": "The browsers used to view your website."
                                            },
                                            "newUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of users who interacted with your site or launched your app for the first time (event triggered: first_open or first_visit)."
                                            },
                                            "sessions": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of sessions that began on your site or app (event triggered: session_start)."
                                            },
                                            "bounceRate": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The percentage of sessions that were not engaged ((Sessions Minus Engaged sessions) divided by Sessions). This metric is returned as a fraction; for example, 0.2761 means 27.61% of sessions were bounces."
                                            },
                                            "totalUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of distinct users who have logged at least one event, regardless of whether the site or app was in use when that event was logged."
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "deviceCategory": {
                                                "type": "string",
                                                "description": "The type of device: Desktop, Tablet, or Mobile."
                                            },
                                            "operatingSystem": {
                                                "type": "string",
                                                "description": "The operating systems used by visitors to your app or website. Includes desktop and mobile operating systems such as Windows and Android."
                                            },
                                            "screenPageViews": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed. Repeated views of a single page or screen are counted. (screen_view + page_view events)."
                                            },
                                            "sessionsPerUser": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average number of sessions per user (Sessions divided by Active Users)."
                                            },
                                            "averageSessionDuration": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average duration (in seconds) of users` sessions."
                                            },
                                            "screenPageViewsPerSession": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed per session. Repeated views of a single page or screen are counted. (screen_view + page_view events) / sessions."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "devices",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            },
                            {
                                "stream": {
                                    "name": "locations",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "city": {
                                                "type": "string",
                                                "description": "The city from which the user activity originated."
                                            },
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "region": {
                                                "type": "string",
                                                "description": "The geographic region from which the user activity originated, derived from their IP address."
                                            },
                                            "country": {
                                                "type": "string",
                                                "description": "The country from which the user activity originated."
                                            },
                                            "newUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of users who interacted with your site or launched your app for the first time (event triggered: first_open or first_visit)."
                                            },
                                            "sessions": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of sessions that began on your site or app (event triggered: session_start)."
                                            },
                                            "bounceRate": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The percentage of sessions that were not engaged ((Sessions Minus Engaged sessions) divided by Sessions). This metric is returned as a fraction; for example, 0.2761 means 27.61% of sessions were bounces."
                                            },
                                            "totalUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of distinct users who have logged at least one event, regardless of whether the site or app was in use when that event was logged."
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "screenPageViews": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed. Repeated views of a single page or screen are counted. (screen_view + page_view events)."
                                            },
                                            "sessionsPerUser": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average number of sessions per user (Sessions divided by Active Users)."
                                            },
                                            "averageSessionDuration": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average duration (in seconds) of users` sessions."
                                            },
                                            "screenPageViewsPerSession": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed per session. Repeated views of a single page or screen are counted. (screen_view + page_view events) / sessions."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "locations",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            },
                            {
                                "stream": {
                                    "name": "pages",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "hostName": {
                                                "type": "string",
                                                "description": "Includes the subdomain and domain names of a URL; for example, the Host Name of www.example.com/contact.html is www.example.com."
                                            },
                                            "bounceRate": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The percentage of sessions that were not engaged ((Sessions Minus Engaged sessions) divided by Sessions). This metric is returned as a fraction; for example, 0.2761 means 27.61% of sessions were bounces."
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "screenPageViews": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed. Repeated views of a single page or screen are counted. (screen_view + page_view events)."
                                            },
                                            "pagePathPlusQueryString": {
                                                "type": "string",
                                                "description": "The portion of the URL following the hostname for web pages visited; for example, the `pagePathPlusQueryString` portion of `https://www.example.com/store/contact-us?query_string=true` is `/store/contact-us?query_string=true`."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "pages",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            },
                            {
                                "stream": {
                                    "name": "traffic_sources",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "newUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of users who interacted with your site or launched your app for the first time (event triggered: first_open or first_visit)."
                                            },
                                            "sessions": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of sessions that began on your site or app (event triggered: session_start)."
                                            },
                                            "bounceRate": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The percentage of sessions that were not engaged ((Sessions Minus Engaged sessions) divided by Sessions). This metric is returned as a fraction; for example, 0.2761 means 27.61% of sessions were bounces."
                                            },
                                            "totalUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of distinct users who have logged at least one event, regardless of whether the site or app was in use when that event was logged."
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "sessionMedium": {
                                                "type": "string",
                                                "description": "The medium that initiated a session on your website or app."
                                            },
                                            "sessionSource": {
                                                "type": "string",
                                                "description": "The source that initiated a session on your website or app."
                                            },
                                            "screenPageViews": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed. Repeated views of a single page or screen are counted. (screen_view + page_view events)."
                                            },
                                            "sessionsPerUser": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average number of sessions per user (Sessions divided by Active Users)."
                                            },
                                            "averageSessionDuration": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average duration (in seconds) of users` sessions."
                                            },
                                            "screenPageViewsPerSession": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed per session. Repeated views of a single page or screen are counted. (screen_view + page_view events) / sessions."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "traffic_sources",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            },
                            {
                                "stream": {
                                    "name": "website_overview",
                                    "jsonSchema": {
                                        "type": [
                                            "null",
                                            "object"
                                        ],
                                        "$schema": "https://json-schema.org/draft-07/schema#",
                                        "properties": {
                                            "date": {
                                                "type": "string",
                                                "description": "The date of the event, formatted as YYYYMMDD."
                                            },
                                            "uuid": {
                                                "type": [
                                                    "string"
                                                ],
                                                "description": "Custom unique identifier for each record, to support primary key"
                                            },
                                            "newUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of users who interacted with your site or launched your app for the first time (event triggered: first_open or first_visit)."
                                            },
                                            "sessions": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of sessions that began on your site or app (event triggered: session_start)."
                                            },
                                            "bounceRate": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The percentage of sessions that were not engaged ((Sessions Minus Engaged sessions) divided by Sessions). This metric is returned as a fraction; for example, 0.2761 means 27.61% of sessions were bounces."
                                            },
                                            "totalUsers": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of distinct users who have logged at least one event, regardless of whether the site or app was in use when that event was logged."
                                            },
                                            "property_id": {
                                                "type": [
                                                    "string"
                                                ]
                                            },
                                            "screenPageViews": {
                                                "type": [
                                                    "null",
                                                    "integer"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed. Repeated views of a single page or screen are counted. (screen_view + page_view events)."
                                            },
                                            "sessionsPerUser": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average number of sessions per user (Sessions divided by Active Users)."
                                            },
                                            "averageSessionDuration": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The average duration (in seconds) of users` sessions."
                                            },
                                            "screenPageViewsPerSession": {
                                                "type": [
                                                    "null",
                                                    "number"
                                                ],
                                                "description": "The number of app screens or web pages your users viewed per session. Repeated views of a single page or screen are counted. (screen_view + page_view events) / sessions."
                                            }
                                        },
                                        "additionalProperties": True
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
                                            "uuid"
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
                                            "uuid"
                                        ]
                                    ],
                                    "aliasName": "website_overview",
                                    "selected": True,
                                    "fieldSelectionEnabled": False
                                }
                            }
                        ]
                    },
                    "scheduleType": "manual",
                    "status": "active",
                    "geography": "auto",
                    "breakingChange": False,
                    "notifySchemaChanges": False,
                    "notifySchemaChangesByEmail": False,
                    "nonBreakingChangesPreference": "ignore"
                }

    def update_source_payload(self, name, source_id, airbyte_workspace_id):
        return {
                "connectionConfiguration": {
                    "credentials": {
                    "auth_type": "Service",
                        "credentials_json": "<YOUR_GCP_SERVICE_ACCOUNT_JSON>"
                            },
                    "property_id": self.property_id,
                    "window_in_days": 1,
                    "date_ranges_start_date": self.start_date,
                    "is_recovery": self.is_recovery,
                    "pages_recovery_dates": self.pages_recovery_dates,
                    "devices_recovery_dates": self.devices_recovery_dates,
                    "locations_recovery_dates": self.locations_recovery_dates,
                    "traffic_sources_recovery_dates": self.traffic_sources_recovery_dates,
                    "website_overview_recovery_dates": self.website_overview_recovery_dates,
                    "daily_active_users_recovery_dates": self.daily_active_users_recovery_dates,
                    "weekly_active_users_recovery_dates": self.weekly_active_users_recovery_dates,
                    "four_weekly_active_users_recovery_dates": self.four_weekly_active_users_recovery_dates
                },
                "name": name,
                "sourceName": GoogleAnalytics4.source_name,
                "sourceDefinitionId": GoogleAnalytics4.source_definition_id,
                "sourceId": source_id,
                "workspaceId": airbyte_workspace_id
        }
