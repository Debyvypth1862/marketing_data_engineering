class StreamStatePayload:
    def stream_state_payload(connection_id, stream_state):
        state = {
            "connectionId": connection_id,
            "connectionState": {
                "stateType": "stream",
                "streamState": []
            }
        }

        for d in stream_state:
            for stream, date in d.items():
                state["connectionState"]["streamState"].append(
                    {
                        "streamDescriptor": {
                            "name": stream
                        },
                        "streamState": {
                            "postbackTimestamp" if stream == "conversions" else "date": date
                        }
                    }
                )

        return state

    def redtrack_stream_state_payload(connection_id, stream_state):
        state = {
            "connectionId": connection_id,
            "connectionState": {
                "stateType": "stream",
                "streamState": []
            }
        }

        for d in stream_state:
            for stream, date in d.items():
                state["connectionState"]["streamState"].append(
                    {
                        "streamDescriptor": {
                            "name": stream
                        },
                        "streamState": {
                            "date_from": date
                        }
                    }
                )

        return state
    
    def q_stream_state_payload(connection_id, stream_state):
        state = {
            "connectionId": connection_id,
            "connectionState": {
                "stateType": "stream",
                "streamState": []
            }
        }

        for d in stream_state:
            for stream, date in d.items():
                state["connectionState"]["streamState"].append(
                    {
                        "streamDescriptor": {
                            "name": stream
                        },
                        "streamState": {
                            "end": date
                        }
                    }
                )

        return state


    def brc_stream_state_payload(connection_id, stream_state):
        state = {
            "connectionId": connection_id,
            "connectionState": {
                "stateType": "stream",
                "streamState": []
                }
            }

        for d in stream_state:
            for stream, date in d.items():
                if stream == "postback_tracking":
                    cursor_field = "post_click_timestamp"
                    cursor_record_count = 2
                elif stream == "postback_3rd_party_click_log":
                    cursor_field = "post_click_timestamp"
                    cursor_record_count = 1

                state["connectionState"]["streamState"].append(
                    {
                        "streamDescriptor": {
                            "name": stream,
                            "namespace": "whitelabel"
                        },
                        "streamState": {
                            "cursor": date,
                            "version": 2,
                            "state_type": "cursor_based",
                            "stream_name": stream,
                            "cursor_field": [
                                cursor_field
                            ],
                            "stream_namespace": "whitelabel",
                            "cursor_record_count": cursor_record_count
                        }
                    }
                )

        return state
    
    def brt_stream_state_payload(connection_id, stream_state):
        state = {
            "connectionId": connection_id,
            "connectionState": {
                "stateType": "stream",
                "streamState": []
                }
            }

        for d in stream_state:
            for stream, date in d.items():
                if stream == "offers":
                    cursor_field = "updated_at"
                    cursor_record_count = 2
                elif stream == "offers_history":
                    cursor_field = "updated_at"
                    cursor_record_count = 1

                state["connectionState"]["streamState"].append(
                    {
                        "streamDescriptor": {
                            "name": stream,
                            "namespace": "su_user_api"
                        },
                        "streamState": {
                            "cursor": date,
                            "version": 2,
                            "state_type": "cursor_based",
                            "stream_name": stream,
                            "cursor_field": [
                                cursor_field
                            ],
                            "stream_namespace": "su_user_api",
                            "cursor_record_count": cursor_record_count
                        }
                    }
                )

        return state
