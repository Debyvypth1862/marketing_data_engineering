#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import requests
from freezegun import freeze_time
from source_google_analytics_data_api.authenticator import GoogleServiceKeyAuthenticator


@freeze_time("2023-01-01 00:00:00")
def test_token_rotation(requests_mock):
    credentials = {
        "client_email": "client_email",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA2example\nTEST_KEY_ONLY_NOT_REAL\n-----END PRIVATE KEY-----\n",
        "client_id": "client_id"
    }
    authenticator = GoogleServiceKeyAuthenticator(credentials)

    auth_request = requests_mock.register_uri(
        "POST",
        authenticator._google_oauth2_token_endpoint,
        json={"access_token": "bearer_token", "expires_in": 3600}
    )

    authenticated_request = authenticator(requests.Request())
    assert auth_request.call_count == 1
    assert auth_request.last_request.qs.get("grant_type") == ["urn:ietf:params:oauth:grant-type:jwt-bearer"]
    assert authenticator._token.get("expires_at") == 1672534800
    assert authenticated_request.headers.get("Authorization") == "Bearer bearer_token"
