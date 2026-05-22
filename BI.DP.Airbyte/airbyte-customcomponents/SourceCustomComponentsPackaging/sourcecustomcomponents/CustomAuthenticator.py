import logging
import pendulum
import requests
from dataclasses import dataclass, InitVar
from typing import Any, Mapping, MutableMapping, Union
from airbyte_cdk.sources.declarative.auth import DeclarativeOauth2Authenticator
from airbyte_cdk.sources.declarative.types import Config
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator, BasicHttpAuthenticator, HttpAuthenticator
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.models import FailureType
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException

logger = logging.getLogger('airbyte')


@dataclass
class CustomAuthenticator(DeclarativeAuthenticator):
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    basic_auth = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.basic_auth = BasicHttpAuthenticator(username=self.config['username'], password=self.config['password'])

    @property
    def auth_header(self) -> str:
        return "X-Auth-Token"

    @property
    def token(self) -> str:
        login_headers = self.basic_auth.get_auth_header()
        try:
            response = requests.get(self.config['base_url'] + '/rest/login', headers=login_headers)
            if response.status_code == 200:
                response_json = response.json()
                # logger.info(f'response from login url {response_json}')
                token = response_json['data']['value']
                try:
                    response = requests.put(self.config['base_url'] + '/rest/login/trackLogin',
                                            headers={"X-Auth-Token": token})
                    if response.status_code == 200:
                        # logger.info(token)
                        return token
                except:
                    return None

            elif response.status_code == 401:
                logger.info(f"Could not request token, {response.status_code} UnAuthorised")
                assert False
        except:
            raise ValueError("Invalid base URL or crendentials")
            assert False
            return None


@dataclass
class CustomAuthenticator_for_Myaffiliates(DeclarativeOauth2Authenticator):  # different
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    token_refresh_endpoint: Union[InterpolatedString, str]
    client_id: Union[InterpolatedString, str]
    client_secret: Union[InterpolatedString, str]
    oauth = None
    scopes: Union[InterpolatedString, str]
    grant_type: Union[InterpolatedString, str]
    refresh_access_token_headers: Union[Mapping[str, Any], None] = None
    refresh_access_token_authenticator: Union[HttpAuthenticator, None] = None
    _token_expiry_date = pendulum.now().subtract(days=1)
    access_token_name: Union[InterpolatedString, str]
    expires_in_name: Union[InterpolatedString, str]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        # logger.info(f'config passed is  -> {self.config}')
        self.token_refresh_endpoint = InterpolatedString.create(self.config['base_url'] + '/oauth/access_token',
                                                                parameters=parameters)
        self.access_token_name = InterpolatedString.create("access_token", parameters=parameters)
        self.expires_in_name = InterpolatedString.create("expires_in", parameters=parameters)
        self.grant_type = "client_credentials"
        self.scopes = "r_user_stats"
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.oauth = Oauth2Authenticator(client_id=self.client_id, client_secret=self.client_secret,
                                         scopes="r_user_stats",
                                         refresh_token=None,
                                         token_refresh_endpoint=self.token_refresh_endpoint,
                                         refresh_access_token_authenticator=self.refresh_access_token_authenticator,
                                         refresh_access_token_headers=self.refresh_access_token_headers)

    def build_refresh_request_body(self) -> Mapping[str, Any]:
        payload: MutableMapping[str, Any] = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scopes:
            payload["scope"] = self.scopes

        if self.grant_type:
            payload["grant_type"] = self.grant_type

        return payload

    def _get_refresh_access_token_response(self):
        try:
            response = requests.request(method="POST", url=self.get_token_refresh_endpoint(),
                                        data=self.build_refresh_request_body())
            self._log_response(response)
            if response.status_code != 200:
                raise ValueError("Invalid base URL or crendentials")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            try:
                if e.response.status_code == 429 or e.response.status_code >= 500:
                    raise DefaultBackoffException(request=e.response.request, response=e.response)
                if self._wrap_refresh_token_exception(e):
                    message = "Refresh token is invalid or expired. Please re-authenticate from Sources/<your source>/Settings."
                    raise AirbyteTracedException(internal_message=message, message=message,
                                                 failure_type=FailureType.config_error)
                raise
            except:
                raise ValueError('Invalid base URL or crendentials')
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e


@dataclass
class CustomAuthenticator_for_Buffalopartner(DeclarativeAuthenticator):  # diff
    config: Config
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        logger.info(f'Config passed is  ->   {self.config}')

    @property
    def auth_header(self) -> str:
        return "Authorization"

    @property
    def token(self) -> str:
        return ""


@dataclass
class CustomAuthenticator_for_Ego(DeclarativeAuthenticator):  # diff
    config: Config
    basic_auth = None

    @property
    def auth_header(self) -> str:
        return "Cookie"

    @property
    def token(self) -> str:
        token = None
        body = {
            "username": self.config["username"],
            "password": self.config["password"]
        }
        try:
            session = requests.Session()

            response = session.post(self.config["base_url"] + '/login.html', data=body)
        except:
            pass

        try:
            if response.status_code == 200:
                cookies_dict = session.cookies.get_dict()
                if "master_login" not in cookies_dict:
                    raise ValueError("Invalid base URL or bad credentials")
                token = "; ".join([str(x) + "=" + str(y) for x, y in cookies_dict.items()])
                print("token")
                print(token)
                return token
        except NameError:
            raise ValueError("Invalid base URL")