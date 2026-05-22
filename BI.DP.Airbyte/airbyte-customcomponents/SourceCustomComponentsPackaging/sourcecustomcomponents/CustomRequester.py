import json
import logging
import os
from dataclasses import dataclass, InitVar
from typing import Any, Mapping, Optional, Union
from airbyte_cdk.sources.declarative.types import StreamSlice, StreamState
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from typing import Any, Mapping, Optional, List, Union, MutableMapping
from airbyte_cdk.sources.declarative.types import Config, StreamSlice, StreamState
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.requesters.requester import HttpMethod
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.requesters.error_handlers.default_error_handler import DefaultErrorHandler

from sourcecustomcomponents.CustomErrorHandlers import CustomErrorHandler, CustomErrorHandler_for_Q, \
    CustomErrorHandler_for_Smartico, CustomErrorHandler_for_Softswiss, CustomErrorHandler_for_Netrefer, \
    CustomErrorHandler_for_Income_access, CustomErrorHandler_for_cellxpert
from jinja2 import Template

logger = logging.getLogger('airbyte')


@dataclass
class CustomRequester_for_mexos(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_body_json(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping]:
        # logger.info(f'inside request body.......')
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        f = open(os.path.join(__location__, 'body.json'))
        body = json.load(f)
        body['report']['campaignId'] = self.config['campaign_id']
        body['report']['id'] = self.config['id']
        body['report']['input'][0]['sublist'][1]['value'] = stream_slice['start_time']
        body['report']['input'][0]['sublist'][2]['value'] = stream_slice['start_time']
        body['report']['input'][5]['value'] = self.config['variable']
        f.close()
        return body

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format
            return {"host": Host}
        # else:
        #     return

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base


@dataclass
class CustomRequester_for_Ego(HttpRequester):  # diff
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_body_data(  # type: ignore
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        body = {
            "to_date": stream_slice["start_time"],
            "from_date": stream_slice["end_time"],
            "reports_table": stream_slice["report"],
            "limit_to_affiliate_name": "",
            "limit_to_affiliate_zone_data": "",
            "sortfield": "entry_date",
            "sortorder": "asc",
            "Submit2": "",
            "show_report": 1
        }
        return body

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            headers = {
                'Host': Host
            }
            return headers

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base


@dataclass
class CustomRequester_for_smartico(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler_for_Smartico
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        date_from = stream_slice['start_time']
        date_to = stream_slice['end_time']

        return {
            "aggregation_period": "DAY",
            "group_by": "utm_campaign,utm_medium,utm_source,afp",
            "date_from": date_from,
            "date_to": date_to
        }

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        Authorization = self.config.get('token')
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()

            # Return the headers in the required format
            return {"host": Host,
                    "Authorization": Authorization}
        else:
            return {
                "Authorization": Authorization}


@dataclass
class CustomRequester_for_softswiss(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler_for_Softswiss
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        statistic_token = self.config.get('statistic_token')
        Authorization = f"statistictoken {statistic_token}"
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            statistic_token = self.config.get('statistic_token')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format

            return {"host": Host,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": Authorization}
        else:
            return {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": Authorization}


@dataclass
class CustomRequester_for_q(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler_for_Q
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':

            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        token = self.config.get('token')
        start = stream_slice['start']
        end = stream_slice['end']
        merchant = stream_slice['merchant']

        return {
            "start": start,
            "end": end,
            "merchant": merchant,
            "token": token
        }

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format
            return {"host": Host}
        # else:
        #     return


@dataclass
class CustomRequester_for_buffalo_partner(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format
            return {"host": Host}
        # else:
        #     return

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        username = self.config.get('username')
        apikey = self.config.get('apikey')

        start = stream_slice['start_time']

        return {
            "username": username,
            "apikey": apikey,
            "start": start,
            "end": start
        }

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base


@dataclass
class CustomRequester_cellxpert_activityreport(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler_for_cellxpert
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            headers = {
                'Host': Host,
            }
            return headers

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        api_username = self.config.get('api_username')
        api_password = self.config.get('api_password')
        tracker_login_id = self.config.get('tracker_login_id')

        fromdate = stream_slice['start_time']

        return {
            "api_username": api_username,
            "api_password": api_password,
            "fromdate": fromdate,
            "todate": fromdate,
            "DateFormat": "day",
            "daterange": "dd",
            "command": "activityreport",
            "json": "1",
            "tracker_login_id": tracker_login_id
        }


@dataclass
class CustomRequester_cellxpert_registrations(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler_for_cellxpert
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            headers = {
                'Host': Host,
            }
            return headers

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        api_username = self.config.get('api_username')
        api_password = self.config.get('api_password')
        tracker_login_id = self.config.get('tracker_login_id')

        fromdate = stream_slice['start_time']

        return {
            "api_username": api_username,
            "api_password": api_password,
            "fromdate": fromdate,
            "todate": fromdate,
            "DateFormat": "day",
            "daterange": "dd",
            "command": "registrations",
            "json": "1",
            "tracker_login_id": tracker_login_id
        }

@dataclass
class CustomRequester_cellxpert_ftd_registrations(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler_for_cellxpert
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            headers = {
                'Host': Host,
            }
            return headers

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        api_username = self.config.get('api_username')
        api_password = self.config.get('api_password')
        tracker_login_id = self.config.get('tracker_login_id')

        fromdate = stream_slice['start_time']

        return {
            "api_username": api_username,
            "api_password": api_password,
            "fromdate": fromdate,
            "todate": fromdate,
            "DateFormat": "day",
            "daterange": "fdd",
            "command": "registrations",
            "json": "1",
            "tracker_login_id": tracker_login_id
        }


@dataclass
class CustomRequester_for_income_access(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        date_from = stream_slice['start_time']
        date_to = stream_slice['end_time']

        return {
            "aggregation_period": "DAY",
            "group_by": "utm_campaign,utm_medium,utm_source,afp",
            "date_from": date_from,
            "date_to": date_to
        }

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format
            return {"host": Host}
        # else:
        #     return

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        api_key = self.config.get('api_key')
        reportstartdate = stream_slice['start_time']
        return {
            "key": api_key,
            "reportname": "AccountReport",
            "reportformat": "xml",
            "reportmerchantid": "0",
            "reportstartdate": reportstartdate,
            "reportenddate": reportstartdate
        }


@dataclass
class CustomRequester_for_myaffiliates(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format
            return {"host": Host}
        # else:
        #     return

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base


@dataclass
class CustomRequester_for_netrefer(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: DefaultErrorHandler = CustomErrorHandler_for_Netrefer
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        Authorization = self.config.get('api_key')
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format
            return {"host": Host,
                    "Authorization": Authorization}
        else:
            return {
                "Authorization": Authorization}

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        start_time = stream_slice['start_time']
        return {
            "dateFrom": start_time,
            "dateTo": start_time,
            "mediaID": "all",
            "marketingSourceID": "all"
        } 
            
@dataclass
class CustomRequester_for_referon(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[DefaultErrorHandler] = CustomErrorHandler
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.GET

    def __hash__(self):
        return hash(tuple(self.__dict__))

    def get_request_headers(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            base_url = self.config.get('base_url')
            logger.info(f'base_url-----{base_url}')
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """

            # Create a template object
            template = Template(template_str)

            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            # Return the headers in the required format
            return {"host": Host}
        else:
            return {}

    def get_url_base(self) -> str:
        APISIX_ENABLED = os.environ['APISIX_ENABLED'].lower()
        if APISIX_ENABLED == 'true':
            url_base = os.environ['APISIX_ENDPOINT']
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'base_url-----{url_base}')
            return url_base

    def get_request_params(
            self,
            *,
            stream_state: Optional[StreamState] = None,
            stream_slice: Optional[StreamSlice] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        api_key = json.loads(self.config.get('api_key'))
        token = api_key['token']
        id = api_key['id']
        from_date = stream_slice['start_time']
        to_date = stream_slice['start_time']
        return {
            "token": token,
            "id": id,
            "from": from_date,
            "to": to_date,
            "download": "true",
        }