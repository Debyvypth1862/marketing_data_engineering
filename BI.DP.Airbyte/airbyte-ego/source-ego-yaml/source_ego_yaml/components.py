#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from datetime import datetime, timedelta
import logging
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, List, Iterable, Union
from dataclasses import InitVar
import os
import bs4
import requests
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.incremental import DatetimeBasedCursor
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.types import Config, StreamSlice, StreamState
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from isodate import Duration
from jinja2 import Template

import json
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.requesters.error_handlers.error_handler import ErrorHandler
from airbyte_cdk.sources.declarative.requesters.requester import HttpMethod
from typing import Callable
from airbyte_cdk.utils.constants import ENV_REQUEST_CACHE_PATH
from pathlib import Path
from functools import lru_cache
from airbyte_cdk.sources.declarative.exceptions import ReadException
from airbyte_cdk.sources.streams.http.rate_limiting import default_backoff_handler, user_defined_backoff_handler
from airbyte_cdk.models import Level
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, RequestBodyException, UserDefinedBackoffException
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_options_provider import (
    InterpolatedRequestOptionsProvider,
)
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import NoAuth
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from requests.auth import AuthBase
from airbyte_cdk.sources.declarative.requesters.error_handlers.response_status import ResponseStatus
from airbyte_cdk.sources.declarative.requesters.error_handlers.response_action import ResponseAction

MAX_CONNECTION_POOL_SIZE = 20
logger = logging.getLogger('airbyte')


def _parse_html(html_text: str):
    records = []
    soup = bs4.BeautifulSoup(html_text, 'html.parser')
    table = soup.find('table', id='reptable')
    if table:
        theaders = table.findChild('thead').find_all('th')
        trows = table.findChild('tbody').find_all('tr')

        for row in trows:
            record = {}
            cells = row.findChildren('td')
            for i in range(0, len(cells)):
                record[theaders[i].get_text()] = cells[i].get_text()
            records.append(record)

    return records


@dataclass
class CustomRecordExtractor(RecordExtractor):
    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        data = []

        if response.status_code == 200:
            records = _parse_html(response.text)

            if len(records) > 0:
                for record in records:
                    record_raw = {'data': record}
                    data.append(record_raw)

        return data


@dataclass
class CustomAuthenticator(DeclarativeAuthenticator):
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
            if response.status_code == 200:
                cookies_dict = session.cookies.get_dict()
                token = "; ".join([str(x) + "=" + str(y) for x, y in cookies_dict.items()])
                print(token)
        except:
            token = None
        return token


@dataclass
class CustomRequester(HttpRequester):
    name: str
    url_base: Union[InterpolatedString, str]
    path: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: Optional[DeclarativeAuthenticator] = None
    error_handler: Optional[ErrorHandler] = ErrorHandler
    disable_retries: bool = False
    use_cache: bool = False
    http_method: Union[str, HttpMethod] = HttpMethod.POST
    attempt_count: int = 0
    
    _DEFAULT_MAX_RETRY = 6
    _DEFAULT_RETRY_FACTOR = 5
    _DEFAULT_MAX_TIME = 60 * 10

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._url_base = InterpolatedString.create(self.url_base, parameters=parameters)
        self._path = InterpolatedString.create(self.path, parameters=parameters)
        if self.request_options_provider is None:
            self._request_options_provider = InterpolatedRequestOptionsProvider(config=self.config, parameters=parameters)
        elif isinstance(self.request_options_provider, dict):
            self._request_options_provider = InterpolatedRequestOptionsProvider(config=self.config, **self.request_options_provider)
        else:
            self._request_options_provider = self.request_options_provider
        self._authenticator = self.authenticator or NoAuth(parameters=parameters)
        self.error_handler = self.error_handler
        self._parameters = parameters
        self.decoder = JsonDecoder(parameters={})
        self._http_method = HttpMethod[self.http_method] if isinstance(self.http_method, str) else self.http_method
        self._session = self.request_cache()
        self._session.mount(
            "https://", requests.adapters.HTTPAdapter(pool_connections=MAX_CONNECTION_POOL_SIZE, pool_maxsize=MAX_CONNECTION_POOL_SIZE)
        )
        if isinstance(self._authenticator, AuthBase):
            self._session.auth = self._authenticator

    def __hash__(self):
        return hash(tuple(self.__dict__))
    
    def send_request(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
        path: Optional[str] = None,
        request_headers: Optional[Mapping[str, Any]] = None,
        request_params: Optional[Mapping[str, Any]] = None,
        request_body_data: Optional[Union[Mapping[str, Any], str]] = None,
        request_body_json: Optional[Mapping[str, Any]] = None,
        log_formatter: Optional[Callable[[requests.Response], Any]] = None,
    ) -> Optional[requests.Response]:
        request = self._create_prepared_request(
            path=path
            if path is not None
            else self.get_path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            headers=self._request_headers(stream_state, stream_slice, next_page_token, request_headers),
            params=self._request_params(stream_state, stream_slice, next_page_token, request_params),
            json=self._request_body_json(stream_state, stream_slice, next_page_token, request_body_json),
            data=self._request_body_data(stream_state, stream_slice, next_page_token, request_body_data),
        )

        response = self._send_with_retry(request, log_formatter=log_formatter)
        return self._validate_response(response)
    
    def request_cache(self) -> requests.Session:
        if self.use_cache:
            cache_dir = os.getenv(ENV_REQUEST_CACHE_PATH)
            # Use in-memory cache if cache_dir is not set
            # This is a non-obvious interface, but it ensures we don't write sql files when running unit tests
            if cache_dir:
                sqlite_path = str(Path(cache_dir) / self.cache_filename)
            else:
                sqlite_path = "file::memory:?cache=shared"
            return requests_cache.CachedSession(sqlite_path, backend="sqlite")  # type: ignore # there are no typeshed stubs for requests_cache
        else:
            return requests.Session()
    
    @property
    def max_retries(self) -> Union[int, None]:
        if self.disable_retries:
            return 0
    
        return self._DEFAULT_MAX_RETRY
    
    @property
    def max_time(self) -> Union[int, None]:

        if self.error_handler is None:
            return self._DEFAULT_MAX_TIME
        
        if self.attempt_count < (self.max_retries*2)+1:
            self.attempt_count += 1
            return round(5 * ((2 ** 0.5) ** (self.attempt_count)))
    
    def interpret_response_status(self, response: requests.Response) -> ResponseStatus:
        if self.error_handler is None:
            raise ValueError("Cannot interpret response status without an error handler")

        try:
            response_json = json.loads(response.text)
            if response.status_code == 200:
                try:
                    if response_json["message"] == "Internal server error":
                        retry_in = self.max_time
                        return ResponseStatus(response_action=ResponseAction.RETRY, retry_in=retry_in, error_message="Internal server error")
                except:
                    pass
        except:
            pass
        return ResponseStatus(response_action=ResponseAction.SUCCESS, retry_in=None, error_message="")
    
    def _backoff_time(self, response: requests.Response) -> Optional[float]:
        if self.error_handler is None:
            return None
        should_retry = self.interpret_response_status(response)
        if should_retry.action != ResponseAction.RETRY:
            raise ValueError(f"backoff_time can only be applied on retriable response action. Got {should_retry.action}")
        assert should_retry.action == ResponseAction.RETRY
        return should_retry.retry_in
    
    def _should_retry(self, response: requests.Response) -> bool:
        if self.error_handler is None:
            return response.status_code == 429 or 500 <= response.status_code < 600

        if self.use_cache:
            interpret_response_status = self.interpret_response_status
        else:
            # Use a tiny cache to limit the memory footprint. It doesn't have to be large because we mostly
            # only care about the status of the last response received
            # Cache the result because the HttpStream first checks if we should retry before looking at the backoff time
            interpret_response_status = lru_cache(maxsize=10)(self.interpret_response_status)

        return bool(interpret_response_status(response).action == ResponseAction.RETRY)

    def _validate_response(
        self,
        response: requests.Response,
    ) -> Optional[requests.Response]:
        response_status = self.interpret_response_status(response)
        
        if response_status.action == ResponseAction.FAIL:
            error_message = f"Request failed with Status code - {response.status_code}, Error - {HttpRequester.parse_response_error_message(response)}"
            raise ReadException(error_message)
        elif response_status.action == ResponseAction.IGNORE:
            self.logger.info(
                f"Ignoring response for failed request with error message {HttpRequester.parse_response_error_message(response)}"
            )

        return response
    
    def _send_with_retry(
        self,
        request: requests.PreparedRequest,
        log_formatter: Optional[Callable[[requests.Response], Any]] = None,
    ) -> requests.Response:
        max_tries = self.max_retries
        max_time = self._DEFAULT_MAX_TIME

        # if max_tries is not None:
        #     max_tries = max(0, max_tries) + 1
        
        user_backoff_handler = user_defined_backoff_handler(max_tries=max_tries, max_time=max_time)(self._send)  # type: ignore # we don't pass in kwargs to the backoff handler
        backoff_handler = default_backoff_handler(max_tries=max_tries, max_time=max_time, factor=self._DEFAULT_RETRY_FACTOR)
        # backoff handlers wrap _send, so it will always return a response
        return backoff_handler(user_backoff_handler)(request, log_formatter=log_formatter)  # type: ignore
    
    def _send(
        self,
        request: requests.PreparedRequest,
        log_formatter: Optional[Callable[[requests.Response], Any]] = None,
    ) -> requests.Response:

        self.logger.debug(
            "Making outbound API request", extra={"headers": request.headers, "url": request.url, "request_body": request.body}
        )
        response: requests.Response = self._session.send(request)
        self.logger.debug("Receiving response", extra={"headers": response.headers, "status": response.status_code, "body": response.text})
        if log_formatter:
            formatter = log_formatter
            self.message_repository.log_message(
                Level.DEBUG,
                lambda: formatter(response),
            )
        if self._should_retry(response):
            custom_backoff_time = self._backoff_time(response)
            if custom_backoff_time:
                raise UserDefinedBackoffException(backoff=custom_backoff_time, request=request, response=response)
            else:
                raise DefaultBackoffException(request=request, response=response)
        return response

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
            logger.info(f'APISIX_ENDPOINT url_base --> {url_base}')
            return url_base
        else:
            url_base = self.config.get('base_url')
            logger.info(f'url_base --> {url_base}')
            return url_base  


class CustomDateTimeBasedCursor(DatetimeBasedCursor):
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    recovery_dates: Optional[Union[InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        super().__post_init__(self)
        self._recovery_dates = InterpolatedString.create(self.recovery_dates, parameters=parameters)


    def stream_slices(self) -> Iterable[StreamSlice]:
        end_datetime = self._select_best_end_datetime()
        start_datetime = self._calculate_earliest_possible_value(self._select_best_end_datetime())
        return self._partition_daterange(start_datetime, end_datetime, self._step)

    def _partition_daterange(self, start: datetime, end: datetime, step: Union[timedelta, Duration]):
        start_field = "start_time"
        end_field = "end_time"
        dates = []
        loopback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self.config.get('recovery_dates')
        print(recoveryDates)
        is_recovery = self.config.get('is_recovery')
        if is_recovery:
            if recoveryDates:
                rdates = re.split(r',', recoveryDates)
                print(rdates)
                if rdates:
                    for d in rdates:
                        print(d)
                        try:
                            dt = datetime.strptime(d, "%Y-%m-%d")
                        except Exception as e:
                            logger.error(e)
                        else:
                            start_date = end_date = self._format_datetime(dt)     
                            for report in self.config["reports"].split(','):
                                dates.append({start_field: start_date, end_field: end_date, 'report': report})           
        else:
            start = start + timedelta(days=-loopback_days)
            while start <= end:
                next_start = self._evaluate_next_start_date_safely(start, step)
                end_date = self._get_date(next_start - self._cursor_granularity, end, min)
                for report in self.config["reports"].split(','):
                    dates.append({start_field: self._format_datetime(start), end_field: self._format_datetime(end_date), 'report': report})
                start = next_start
        print(dates)        
        return dates

class CustomDateTimeBasedCursor(DatetimeBasedCursor):
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    recovery_dates: Optional[Union[InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        super().__post_init__(self)
        self._recovery_dates = InterpolatedString.create(self.recovery_dates, parameters=parameters)


    def stream_slices(self) -> Iterable[StreamSlice]:
        end_datetime = self._select_best_end_datetime()
        start_datetime = self._calculate_earliest_possible_value(self._select_best_end_datetime())
        return self._partition_daterange(start_datetime, end_datetime, self._step)

    def _partition_daterange(self, start: datetime, end: datetime, step: Union[timedelta, Duration]):
        start_field = "start_time"
        end_field = "end_time"
        dates = []
        loopback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self.config.get('recovery_dates')
        print(recoveryDates)
        is_recovery = self.config.get('is_recovery')
        if is_recovery:
            if recoveryDates:
                rdates = re.split(r',', recoveryDates)
                print(rdates)
                if rdates:
                    for d in rdates:
                        print(d)
                        try:
                            dt = datetime.strptime(d, "%Y-%m-%d")
                        except Exception as e:
                            logger.error(e)
                        else:
                            start_date = end_date = self._format_datetime(dt)     
                            for report in self.config["reports"].split(','):
                                dates.append({start_field: start_date, end_field: end_date, 'report': report})           
        else:
            start = start + timedelta(days=-loopback_days)
            while start <= end:
                next_start = self._evaluate_next_start_date_safely(start, step)
                end_date = self._get_date(next_start - self._cursor_granularity, end, min)
                for report in self.config["reports"].split(','):
                    dates.append({start_field: self._format_datetime(start), end_field: self._format_datetime(end_date), 'report': report})
                start = next_start
        print(dates)        
        return dates
