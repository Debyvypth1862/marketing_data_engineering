import json
import logging
import os
from dataclasses import dataclass, InitVar
from datetime import datetime, timedelta , timezone
from typing import Any, Mapping, Optional, List, Iterable, Union,Callable, MutableMapping
import sys
import dpath.util
import requests
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.incremental import DatetimeBasedCursor
from airbyte_cdk.sources.declarative.types import Config, StreamSlice, StreamState, Record
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from isodate import Duration
import re
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_options_provider import (
    InterpolatedRequestOptionsProvider,
)
from airbyte_cdk.sources.declarative.requesters.error_handlers.http_response_filter import HttpResponseFilter
from airbyte_cdk.sources.declarative.requesters.error_handlers.response_action import ResponseAction
from airbyte_cdk.sources.declarative.requesters.error_handlers.response_status import ResponseStatus
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator, NoAuth
from airbyte_cdk.sources.declarative.requesters.error_handlers.error_handler import ErrorHandler
from airbyte_cdk.sources.streams.http.http import HttpStream
from airbyte_cdk.sources.declarative.requesters.requester import HttpMethod
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.utils.constants import ENV_REQUEST_CACHE_PATH
from requests.auth import AuthBase
from pathlib import Path
from functools import lru_cache
from airbyte_cdk.sources.declarative.exceptions import ReadException
from airbyte_cdk.sources.streams.http.rate_limiting import default_backoff_handler, user_defined_backoff_handler
from airbyte_cdk.models import Level
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, RequestBodyException, UserDefinedBackoffException
MAX_CONNECTION_POOL_SIZE = 20
from jinja2 import Template
logger = logging.getLogger('airbyte')

@dataclass
class CustomRecordExtractor(RecordExtractor):
    responsebody: Union[InterpolatedString, str]
    isattributes: Union[InterpolatedString, bool]
    responseheaders: Union[InterpolatedString,str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    def __post_init__(self, parameters: Mapping[str, Any]):
        self._responsebody = InterpolatedString.create(self.responsebody, parameters=parameters)
        self._isattributes = InterpolatedString.create(self.isattributes, parameters=parameters)
        self._responseheaders = InterpolatedString.create(self.responseheaders, parameters = parameters)
        
        logger.info(self._responsebody.eval(self.responsebody))
        logger.info(self._isattributes)
        logger.info(self._responseheaders.eval(self.responseheaders))
    def extract_records(self, response: requests.Response) -> List[Mapping[str , Any]]:
        # response_body = self.decoder.decode(response)
        response_body = self._responsebody.eval(self.responsebody)
        logger.info(response_body)
        flag = self._isattributes
        response_headers = self._responseheaders.eval(self.responseheaders)
        return self.xmlToJsonResponse(response,response.text, response_body,response_headers, flag)
    
    
    def xmlToJsonResponse(self,response,_xmlresponse, responsepath, headerspath, isattribute):
        data_dict = xmltodict.parse(_xmlresponse)
        if response.status_code == 200:   
            reponse_dict = xmltodict.parse(response.text)
            logger.info(response.status_code)
            if 'SOAP-ENV:Fault' in reponse_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']:
                if reponse_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['SOAP-ENV:Fault']['faultstring'] == 'No Records':
                    logger.info(f"Logging Success")
                    return []
                else:
                    logger.info(f"Logging fail: {reponse_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['SOAP-ENV:Fault']['faultstring']}")
                    sys.exit()
            else:
                logger.info(f"Logging Success")
                expression = parse(responsepath)  # '$.feedStatsResult.results.player[*]'
                match = expression.find(data_dict)
                responseList = []
                metadata_dict = {}
                logger.info(len(match))
                for i in range(len(match)):
                    if(isattribute == False or headerspath.strip() == True):
                        # Read response Headers
                        response_header = {}
                        response_body = {}
                        exp = parse(headerspath)
                        _match = exp.find(data_dict)
                        for j in range(len(_match)):
                            response_header = _match[j].value
                        response_body = {"row": match[i].value}
                        
                        responseList.append(response_header | response_body)

                    elif(isattribute):
                        _dict = match[i].value
                        for key, value in _dict.items():
                            metadata_dict[key.replace('@', '')] = value
                        responseList.append(metadata_dict)
                    else:
                        pass
                #     final_response = []
                # for record in responseList:
                #     final_response.append({"data": record,"tracker_login_id": self.config.get('tracker_login_id')})
                return responseList    
        else:
            logger.info(f"Logging fail: {response.text}")
            sys.exit()

@dataclass
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
        return self._chunk_date_range(start_datetime, end_datetime, self._step)
 
    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[Mapping[str, Any]]:
        start_field = "start_time"
        end_field = "end_time"
        dates = []
        recoveryDates = self.config.get('recovery_dates')
        is_recovery = self.config.get('is_recovery')
        loopback_days = int(self.config.get('loopback_days', 0))
        if is_recovery:
            if recoveryDates:
                rdates = re.split(r',', recoveryDates)
                if rdates:
                    for d in rdates:
                        try:
                            dt = datetime.strptime(d, "%Y-%m-%d")
                        except Exception as e:
                            logger.error(e)
                        else:
                            start_date = end_date = self._format_datetime(dt)
                            dates.append({start_field: start_date, end_field: end_date})
        else:
            start_date = start_date + timedelta(days=-loopback_days)

            while start_date < end_date:
                dates.append({start_field: self._format_datetime(start_date), end_field: self._format_datetime(start_date)})
                start_date += timedelta(days=1)
        logger.info(dates)  
        return dates   
    
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
    http_method: Union[str, HttpMethod] = HttpMethod.GET
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
            # print("/" * 40)
            # print(f"Attempt count: {self.attempt_count} Current wait time: {5 * ((2 ** 0.5) ** (self.attempt_count))}")
            # print("/" * 40)
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
            # Define the template
            template_str = """
            {% set cleaned_url = base_url | replace('https://', '') | replace('http://', '') | trim('/') %}
            {{ cleaned_url }}
            """
            
            # Create a template object
            template = Template(template_str)
            
            # Render the template with the base_url from config
            Host = template.render(base_url=base_url).strip()
            logger.info(f'host --> {Host}')
            # Return the headers in the required format
            return {"host": Host}
        # else:
        #     return
           
    
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