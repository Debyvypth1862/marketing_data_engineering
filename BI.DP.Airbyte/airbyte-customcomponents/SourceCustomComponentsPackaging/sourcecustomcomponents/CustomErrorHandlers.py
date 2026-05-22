import json
import requests
from typing import Optional, Any, Mapping
from dataclasses import dataclass
import sys
from airbyte_cdk.sources.declarative.requesters.error_handlers.http_response_filter import HttpResponseFilter
from airbyte_cdk.sources.declarative.requesters.error_handlers.default_error_handler import DefaultErrorHandler
from airbyte_cdk.sources.declarative.requesters.error_handlers.response_status import ResponseStatus
from airbyte_cdk.sources.declarative.requesters.error_handlers.response_action import ResponseAction


@dataclass
class CustomErrorHandler_for_cellxpert(DefaultErrorHandler):
    DEFAULT_BACKOFF: int = 10
    max_retries: Optional[int] = 5
    max_time: int = 60 * 10
    attempt_count: int = 0

    @classmethod
    def interpret_response(self, response: requests.Response) -> ResponseStatus:
        def matches(response: requests.Response, backoff_time: Optional[float]) -> Optional[ResponseStatus]:
            TOO_MANY_REQUESTS_ERRORS = {429}
            DEFAULT_RETRIABLE_ERRORS = set([x for x in range(500, 600)]).union(TOO_MANY_REQUESTS_ERRORS)

            try:
                print(f'response.status_code = {response.status_code}')
                if response.status_code == 200:
                    if response.text == '[]':
                        return ResponseStatus("SUCCESS")
                    if response.text == '':
                        return ResponseStatus("SUCCESS")
                    if "IP Not Authenticated" in response.text:
                        return ResponseStatus("FAIL", error_message="IP Not Authenticated")

                    if "Bad Authentication" in response.text:
                        return ResponseStatus("FAIL", error_message="Bad Authentication")

                    if "Unauthorized" in response.text:
                        return ResponseStatus("FAIL", error_message="Unauthorized")

                    response_json: dict[str, str] = json.loads(response.text)

                    if response_json.get("message", None) == "Internal server error":
                        return ResponseStatus("RETRY", backoff_time)
                elif response.status_code in DEFAULT_RETRIABLE_ERRORS:
                    return ResponseStatus("RETRY", backoff_time)
                else:
                    return ResponseStatus("FAIL", error_message="Invalid base URL or credentials")
            except Exception as e:
                pass

            return ResponseStatus("SUCCESS")

        matched_status = matches(
            response=response, backoff_time=(self.DEFAULT_BACKOFF * (2 ** self.attempt_count))
        )

        if matched_status.action == ResponseAction.RETRY:
            self.attempt_count += 1
        else:
            self.attempt_count = 0

        if matched_status is not None:
            return matched_status


@dataclass
class CustomErrorHandler(DefaultErrorHandler):
    DEFAULT_BACKOFF: int = 10
    max_retries: Optional[int] = 5
    max_time: int = 60 * 10
    attempt_count: int = 0

    @classmethod
    def interpret_response(self, response: requests.Response) -> ResponseStatus:
        def matches(response: requests.Response, backoff_time: Optional[float]) -> Optional[ResponseStatus]:
            TOO_MANY_REQUESTS_ERRORS = {429}
            DEFAULT_RETRIABLE_ERRORS = set([x for x in range(500, 600)]).union(TOO_MANY_REQUESTS_ERRORS)

            print(f'response.status_code = {response.status_code}')
            try:
                if response.status_code == 404:
                    print(f'response.text = {response.text}')

                if response.status_code == 200:
                    return ResponseStatus(response_action=ResponseAction.SUCCESS)
                elif response.status_code in DEFAULT_RETRIABLE_ERRORS:
                    return ResponseStatus("RETRY", backoff_time)
                else:
                    return ResponseStatus(response_action=ResponseAction.FAIL,
                                          error_message="Invalid base URL or credentials")
            except:
                pass

            return ResponseStatus(response_action=ResponseAction.SUCCESS)

        matched_status = matches(
            response=response, backoff_time=(self.DEFAULT_BACKOFF * (2 ** self.attempt_count))
        )

        if matched_status.action == ResponseAction.RETRY:
            self.attempt_count += 1
        else:
            self.attempt_count = 0

        if matched_status is not None:
            return matched_status


@dataclass
class CustomErrorHandler_for_Softswiss(DefaultErrorHandler):
    DEFAULT_BACKOFF: int = 10
    max_retries: Optional[int] = 5
    max_time: int = 60 * 10
    attempt_count: int = 0

    @classmethod
    def interpret_response(self, response: requests.Response) -> ResponseStatus:
        def matches(response: requests.Response, backoff_time: Optional[float]) -> Optional[ResponseStatus]:
            TOO_MANY_REQUESTS_ERRORS = {429}
            DEFAULT_RETRIABLE_ERRORS = set([x for x in range(500, 600)]).union(TOO_MANY_REQUESTS_ERRORS)
            try:
                print(f'response.status_code = {response.status_code}')
                if response.status_code == 200:
                    return ResponseStatus("SUCCESS")
                elif response.status_code == 404:
                    print(f'response.text = {response.text}')
                    return ResponseStatus("FAIL")
                elif response.status_code in DEFAULT_RETRIABLE_ERRORS:
                    return ResponseStatus("RETRY", backoff_time)
                elif response.status_code == 401:
                    return ResponseStatus("FAIL", error_message="Invalid base URL or credentials")
                elif response.status_code == 422:
                    return ResponseStatus("FAIL", error_message="exchange_rates_date is invalid")

            except Exception as e:
                return ResponseStatus("FAIL", error_message="Invalid base URL or credentials")

            return ResponseStatus("FAIL", error_message="Invalid base URL or credentials")

        matched_status = matches(
            response=response, backoff_time=(self.DEFAULT_BACKOFF * (2 ** self.attempt_count))
        )

        if matched_status.action == ResponseAction.RETRY:
            self.attempt_count += 1
        else:
            self.attempt_count = 0

        if matched_status is not None:
            return matched_status


@dataclass
class CustomErrorHandler_for_Q(DefaultErrorHandler):
    DEFAULT_BACKOFF: int = 10
    max_retries: Optional[int] = 5
    max_time: int = 60 * 10
    attempt_count: int = 0

    @classmethod
    def interpret_response(self, response: requests.Response) -> ResponseStatus:
        def matches(response: requests.Response, backoff_time: Optional[float]) -> Optional[ResponseStatus]:
            TOO_MANY_REQUESTS_ERRORS = {429}
            DEFAULT_RETRIABLE_ERRORS = set([x for x in range(500, 600)]).union(TOO_MANY_REQUESTS_ERRORS)
            try:
                print(f'response.status_code = {response.status_code}')
                if response.status_code == 404:
                    print(f'response.text = {response.text}')

                if response.status_code == 200:
                    response_json: dict[str, str] = json.loads(response.text)
                    if isinstance(response_json, dict):
                        if response_json.get("status", None) == 'Too early':
                            if sys.argv[1] == "check":
                                return ResponseStatus("SUCCESS")
                            return ResponseStatus("RETRY", backoff_time)
                        elif response_json.get("status", None) == 'Permission denied':
                            return ResponseStatus("Fail", error_message="Could not authenticate, wrong token!!!")

                        elif response_json.get("status", None) == 'Invalid date range':
                            return ResponseStatus("Fail",
                                                  error_message="Could authenticate but wrong date range input!!!")


                        elif response_json.get("message", None) == "Internal server error":
                            return ResponseStatus("RETRY", backoff_time)

                        return ResponseStatus("SUCCESS")
                    else:
                        return ResponseStatus("SUCCESS")
                elif response.status_code == 404:
                    return ResponseStatus("FAIL")

                elif response.status_code in DEFAULT_RETRIABLE_ERRORS:
                    return ResponseStatus("RETRY", backoff_time)

                elif response.status_code == 401:
                    return ResponseStatus("FAIL", error_message="Invalid base_url or token")

                else:
                    return ResponseStatus("FAIL", error_message="Could not request authenticate cookies")

            except Exception as e:
                print(f'exception as {e}')
                return ResponseStatus("FAIL", error_message="Invalid base_url or token")

            return ResponseStatus("SUCCESS")

        matched_status = matches(
            response=response, backoff_time=(self.DEFAULT_BACKOFF * (2 ** self.attempt_count))
        )

        if matched_status.action == ResponseAction.RETRY:
            self.attempt_count += 1
        else:
            self.attempt_count = 0

        if matched_status is not None:
            return matched_status


@dataclass
class CustomErrorHandler_for_Smartico(DefaultErrorHandler):
    DEFAULT_BACKOFF: int = 10
    max_retries: Optional[int] = 5
    max_time: int = 60 * 10
    attempt_count: int = 0

    @classmethod
    def interpret_response(self, response: requests.Response) -> ResponseStatus:
        def matches(response: requests.Response, backoff_time: Optional[float]) -> Optional[ResponseStatus]:
            TOO_MANY_REQUESTS_ERRORS = {429}
            DEFAULT_RETRIABLE_ERRORS = set([x for x in range(500, 600)]).union(TOO_MANY_REQUESTS_ERRORS)
            try:
                print(f'response.status_code = {response.status_code}')
                if response.status_code == 404:
                    print(f'response.text = {response.text}')
                # print(response.text)
                response_json: dict[str, str] = json.loads(response.text)

                if response.status_code == 291:
                    if response_json.get("message", None) == "Access to this label is not allowed":
                        return ResponseStatus("RETRY", backoff_time)
                    elif response_json.get("errCode", None) == 3:
                        return ResponseStatus("RETRY", backoff_time)
                elif response_json.get("message", None) == "Internal server error":
                    return ResponseStatus("RETRY", backoff_time)
                elif response.status_code in DEFAULT_RETRIABLE_ERRORS:
                    return ResponseStatus("RETRY", backoff_time)
                elif response.status_code != 200:
                    return ResponseStatus("FAIL", error_message="Invalid base URL or credentials")

            except Exception as e:
                return ResponseStatus("FAIL", error_message="Invalid base URL or credentials")

            return ResponseStatus("SUCCESS")

        matched_status = matches(
            response=response, backoff_time=(self.DEFAULT_BACKOFF * (2 ** self.attempt_count))
        )

        if matched_status.action == ResponseAction.RETRY:
            self.attempt_count += 1
        else:
            self.attempt_count = 0

        if matched_status is not None:
            return matched_status


@dataclass
class CustomErrorHandler_for_Netrefer(DefaultErrorHandler):
    DEFAULT_BACKOFF: int = 10
    max_retries: Optional[int] = 5
    max_time: int = 60 * 10
    attempt_count: int = 0

    @classmethod
    def interpret_response(self, response: requests.Response) -> ResponseStatus:
        def matches(response: requests.Response, backoff_time: Optional[float]) -> Optional[ResponseStatus]:
            TOO_MANY_REQUESTS_ERRORS = {429}
            DEFAULT_RETRIABLE_ERRORS = set([x for x in range(500, 600)]).union(TOO_MANY_REQUESTS_ERRORS)
            print(f'response.status_code = {response.status_code}')

            if response.status_code == 404:
                print(f'response.text = {response.text}')
                return ResponseStatus(response_action=ResponseAction.FAIL, error_message="Invalid base URL")
            elif response.status_code == 401:
                return ResponseStatus(response_action=ResponseAction.FAIL, error_message="Bad credentials")
            elif response.status_code == 200:
                return ResponseStatus("SUCCESS")
            elif response.status_code in DEFAULT_RETRIABLE_ERRORS:
                return ResponseStatus("RETRY", backoff_time)
            else:
                return ResponseStatus(response_action=ResponseAction.FAIL,
                                      error_message="Invalid base URL or credentials")

        matched_status = matches(
            response=response, backoff_time=(self.DEFAULT_BACKOFF * (2 ** self.attempt_count))
        )

        if matched_status.action == ResponseAction.RETRY:
            self.attempt_count += 1
        else:
            self.attempt_count = 0

        if matched_status is not None:
            return matched_status


@dataclass
class CustomErrorHandler_for_Income_access(DefaultErrorHandler):
    DEFAULT_BACKOFF: int = 10
    max_retries: Optional[int] = 5
    max_time: int = 60 * 10
    attempt_count: int = 0

    @classmethod
    def interpret_response(self, response: requests.Response) -> ResponseStatus:
        def matches(response: requests.Response, backoff_time: Optional[float]) -> Optional[ResponseStatus]:
            TOO_MANY_REQUESTS_ERRORS = {429}
            DEFAULT_RETRIABLE_ERRORS = set([x for x in range(500, 600)]).union(TOO_MANY_REQUESTS_ERRORS)
            print(f'response.status_code = {response.status_code}')
            if response.status_code == 404:
                print(f'response.text = {response.text}')

            if response.status_code == 200:
                return ResponseStatus("SUCCESS")
            elif response.status_code in DEFAULT_RETRIABLE_ERRORS:
                return ResponseStatus("RETRY", backoff_time)
            else:
                return ResponseStatus(response_action=ResponseAction.FAIL,
                                      error_message="Invalid base URL or credentials")

        matched_status = matches(
            response=response, backoff_time=(self.DEFAULT_BACKOFF * (2 ** self.attempt_count))
        )

        if matched_status.action == ResponseAction.RETRY:
            self.attempt_count += 1
        else:
            self.attempt_count = 0

        if matched_status is not None:
            return matched_status