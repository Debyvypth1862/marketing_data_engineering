import json
import logging
import bs4
import sys
import xmltodict
import dpath.util
import requests
import csv
import io
from dataclasses import dataclass, InitVar
from jsonpath_ng import parse
from typing import Any, Mapping, Union, List, Dict
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources.declarative.types import Config, Record
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString

logger = logging.getLogger('airbyte')


@dataclass  # Cellxpert and Netrefer
class CustomRecordExtractor(DpathExtractor):
    config: Config

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        # Check VPN connectivity
        final_response = []
        actual_response = []
        response_body = self.decoder.decode(response)
        if len(self.field_path) == 0:
            extracted = response_body
        else:
            path = [path.eval(self.config) for path in self.field_path]
            if "*" in path:
                extracted = dpath.util.values(response_body, path)
            else:
                extracted = dpath.util.get(response_body, path, default=[])
        if isinstance(extracted, list):
            actual_response = extracted
        elif extracted:
            actual_response = [extracted]

        for record in actual_response:
            final_response.append({"data": record, "tracker_login_id": self.config["tracker_login_id"]})

        return final_response


@dataclass
class CustomRecordExtractor_for_netrefer(DpathExtractor):
    config: Config

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:

        if response.status_code == 404:
            assert False
        if "IP Not Authenticated" in response.text:
            logger.error("IP Not Authenticated")
            assert False
        # Check Bad Authentication
        if "Bad Authentication" in response.text:
            logger.error("Bad Authentication")
            assert False
        final_response = []
        actual_response = []
        response_body = self.decoder.decode(response)
        if len(self.field_path) == 0:
            extracted = response_body
        else:
            path = [path.eval(self.config) for path in self.field_path]
            if "*" in path:
                extracted = dpath.util.values(response_body, path)
            else:
                extracted = dpath.util.get(response_body, path, default=[])
        if isinstance(extracted, list):
            actual_response = extracted
        elif extracted:
            actual_response = [extracted]

        for record in actual_response:
            # print(record)
            if response.status_code == 200:
                response_json: dict[str, str] = json.loads(response.text)
                if response_json[0].get("column", None) == "No Data was Found":
                    pass
                elif response_json[0].get("Message",
                                          None) == "Limit for report requests with the given filters have been exceeded. Please try in the next hour.":
                    pass
                else:
                    final_response.append({"data": record, "tracker_login_id": self.config["tracker_login_id"]})

        return final_response


@dataclass
class CustomRecordExtractor_for_softswiss(DpathExtractor):  # To read records as per dates

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        final_response = []
        actual_response = []
        response_body = self.decoder.decode(response)
        response_json = json.loads(response.text)

        response_json = json.loads(response.text)
        if not response_json['rows']['data']:
            pass
        else:
            for record in response_json['rows']['data']:  # Read data from response
                final_response.append({"data": record})

        return final_response


@dataclass
class CustomRecordExtractor_for_xmlresponse(RecordExtractor):
    responsebody: Union[InterpolatedString, str]
    isattributes: Union[InterpolatedString, bool]
    responseheaders: Union[InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]):
        self._responsebody = InterpolatedString.create(self.responsebody, parameters=parameters)
        self._isattributes = InterpolatedString.create(self.isattributes, parameters=parameters)
        self._responseheaders = InterpolatedString.create(self.responseheaders, parameters=parameters)

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        response_body = self._responsebody.eval(self.responsebody)
        flag = self._isattributes
        response_headers = self._responseheaders.eval(self.responseheaders)
        return self.xmlToJsonResponse(response.text, response_body, response_headers, flag)

    def xmlToJsonResponse(self, _xmlresponse, responsepath, headerspath, isattribute):
        data_dict = xmltodict.parse(_xmlresponse)
        # Read response body
        expression = parse(responsepath)  # '$.feedStatsResult.results.player[*]'
        match = expression.find(data_dict)
        responseList = []
        metadata_dict = {}
        for i in range(len(match)):
            if (isattribute == False or headerspath.strip() == True):
                # Read response Headers
                response_header = {}
                exp = parse(headerspath)
                _match = exp.find(data_dict)
                for i in range(len(_match)):
                    response_header = _match[i].value
                response_body = {"row": match[i].value}
                responseList.append(response_header | response_body)
            elif (isattribute):
                _dict = match[i].value
                for key, value in _dict.items():
                    metadata_dict[key.replace('@', '')] = value
                responseList.append(metadata_dict)
            else:
                logger.info("empty records")
                return []

        return responseList


@dataclass
class CustomRecordExtractor_for_mexos(DpathExtractor):
    config: Config

    def extract_records(self, response: requests.Response) -> List[Record]:
        response_body = self.decoder.decode(response)
        if 'hasData' in response_body:
            if response_body['hasData']:
                if response_body['data'] is None:
                    return []

                data_headers = response_body['data']['headerTitles']
                data_values = response_body['data']['values']
                finalrecord = []
                for value in data_values:
                    row = {}
                    record = {}
                    row_data = value['data']
                    for i in range(len(row_data)):
                        row[data_headers[i]] = row_data[i]
                    finalrecord.append(row)

                final_response = []
                actual_response = []
                if len(self.field_path) == 0:
                    extracted = finalrecord
                else:
                    path = [path.eval(self.config) for path in self.field_path]
                    if "*" in path:
                        extracted = dpath.util.values(finalrecord, path)
                    else:
                        extracted = dpath.util.get(finalrecord, path, default=[])
                if isinstance(extracted, list):
                    actual_response = extracted
                elif extracted:
                    actual_response = [extracted]

                for record in actual_response:
                    final_response.append({"data": record, "tracker_login_id": self.config["tracker_login_id"]})

                return final_response
        return []


@dataclass
class CustomRecordExtractor_for_Myaffiliates_xml(RecordExtractor):
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    StreamSlice = Mapping[str, Any]
    cursor_field = "date"

    def __post_init__(self, parameters: Mapping[str, Any]):
        pass

    def extract_records(self, response: requests.Response) -> List[Dict[Union[str, Any], Union[str, Any]]]:
        return self.parse_response(response)

    def _guaranteed_list(self, x):
        if not x:
            return []
        elif isinstance(x, list):
            return x
        else:
            return [x]

    def _parse_xml(self, xml_response):
        data_dict = xmltodict.parse(xml_response)
        report_tables = self._guaranteed_list(data_dict['reports']['table'])
        rows_lst = []
        for table in report_tables:
            metas = table['meta']
            colDefs = table['colDefs']['col']
            rows = self._guaranteed_list(table['cellsRegion'][0]['row'])
            metadata_dict = {}
            colDefs_lst = []
            metadata_dict['plan_id'] = table['@planId']
            metadata_dict['subscription'] = table['@subscription']
            metadata_dict['customer_group'] = table['@customer_group']
            metadata_dict['caption'] = table['caption']
            for meta in metas:
                if '#text' in meta:
                    metadata_dict[meta['@name']] = meta['#text']
                else:
                    metadata_dict[meta['@name']] = ''
            for col in colDefs:
                colDefs_lst.append(col['def']['#text'])
            for row in rows:
                row_json = {}
                cells = row['cell']
                for i in range(len(cells)):
                    cell = cells[i]
                    row_json[colDefs_lst[i]] = cell['@value']
                rows_lst.append(row_json | metadata_dict)
        return rows_lst

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> List[Dict[Union[str, Any], Union[str, Any]]]:
        # The response is a simple JSON whose schema matches our stream's schema exactly,
        # so we just return a list containing the response
        # if response.status_code == 200:
        # print(response.text)
        if '<html' not in response.text:
            if 'table' in response.text:
                data = self._parse_xml(response.text)
                # logger.info(f"tracker_login_id {self.config['tracker_login_id']}")
                final_record = []
                for record in data:
                    # record['tracker_login_id'] = self.config["tracker_login_id"]
                    # .strftime('%Y-%m-%d')
                    final_record.append({"data": record, "tracker_login_id": self.config["tracker_login_id"]})

                return final_record
            else:
                logger.warning(f"Response not in html format for slice data")
                return []
        else:
            logger.warning(f"Response not in html format for slice data")
            return []


@dataclass
class CustomRecordExtractor_for_Buffalopartner(RecordExtractor):  # diff
    jsonpath: Union[InterpolatedString, str]
    isattributes: Union[InterpolatedString, bool]
    parameters: InitVar[Mapping[str, Any]]
    config: Config

    def __post_init__(self, parameters: Mapping[str, Any]):
        self._jsonpath = InterpolatedString.create(self.jsonpath, parameters=parameters)
        self._isattributes = InterpolatedString.create(self.isattributes, parameters=parameters)

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        # response_body = self.decoder.decode(response)
        json_string = self._jsonpath.eval(self.jsonpath)
        flag = self._isattributes

        # logger.info(f'##########{self.xmlToJsonResponse(response ,response.text, json_string,"", flag)}')
        # response = self.xmlToJsonResponse(response ,response.text, json_string,"", flag)
        # logger.info(response_body)

        return self.xmlToJsonResponse(response, response.text, json_string, "", flag)

    def xmlToJsonResponse(self, response, _xmlresponse, responsepath, headerspath, isattribute):

        logger.info(response.status_code)
        logger.info('xmlToJsonResponse')
        try:
            # response = requests.get(base_url + '/api/feed/revshare/player', params = request_params)
            if response.status_code == 200:
                if 'feedStatsResult' in response.text:
                    if 'results' in response.text:
                        data_dict = xmltodict.parse(_xmlresponse)
                        # Read response body
                        expression = parse(responsepath)  # '$.feedStatsResult.results.player[*]'
                        match = expression.find(data_dict)
                        responseList = []
                        # metadata_dict = {}
                        logger.info(len(match))
                        for i in range(len(match)):
                            if (isattribute == False or headerspath.strip() == True):
                                # Read response Headers
                                response_header = {}
                                response_body = {}
                                exp = parse(headerspath)
                                _match = exp.find(data_dict)
                                for j in range(len(_match)):
                                    response_header = _match[j].value
                                response_body = {"row": match[i].value}

                                responseList.append(response_header | response_body)
                            elif (isattribute):
                                _dict = match[i].value

                                metadata_dict = {}
                                for key, value in _dict.items():
                                    metadata_dict[key.replace('@', '')] = value
                                responseList.append(metadata_dict)
                            else:
                                pass

                        final_response = []
                        for record in responseList:
                            final_response.append(
                                {"data": record, "tracker_login_id": self.config.get('tracker_login_id')})
                        return final_response
                    else:
                        logger.error(f"Logging fail")
                        # AirbyteLogger().log("ERROR", f"Logging fail")
                        return False, None
                else:
                    if "XmlFeedError" in _xmlresponse:
                        error_message_start_index = _xmlresponse.find("<ErrorMessage>")
                        error_message_end_index = _xmlresponse.find("</ErrorMessage>")
                        error_message = _xmlresponse[
                                        error_message_start_index + len("<ErrorMessage>"): error_message_end_index]
                        logging.error("Error: %s", error_message)
                        raise ValueError
                        sys.exit()
                    else:
                        logger.error(f"Logging fail")
                        error_message = "Invalid base URL"
                        sys.exit()
                    # AirbyteLogger().log("ERROR", f"Logging fail")
                    return False, None
            else:
                error_message = "Invalid base URL"
                logger.error(f"Logging fail")
                sys.exit()
                # AirbyteLogger().log("ERROR", f"Logging fail")
                return False, None
        except Exception as e:
            logger.error(f"Logging fail {e}")
            # AirbyteLogger().log("ERROR", f"Logging fail: Send request got error {e}")
            raise ValueError(error_message)
            return False, error_message


@dataclass  # diff(Soap env fault)
class CustomRecordExtractor_for_IncomeAccess(RecordExtractor):
    responsebody: Union[InterpolatedString, str]
    isattributes: Union[InterpolatedString, bool]
    responseheaders: Union[InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config

    def __post_init__(self, parameters: Mapping[str, Any]):
        self._responsebody = InterpolatedString.create(self.responsebody, parameters=parameters)
        self._isattributes = InterpolatedString.create(self.isattributes, parameters=parameters)
        self._responseheaders = InterpolatedString.create(self.responseheaders, parameters=parameters)

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        # response_body = self.decoder.decode(response)
        response_body = self._responsebody.eval(self.responsebody)
        flag = self._isattributes
        response_headers = self._responseheaders.eval(self.responseheaders)
        return self.xmlToJsonResponse(response, response.text, response_body, response_headers, flag)

    def xmlToJsonResponse(self, response, _xmlresponse, responsepath, headerspath, isattribute):
        data_dict = xmltodict.parse(_xmlresponse)
        if response.status_code == 200:
            reponse_dict = xmltodict.parse(response.text)
            if 'SOAP-ENV:Fault' in reponse_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']:
                if reponse_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['SOAP-ENV:Fault']['faultstring'] == 'No Records':
                    logger.info(f"Logging Success")
                    return []
                else:
                    logger.info(
                        f"Logging fail: {reponse_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['SOAP-ENV:Fault']['faultstring']}")
                    raise ValueError("Bad credentials")
            else:
                logger.info(f"Logging Success")
                expression = parse(responsepath)  # '$.feedStatsResult.results.player[*]'
                match = expression.find(data_dict)
                responseList = []
                metadata_dict = {}
                for i in range(len(match)):
                    if (isattribute == False or headerspath.strip() == True):
                        # Read response Headers
                        response_header = {}
                        response_body = {}
                        exp = parse(headerspath)
                        _match = exp.find(data_dict)
                        for j in range(len(_match)):
                            response_header = _match[j].value
                        response_body = {"row": match[i].value}

                        responseList.append(response_header | response_body)

                    elif (isattribute):
                        _dict = match[i].value
                        for key, value in _dict.items():
                            metadata_dict[key.replace('@', '')] = value
                        responseList.append(metadata_dict)
                    else:
                        pass
                return responseList
        else:
            logger.info(f"Logging fail: {response.text}")
            raise ValueError("Invalid base URL")


@dataclass
class CustomRecordExtractor_for_Q(DpathExtractor):  # diff

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        final_response = []
        actual_response = []
        response_body = self.decoder.decode(response)
        # print(response.status_code)
        x = 0
        try:
            if response.status_code == 200:

                # print(response_body)
                if isinstance(response_body, dict):
                    if 'status' in response_body:
                        if response_body['status'] == 'Permission denied':

                            AirbyteLogger().log("ERROR", f"Could not authenticate, wrong token!!!")



                        elif response_body['status'] == 'Invalid date range':
                            AirbyteLogger().log("ERROR", f"Could authenticate but wrong date range input!!!")


                        else:
                            AirbyteLogger().log("INFO", f"Authenticate succeeded")
                            x = 1
                    elif response.text == '[]':
                        AirbyteLogger().log("INFO", f"Authenticate succeeded")
                        x = 1
                    else:
                        AirbyteLogger().log("INFO", f"Authenticate succeeded")
                        x = 1
                else:
                    AirbyteLogger().log("INFO", f"Authenticate succeeded!!! list of records!!!")
                    x = 1
            else:
                AirbyteLogger().log("ERROR", f"Could not request authenticate cookies")


        except:
            AirbyteLogger().log("ERROR", f"Could not authenticate, please check base_url and token")

        if x == 1:

            if len(self.field_path) == 0:
                extracted = response_body
            else:
                path = [path.eval(self.config) for path in self.field_path]
                if "*" in path:
                    extracted = dpath.util.values(response_body, path)
                else:
                    extracted = dpath.util.get(response_body, path, default=[])
            if isinstance(extracted, list):
                actual_response = extracted
            elif extracted:
                actual_response = [extracted]

            for record in actual_response:
                final_response.append({"data": record})

            return final_response
        else:
            sys.exit()


@dataclass
class CustomRecordExtractor_for_Smartico(DpathExtractor):  # diff
    config: Config

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        # Check VPN connectivity
        if "IP Not Authenticated" in response.text:
            logger.error("IP Not Authenticated")
            assert False
        # Check Bad Authentication
        if "Bad Authentication" in response.text:
            logger.error("Bad Authentication")
            assert False
        if response.status_code != 200:
            logger.error("Could not authenticate")
            sys.exit()
            assert False

        final_response = []
        actual_response = []
        response_body = self.decoder.decode(response)
        if len(self.field_path) == 0:
            extracted = response_body
        else:
            path = [path.eval(self.config) for path in self.field_path]
            if "*" in path:
                extracted = dpath.util.values(response_body, path)
            else:
                extracted = dpath.util.get(response_body, path, default=[])
        if isinstance(extracted, list):
            actual_response = extracted
        elif extracted:
            actual_response = [extracted]

        for record in actual_response:
            if 'id' in record:
                del record['id']
            final_response.append({"data": record, "tracker_login_id": self.config["tracker_login_id"]})

        return final_response


def _parse_html(html_text: str):
    records = []
    soup = bs4.BeautifulSoup(html_text, 'html.parser')
    table = soup.find('table', id='reptable')
    if table:
        print('Found table in html_text')
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
class CustomRecordExtractor_for_Ego(RecordExtractor):
    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        data = []
        if response.status_code == 200:
            records = _parse_html(response.text)
            if len(records) > 0:
                for record in records:
                    record_raw = {'data': record}
                    data.append(record_raw)

        return data


@dataclass  # Referon

class CustomRecordExtractor_for_Referon(RecordExtractor):
    config: Config
    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        final_response = []
        if response.status_code == 200:
            csv_data = csv.DictReader(io.StringIO(response.text),delimiter = ",",quotechar = '"',lineterminator='\n',doublequote=True)
            for row in csv_data:
                final_response.append({"data": row, "tracker_login_id": self.config["tracker_login_id"]})
        return final_response