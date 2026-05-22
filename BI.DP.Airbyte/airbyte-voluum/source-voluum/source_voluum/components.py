import json
import logging
import os
from dataclasses import dataclass, InitVar
from datetime import datetime, timedelta , timezone
from typing import Any, Mapping, Optional, List, Iterable, Union
import sys
import dpath.util
import requests
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk.sources.declarative.incremental import DatetimeBasedCursor
from airbyte_cdk.sources.declarative.types import Config, StreamSlice, StreamState, Record
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from isodate import Duration
import re
logger = logging.getLogger('airbyte')


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
        # print("Step ---->", self._step)
        # print(self._chunk_date_range(start_datetime, end_datetime, self._step))
        return self._chunk_date_range(start_datetime, end_datetime, self._step)

    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[Mapping[str, Any]]: 
        start_field = "start_time"
        end_field = "end_time"
        dates = []
        loopback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self._recovery_dates.eval(self.config)
        is_recovery = self.config.get('is_recovery')
        if is_recovery:
            if recoveryDates:
                rdates = re.split(r',', recoveryDates)
                # print("rdates ---->", rdates)
                if rdates:
                    for d in rdates:
                        try:
                            dt = datetime.strptime(d, "%Y-%m-%d")
                        except Exception as e:
                            logger.error(e)
                        else:
                            # start_date = self._format_datetime(dt)
                            # end_date = self._format_datetime(dt + timedelta(days=1))
                            dates.append({
                                "start_time": dt.strftime('%Y-%m-%d'), 
                                "end_time": (dt + timedelta(days=1)).strftime("%Y-%m-%d")
                            })
        else:
            date_now = str(datetime.now())
            given_date = datetime.strptime(date_now, "%Y-%m-%d %H:%M:%S.%f")
            converted_date = given_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            start_date = start_date + timedelta(days=-loopback_days)

            while start_date <= converted_date:
                dates.append({
                    'start_time': start_date.strftime('%Y-%m-%d'),
                    'end_time':(start_date + timedelta(days=1)).strftime('%Y-%m-%d')
                })
                start_date += timedelta(days=1)
        return dates