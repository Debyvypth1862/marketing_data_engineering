import re
import logging
from dataclasses import dataclass, InitVar
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional, Union, List
from airbyte_cdk.sources.declarative.incremental import DatetimeBasedCursor
from isodate import Duration
from airbyte_cdk.sources.declarative.types import Config, StreamSlice
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString

logger = logging.getLogger('airbyte')


@dataclass
class CustomDateTimeBasedCursor_for_softswiss(DatetimeBasedCursor):
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

    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[
        Mapping[str, Any]]:
        start_field = "start_time"
        end_field = "end_time"
        dates = []
        slices = []
        lookback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self._recovery_dates.eval(self.config)
        is_recovery = self.config.get('is_recovery')
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
                            slices.append({start_field: start_date, end_field: end_date})
        else:

            date_now = str(datetime.now())
            given_date = datetime.strptime(date_now, "%Y-%m-%d %H:%M:%S.%f")
            converted_date = given_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

            stream_state = str(start_date).split(" ")[:-1][0]  # get stream state date
            start_date = start_date + timedelta(days=-lookback_days)
            config_start_date = self.config.get('start_date')  # start date from config

            # if config_start_date == stream_state:  # If condition statisfy then its full refresh so step is 30 else 1 --> super wrong
            #     chunk_size = int(self.config.get('step'))
            # else:
            #     chunk_size = 1

            chunk_size = int(self.config.get('step'))
            
            while start_date < converted_date:
                dates.append(start_date)
                start_date += timedelta(days=chunk_size)

            for start_date in dates:
                if start_date + timedelta(days=chunk_size) > converted_date:
                    end_date = converted_date
                else:
                    end_date = start_date + timedelta(days=chunk_size)
                end_date = end_date - timedelta(days=1)

                slices.append({start_field: start_date.strftime('%Y-%m-%d'), end_field: end_date.strftime('%Y-%m-%d')})
        logger.info(slices)
        return slices


@dataclass  # for (Myaffiliates)
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

    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[
        Mapping[str, Any]]:
        start_field = "start_time"
        end_field = "end_time"
        dates = []

        lookback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self._recovery_dates.eval(self.config)
        is_recovery = self.config.get('is_recovery')
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
            start_date = start_date + timedelta(days=-lookback_days)
            while start_date <= end_date:
                dates.append(
                    {start_field: self._format_datetime(start_date), end_field: self._format_datetime(start_date)})
                start_date += timedelta(days=1)
        logger.info(dates)
        return dates


class CustomDateTimeBasedCursor_for_Buffalo_and_IncomeAccess(DatetimeBasedCursor):  # diff but same as IncAcc

    def stream_slices(self, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[
        StreamSlice]:
        end_datetime = self._select_best_end_datetime()
        start_datetime = self._calculate_earliest_possible_value(self._select_best_end_datetime())
        return self._chunk_date_range(start_datetime, end_datetime)

    def _chunk_date_range(self, start_date: datetime, end_date: datetime) -> List[Mapping[str, Any]]:
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
                dates.append(
                    {start_field: self._format_datetime(start_date), end_field: self._format_datetime(start_date)})
                start_date += timedelta(days=1)
        logger.info(dates)
        return dates


class CustomDateTimeBasedCursor_for_Q(DatetimeBasedCursor):  # diff
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    recovery_dates: Optional[Union[InterpolatedString, str]] = None
    cursor_field = "end"

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        super().__post_init__(self)
        self._recovery_dates = InterpolatedString.create(self.recovery_dates, parameters=parameters)

    def stream_slices(self) -> Iterable[StreamSlice]:
        end_datetime = self._select_best_end_datetime()
        start_datetime = self._calculate_earliest_possible_value(self._select_best_end_datetime())
        return self._chunk_date_range(start_datetime, end_datetime, self._step)

    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[
        Mapping[str, Any]]:
        start_field = "start_time"
        end_field = "end_time"
        slices = []
        dates = []
        merchants = self.config.get("merchant").split(",")
        loopback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self.config.get('recovery_dates')
        is_recovery = self.config.get('is_recovery')
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
                            start_date = self._format_datetime(dt)
                            # end_date = self._format_datetime(dt + timedelta(days=1))
                            dates.append(start_date)
            # checking logic sample code
            for merchantid in merchants:
                for start_date in dates:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d")

                    formatted_date = start_date.strftime("%Y-%m-%d")
                    end_date = start_date
                    slices.append({'start': formatted_date, 'end': formatted_date, 'merchant': merchantid})


        else:

            date_now = str(datetime.now())
            given_date = datetime.strptime(date_now, "%Y-%m-%d %H:%M:%S.%f")
            converted_date = given_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

            start_date = start_date + timedelta(days=-loopback_days)
            while start_date < converted_date:
                dates.append(start_date)
                start_date += timedelta(days=30)

            for merchantid in merchants:
                for start_date in dates:
                    if start_date + timedelta(days=30) > converted_date:
                        end_date = converted_date
                    else:
                        end_date = start_date + timedelta(days=30)
                    end_date = end_date - timedelta(days=1)

                    slices.append({'start': start_date.strftime('%Y-%m-%d'), 'end': end_date.strftime('%Y-%m-%d'),
                                   'merchant': merchantid})

        logger.info(slices)
        return slices


@dataclass
class CustomDateTimeBasedCursor_for_Smartico(DatetimeBasedCursor):  # diff
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

    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[
        Mapping[str, Any]]:
        start_field = "start_time"
        end_field = "end_time"
        dates = []
        loopback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self.config.get('recovery_dates')
        is_recovery = self.config.get('is_recovery')
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
                            start_date = self._format_datetime(dt)
                            end_date = self._format_datetime(dt + timedelta(days=1))
                            dates.append({start_field: start_date, end_field: end_date})
        else:
            date_now = str(datetime.now())
            given_date = datetime.strptime(date_now, "%Y-%m-%d %H:%M:%S.%f")
            converted_date = given_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            start_date = start_date + timedelta(days=-loopback_days)
            while start_date <= converted_date:
                dates.append({'start_time': start_date.strftime('%Y-%m-%d'),
                              'end_time': (start_date + timedelta(days=1)).strftime('%Y-%m-%d')})
                start_date += timedelta(days=1)
        logger.info(dates)
        return dates


@dataclass
class CustomDateTimeBasedCursor_for_Netrefer(DatetimeBasedCursor):  # diff
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

    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[
        Mapping[str, Any]]:
        start_field = "start_time"
        end_field = "end_time"
        dates = []
        loopback_days = int(self.config.get('loopback_days', 0))
        recoveryDates = self.config.get('recovery_dates')
        is_recovery = self.config.get('is_recovery')
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
            date_now = str(datetime.now())
            given_date = datetime.strptime(date_now, "%Y-%m-%d %H:%M:%S.%f")
            converted_date = given_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            start_date = start_date + timedelta(days=-loopback_days)
            while start_date < converted_date:
                dates.append(
                    {'start_time': start_date.strftime('%Y-%m-%d'), 'end_time': start_date.strftime('%Y-%m-%d')})
                start_date += timedelta(days=1)
        logger.info(dates)
        return dates


class CustomDateTimeBasedCursor_for_Ego(DatetimeBasedCursor):  # diff
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
        is_recovery = self.config.get('is_recovery')
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
                            for report in self.config["reports"].split(','):
                                dates.append({start_field: start_date, end_field: end_date, 'report': report})
        else:
            start = start + timedelta(days=-loopback_days)
            while start <= end:
                next_start = self._evaluate_next_start_date_safely(start, step)
                end_date = self._get_date(next_start - self._cursor_granularity, end, min)
                for report in self.config["reports"].split(','):
                    dates.append({start_field: self._format_datetime(start), end_field: self._format_datetime(end_date),
                                  'report': report})
                start = next_start
        return dates


@dataclass
class CustomDateTimeBasedCursor_for_Voluum(DatetimeBasedCursor):
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

    def _chunk_date_range(self, start_date: datetime, end_date: datetime, step: Union[timedelta, Duration]) -> List[
        Mapping[str, Any]]:
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
                    'end_time': (start_date + timedelta(days=1)).strftime('%Y-%m-%d')
                })
                start_date += timedelta(days=1)
        return dates

