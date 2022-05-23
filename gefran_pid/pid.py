import atexit
import logging
import threading
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Union, Any, List

from hein_utilities.runnable import Runnable
from hein_utilities.temporal_data import TemporalData
import modbus_tk.defines as cst
from modbus_tk.modbus_rtu import RtuMaster
import serial

from .errors import GefranPidError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Pid(Runnable):
    """
        Establish a connection with a Gefran PID temperature controller
        Supported models (if equipped with a Modbus RTU communication interface):
            - Gefran 650
            - Gefran 1250
            - Gefran 1350
        """

    CONNECTION_SETTINGS = dict(
        bytesize=8,
        stopbits=1,
        parity='N',
        xonxoff=0
    )

    class ColumnHeadings:
        pv = 'Process value (°C)'
        sp = 'Setpoint value (°C)'
        command = 'Command'

    class Commands:
        wait = 'Wait'
        set_setpoint = "Set setpoint"

    ADDRESS_PV = 0
    ADDRESS_SP = 55
    ADDRESS_DECP = 136

    def __init__(self,
                 port: str,
                 controller_id=1,
                 baudrate: int = 19200,
                 save_log: bool = True,
                 save_path: Union[Path, str] = None,
                 log_interval: int = 5,
                 save_interval: int = 60,
                 datetime_format: str = '%Y-%m-%d %H:%M:%S',
                 **kwargs,
                 ):
        """

        :param str, port: Port on computer to connect to the PID controller. For example, 'COM5'.
        :param int, controller_id: Modbus RTU device ID of the controller. Default: 1.
        :param int, baudrate : Baudrate set in the PID controller for communication. Default: 19200.
        :param bool, save_log: Save temperature log and command log as csv files. Default: True.
        :param save_path: Path of where to save the data files to. The file names will be appended with either
            'temp_log' or 'command_log', as 2 csv files are generated.
        :param int, log_interval: Interval in seconds for retrieving status from the controller.
        :param int, save_interval: Interval in seconds for writing the temperature log as a csv file to disk.
        :param str, datetime_format: String format for the timestamps.

        """

        Runnable.__init__(self, logger=logger)

        self.modbus: RtuMaster = None
        self._lock = threading.Lock()
        self._controller_id: int = controller_id
        self.save_log: bool = save_log
        self._datetime_format: str = datetime_format
        self.log_interval: int = log_interval
        self.save_interval: int = save_interval
        self.CONNECTION_SETTINGS['port'] = port
        self.CONNECTION_SETTINGS['baudrate'] = baudrate

        self.temp_data: TemporalData = TemporalData(datetime_format=datetime_format)
        self.command_data: TemporalData = TemporalData(datetime_format=datetime_format)

        self._save_path: Path = None
        self.save_path = save_path

        self.connect()

    @property
    def datetime_format(self) -> str:
        return self._datetime_format

    @property
    def save_path(self) -> Path:
        return self._save_path

    @save_path.setter
    def save_path(self,
                  value: Union[Path, str]):
        now = datetime.now().strftime("%Y%m%d %H%M%S")
        self._save_path = value if value is not None else Path.cwd().joinpath('data/gefran')
        if type(self._save_path) != Path:
            self._save_path = Path(self._save_path)
        file_name = self._save_path.name
        temp_path = self.save_path.with_name(f'{file_name} temp log - {now}')
        command_path = self.save_path.with_name(f'{file_name} command log - {now}')
        self.temp_data.save_path = temp_path
        self.command_data.save_path = command_path
        # Change the save path if a file with that name already exists
        index = 1
        while self.command_data.save_path.exists():
            self._save_path = self._save_path.parent.joinpath(f'{file_name}_copy_{index}')
            new_file_name = self._save_path.name
            temp_path = self._save_path.with_name(f'{new_file_name} temp log')
            command_path = self._save_path.with_name(f'{new_file_name} command log')
            index += 1
            self.temp_data.save_path = temp_path
            self.command_data.save_path = command_path
        self._save_path.parent.mkdir(exist_ok=True, parents=True)  # Creates non-existing directories in the path

    def _set_up_data(self):
        start_time: str = TemporalData.now_string(self.datetime_format)
        self._set_up_temp_data(start_time=start_time)
        self._set_up_command_data(start_time=start_time)

    def connect(self):
        """
                Connect to the PID controller

        """
        try:
            if self.modbus is None:
                self.modbus = RtuMaster(
                    serial.Serial(**self.CONNECTION_SETTINGS))
                self.modbus.set_timeout(1.5)

            self.modbus.open()

            logger.info('Connected to PID controller')
            # Ensure that the serial port is closed on system exit
            atexit.register(self.disconnect)
        except Exception as e:
            logger.warning("Could not connect")
            raise GefranPidError(msg='Could not connect to the PID controller, make sure the right port was selected')

    def disconnect(self):
        """
                Close the connection with the PID controller

        """
        if self.running:
            self.stop_temp_logging()
        if self.save_log:
            self.save_csv_files()
        if self.modbus is not None:
            try:
                # TODO: implement: to safe state?
                # disconnect
                self.modbus.close()
            except Exception as e:
                logger.warning("Could not disconnect")
                raise GefranPidError(msg='Could not disconnect from the PID controller')

    @property
    def process_value(self) -> float:
        """
        PID controller process value

        :return: float, current process value of the PID controller
        """
        return self.read_process_value()

    @property
    def pv(self) -> float:
        """
        PID controller process value

        :return: float, current process value of the PID controller
        """
        return self.read_process_value()

    @property
    def setpoint(self) -> float:
        """
        PID controller setpoint

        :return:
        """
        return self.read_setpoint()

    @setpoint.setter
    def setpoint(self,
                 value,
                 ):
        self.write_setpoint(value=value)

    def set_setpoint(self,
                     value,
                     ):
        self.setpoint = value

    @property
    def sp(self) -> float:
        """
        PID controller setpoint

        :return:
        """
        return self.read_setpoint()

    @sp.setter
    def sp(self,
           value,
           ):
        self.write_setpoint(value=value)

    def read_setpoint(self) -> float:
        raw_decp = self.modbus.execute(self._controller_id, cst.READ_INPUT_REGISTERS, Pid.ADDRESS_DECP, 1)[0]
        raw_sp = self.modbus.execute(self._controller_id, cst.READ_INPUT_REGISTERS, Pid.ADDRESS_SP, 1)[0]
        return raw_sp / (10 ** raw_decp)

    def write_setpoint(self, value):
        raw_decp = self.modbus.execute(self._controller_id, cst.READ_INPUT_REGISTERS, Pid.ADDRESS_DECP, 1)[0]
        raw_sp = round(value * (10 ** raw_decp))
        self.modbus.execute(self._controller_id, cst.WRITE_MULTIPLE_REGISTERS, Pid.ADDRESS_SP, output_value=[raw_sp])
        command_str = Pid.Commands.set_setpoint + f' to {value:.2f}°C'
        self._register_command(command_str)

    def read_process_value(self) -> float:
        raw_decp = self.modbus.execute(self._controller_id, cst.READ_INPUT_REGISTERS, Pid.ADDRESS_DECP, 1)[0]
        raw_pv = self.modbus.execute(self._controller_id, cst.READ_INPUT_REGISTERS, Pid.ADDRESS_PV, 1)[0]
        return raw_pv / (10 ** raw_decp)

    def _register_command(self, command: str) -> None:
        add_data = {Pid.ColumnHeadings.command: command}
        now = datetime.now()
        self.command_data.add_data(data=add_data, t=now)
        if self.save_log is True:
            self.command_data.save_csv()

    def _set_up_temp_data(self, start_time) -> None:
        """
        For the Time column, what's in the parenthesis is the datetime format set, and the values are strings,
        but for the rest, the values are floats or datetime objects

        Example table:

        +------------------------------+----------+------------+-------------+--------------------+---------------------+
        | Time (self._datetime_format) | Time (s) | Time (min) | Time (hour) | Process value (°C) | Setpoint value (°C) |
        +------------------------------+----------+------------+-------------+--------------------+---------------------+

        :return: None
        """
        add_data = self.get_temp_data()
        self.temp_data.add_data(add_data, t=start_time)

    def _set_up_command_data(self, start_time) -> None:
        """
        For the Time column, what's in the parenthesis is the datetime format set, and the values are strings,
        but for the rest of the time columns, the values are floats

        In the command column, put select commands that are through Python to the hot plate in human readable form

        Example table:

        +------------------------------+----------+------------+-------------+-----------+
        | Time (self._datetime_format) | Time (s) | Time (min) | Time (hour) |  Command  |
        +------------------------------+----------+------------+-------------+-----------+
        | datetime_1                   |       0  |          0 |           0 | START     |
        | datetime_2                   |       30 |        0.5 |      0.0083 | COMMAND_2 |
        +------------------------------+----------+------------+-------------+-----------+

        :return: None
        """
        add_data = {Pid.ColumnHeadings.command: 'START',
                    }
        self.command_data.add_data(add_data, start_time)

    def save_csv_files(self):
        if len(self.temp_data.data != 0):
            self.temp_data.save_csv()
        self.command_data.save_csv()

    def get_temp_data(self) -> Dict[str, Union[float]]:
        """
        Return a dictionary of the important PID data:
            - process_value
            - setpoint
        :return:
        """
        pv = self.process_value
        sp = self.setpoint
        temp_data = {Pid.ColumnHeadings.pv: pv,
                     Pid.ColumnHeadings.sp: sp,
                     }
        return temp_data

    def register_state(self) -> None:
        add_data = self.get_temp_data()
        self.temp_data.add_data(add_data)
        nrows = len(self.temp_data.data)
        if nrows % math.floor(self.save_interval / self.log_interval) == 0 and self.save_log is True:
            self.temp_data.save_csv()

    def run(self):
        """function to monitor temperature in the background every second\
        """
        while self.running:
            time.sleep(self.log_interval)
            self.register_state()

    def start_temp_logging(self):
        """start the thread to monitor temperature in the background every second"""
        self.start()

    def stop_temp_logging(self):
        """stop the thread to monitor temperature in the background every second"""
        self.stop()

    def wait(self, seconds: Union[int, float]):
        command_str = Pid.Commands.wait + f' for {seconds}s'
        self._register_command(command_str)
        time.sleep(seconds)
        command_str = Pid.Commands.wait + f' end'
        self._register_command(command_str)

    def wait_until_stable(self,
                          n: int = 10,
                          tolerance: float = 1.5,
                          timeout: float = None,
                          custom_stable_func=None,
                          custom_stable_func_kargs: Dict[str, Any] = None) -> bool:
        """
        Wait until the temperature has stabilized or until the timeout number of seconds has passed.

        :param int, n: Number of logged data points that is used for determining if the temperature is stable.
        :param float, timeout: Stop the waiting period, even if no stable temperature has been obtained after this
        number of seconds. Set to None for no timeout.
        :param custom_stable_func: Provide a custom function for determining if the temperature is stable
        (fun(times: List[float], temps: List[float]) -> bool, times are expressed in seconds).
        If None is set, a built-in function will be used which checks if each of the last n data points are
        within a temperature tolerance interval (setpoint - tolerance) < data point < setpoint + tolerance).
        :param float, tolerance: Allowed temperature tolerance (in °C) for obtaining a stable temperature if the default
        stable function is used.
        :param Dict[str, Any], custom_stable_func_kargs: Dictionary that represents optional keyworded arguments
        that will be passed to the custom_stable_func, if specified.

        :return: bool: True if stable situation was obtained. False if a time-out happened.
        """
        self._register_command('Waiting for temperature to stabilize')
        start_time: datetime = datetime.now()
        was_logging = self.running  # if currently data logging in the background
        if was_logging:
            self.stop_temp_logging()
        while True:
            current_time: datetime = datetime.now()
            seconds_passed: float = (current_time - start_time).seconds

            if timeout is not None:
                if seconds_passed > timeout:
                    if was_logging:
                        self.start_temp_logging()
                    self._register_command(f'Temperature has not stabilized, wait has timed out')
                    return False

            time.sleep(self.log_interval)
            self.register_state()
            last_n_times = self.temp_data.tail(n, self.temp_data.time_s_column_heading)
            last_n_times = [float(s) for s in last_n_times]
            last_n_temperatures = self.temp_data.tail(n, self.ColumnHeadings.pv)
            if len(last_n_times) < n:
                continue
            else:
                if custom_stable_func is None:
                    stable = self.is_stable(
                        times=last_n_times,
                        temps=last_n_temperatures,
                        tolerance=tolerance,
                        setpoint=self.setpoint,
                    )
                else:
                    stable = custom_stable_func(times=last_n_times,
                                                temps=last_n_temperatures,
                                                setpoint=self.setpoint,
                                                **custom_stable_func_kargs,
                                                )
                if stable:
                    if was_logging:
                        self.start_temp_logging()
                    self._register_command(f'Temperature has stabilized')
                    return True
                else:
                    continue

    def is_stable(self, times: List[float], temps: List[float], tolerance: float, setpoint: float):
        return all(abs(temp - setpoint) < tolerance for temp in temps)
