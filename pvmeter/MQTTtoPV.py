#!/usr/bin/env python

"""
MQTT to Victron DBus bridge.

This script reads MQTT values for a PV inverter and, optionally, an EV charger,
then publishes them as Victron-compatible DBus services on Venus OS.

Why this works:
Victron does not require a physical inverter protocol for every device shown in
the GUI. Venus OS renders any service that follows the expected DBus service
name and path layout. This script emulates those services using
``VeDbusService``.

Data flow:

1. MQTT device publishes measurement/control values.
2. This script subscribes to the configured MQTT topics.
3. Values are converted into Victron DBus paths.
4. Venus OS displays the service as a PV inverter and/or EV charger.

PV service:
- service name: ``com.victronenergy.pvinverter.mqtt_pv_<device_instance>``
- key paths: ``/Ac/Power``, ``/Ac/L1/Voltage``, ``/Ac/Energy/Forward``,
  ``/Position``, ``/StatusCode``

EV charger service:
- service name: ``com.victronenergy.evcharger.mqtt_evcharger_<device_instance>``
- key paths: ``/Ac/Power``, ``/Current``, ``/MaxCurrent``, ``/SetCurrent``,
  ``/Status``, ``/Session/Energy``

Parts of this code are based on the work of Ralf Zimmermann
(mail@ralfzimmermann.de) in 2020.
The original code and its documentation can be found on:
https://github.com/RalfZim/venus.dbus-fronius-smartmeter
Used https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py
as basis for this service.
"""

"""
Runtime notes:

- Copy this directory to ``/data/pvmeter`` on Venus OS.
- Ensure the service runner in ``service/run`` is executable.
- Use ``install.sh`` / ``uninstall.sh`` to manage the runit service.
- Run this file directly for debugging.

MQTT expectations:

- PV input topics default to ``config["MQTT"]["topic"]/power``,
  ``.../voltage``, ``.../current``, ``.../frequency``, ``.../energy_180``,
  and ``.../energy_280``.
- EV charger input topics are optional and only used when the
  ``[EVCHARGER]`` section is enabled. The EV root topic can carry either a
  JSON payload or flat subtopics such as ``<ev_root>/power`` or
  ``<ev_root>/status``.

Installation quick reference:

1. Copy all files to ``/data/pvmeter`` on Venus OS.
2. Set permissions:
   - ``chmod 755 /data/pvmeter/service/run``
   - ``chmod 744 /data/pvmeter/kill_me.sh``
3. Install or uninstall:
   - ``bash -x /data/pvmeter/install.sh``
   - ``bash -x /data/pvmeter/uninstall.sh``
4. Check status:
   - ``svstat /service/pvmeter``
5. Debug with:
   - ``python /data/pvmeter/MQTTtoPV.py``

If ``paho-mqtt`` is not installed:

```bash
python -m ensurepip --upgrade
pip install paho-mqtt
```
"""
try:
  import gobject  # Python 2.x
except:
  from gi.repository import GLib as gobject # Python 3.x
import platform
import logging
import time
import sys
import json
import os
import paho.mqtt.client as mqtt
import configparser  # for config/ini file
try:
  import thread   # for daemon = True  / Python 2.x
except:
  import _thread as thread   # for daemon = True  / Python 3.x

# our own packages
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '../ext/velib_python'))
from vedbus import VeDbusService


# get values from config.ini file
try:
    config_file = (os.path.dirname(os.path.realpath(__file__))) + "/config.ini"
    if os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        if config["MQTT"]["broker_address"] == "IP_ADDR_OR_FQDN":
            print('ERROR:The "config.ini" is using invalid default values like IP_ADDR_OR_FQDN. The driver restarts in 60 seconds.')
            time.sleep(60)
            sys.exit()
    else:
        print('ERROR:The "' + config_file + '" is not found. Did you copy or rename the "config.sample.ini" to "config.ini"? The driver restarts in 60 seconds.')
        time.sleep(60)
        sys.exit()

except Exception:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    file = exception_traceback.tb_frame.f_code.co_filename
    line = exception_traceback.tb_lineno
    print(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")
    print("ERROR:The driver restarts in 60 seconds.")
    time.sleep(60)
    sys.exit()

path_UpdateIndex = '/UpdateIndex'

# Global state shared between the MQTT callbacks and the DBus services.
# The callbacks update these variables, and the DBus service classes pull from
# them when synchronizing values into Venus OS.
verbunden = 0
power = None
voltage = None
current = None
frequency = None
energy_180 = None
energy_280 = None
dbusservice = None
evdbusservice = None
ev_enabled = False
ev_topic = None
ev_root_topic = None


def _payload_to_text(payload):
    """Normalize an MQTT payload to text for JSON parsing and topic handling."""
    if isinstance(payload, bytes):
        return payload.decode('utf-8')
    return str(payload)


def _as_float(value, default=None):
    """Convert a value to float or return ``default`` when conversion fails."""
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value, default=None):
    """Convert a value to int or return ``default`` when conversion fails."""
    try:
        return int(value)
    except Exception:
        return default


def _config_option(section, option, default=None):
    """Return an INI option when present, otherwise fall back to ``default``."""
    if config.has_section(section) and config.has_option(section, option):
        return config.get(section, option)
    return default

# MQTT Abfragen:

def on_disconnect(client, userdata, rc):
    """Reconnect the MQTT client when the broker connection drops."""
    global verbunden
    print("MQTT client Got Disconnected")
    if rc != 0:
        print('Unexpected MQTT disconnection. Will auto-reconnect')

    else:
        print('rc value:' + str(rc))

    try:
        print("Trying to Reconnect")
        client.connect(host=config["MQTT"]["broker_address"], port=int(config["MQTT"]["broker_port"]))
        verbunden = 1
    except Exception as e:
        logging.exception("Fehler beim reconnecten mit Broker")
        print("Error in Retrying to Connect with Broker")
        verbunden = 0
        print(e)

def on_connect(client, userdata, flags, rc):
        """Subscribe to ``config['MQTT']['topic']/#`` and optional EV topics."""
        global verbunden
        if rc == 0:
            print("Connected to MQTT Broker!")
            verbunden = 1
            ok = client.subscribe(config["MQTT"]["topic"]+"/#", 0)
            print("subscribed to "+config["MQTT"]["topic"]+" ok="+str(ok))
            if ev_enabled and ev_root_topic:
                ok = client.subscribe(ev_root_topic + "/#", 0)
                print("subscribed to " + ev_root_topic + " ok=" + str(ok))
        else:
            print("Failed to connect, return code %d\n" % rc)


def on_message(client, userdata, msg):
    """Dispatch MQTT messages to the PV or EV DBus service."""
    try:
        global power, voltage, current, frequency, energy_180, energy_280

        topic = msg.topic
        payload_text = _payload_to_text(msg.payload)

        # EV charger topics use the dedicated root configured in
        # ``[EVCHARGER].topic``. Messages can be a JSON document on the root
        # topic itself or flat subtopics below it, for example:
        # ``power/evcharger/power``, ``power/evcharger/current``, or
        # ``power/evcharger/status``.
        if ev_enabled and evdbusservice and ev_root_topic and (
            topic == ev_root_topic or topic.startswith(ev_root_topic + "/")
        ):
            # EV charger messages are handled separately because their topic
            # structure and DBus paths differ from the PV meter service.
            evdbusservice.ingest_mqtt(topic, payload_text)
            return

        # PV meter topics are read from ``config['MQTT']['topic']``:
        # ``.../power``, ``.../voltage``, ``.../current``, ``.../frequency``,
        # ``.../energy_180``, and ``.../energy_280``.
        if topic == config["MQTT"]["topic"] + "/power":
            # PV power is inverted so generation appears as a positive value in
            # Victron instead of a negative consumption value.
            power = -1 * float(msg.payload)
        elif topic == config["MQTT"]["topic"] + "/voltage":
            # Voltage in V.
            voltage = float(msg.payload)
        elif topic == config["MQTT"]["topic"] + "/current":
            # Current is also inverted for the same reason as power.
            current = -1 * float(msg.payload)
        elif topic == config["MQTT"]["topic"] + "/frequency":
            # Frequency in Hz.
            frequency = float(msg.payload)
        elif topic == config["MQTT"]["topic"] + "/energy_180":
            # Imported energy from the grid, expressed in Wh on MQTT and
            # converted to kWh for Victron consistency.
            energy_180 = float(msg.payload) / 1000.0
        elif topic == config["MQTT"]["topic"] + "/energy_280":
            # Exported energy from the inverter, expressed in Wh on MQTT and
            # converted to kWh for Victron consistency.
            energy_280 = float(msg.payload) / 1000.0

        if dbusservice is not None:
            dbusservice._update()

    except Exception as e:
        logging.exception("Programm MQTTtoPV ist abgestuerzt. (during on_message function)")
        print(e)
        print("Im MQTTtoPV Programm ist etwas beim Auslesen der Nachrichten schief gegangen")

class DbusDummyService:
  """Victron PV inverter DBus service wrapper.

  The class registers the service name and the standard PV inverter DBus paths
  expected by Venus OS. The MQTT callbacks populate the module-level variables
  and this class mirrors those values into DBus. It is driven by the PV topic
  root configured under ``[MQTT].topic``.
  """
  def __init__(self, servicename, deviceinstance, paths, productname=config["DEFAULT"]["device_name"], connection=config["MQTT"]["connection_name"]):
    self._dbusservice = VeDbusService(servicename)
    self._paths = paths

    logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

    # Management paths identify the process and connection source inside
    # Venus OS.
    self._dbusservice.add_path('/Mgmt/ProcessName', __file__)
    self._dbusservice.add_path('/Mgmt/ProcessVersion', 'Unkown version, and running on Python ' + platform.python_version())
    self._dbusservice.add_path('/Mgmt/Connection', connection)

    # Mandatory Victron paths for a PV inverter service.
    self._dbusservice.add_path('/DeviceInstance', deviceinstance)
    self._dbusservice.add_path('/ProductId', 0xFFFF) # id assigned by Victron Support from SDM630v2.py
    self._dbusservice.add_path('/ProductName', productname)
    #self._dbusservice.add_path("/CustomName", customname)
    self._dbusservice.add_path('/FirmwareVersion', 0.1)
    #self._dbusservice.add_path('/HardwareVersion', 0)
    self._dbusservice.add_path('/Connected', 1)
    self._dbusservice.add_path('/Latency', 0)
    self._dbusservice.add_path('/ErrorCode', 0)
    self._dbusservice.add_path('/Position', int(config["PV"]["position"])) # 0=AC input 1, 1=AC output, 2=AC input 2
    self._dbusservice.add_path("/StatusCode", 0)
    # 0=Startup 0; 1=Startup 1; 2=Startup 2; 3=Startup 3; 4=Startup 4; 5=Startup 5; 6=Startup 6; 7=Running; 8=Standby; 9=Boot loading; 10=Error

    for path, settings in self._paths.items():
      self._dbusservice.add_path(
        path, settings['initial'], gettextcallback=settings['textformat'], writeable=True, onchangecallback=self._handlechangedvalue)

    # register VeDbusService after all paths where added
    self._dbusservice.register()

    # now _update ios called from on_message:
    #gobject.timeout_add(1000, self._update) # pause 1000ms before the next request

  def _update(self):
    """Push the latest MQTT values into the Victron PV inverter paths."""

    if not power is None: self._dbusservice['/Ac/Power'] = power
    if not current is None:
        self._dbusservice['/Ac/Current'] = current
    else:
        if not power is None: self._dbusservice['/Ac/Current'] = power/float(config["DEFAULT"]["voltage"])
    if not voltage is None:
        self._dbusservice['/Ac/Voltage'] = voltage
    else:
        self._dbusservice['/Ac/Voltage'] = float(config["DEFAULT"]["voltage"])
    if not energy_280 is None:
        self._dbusservice['/Ac/Energy/Forward'] = energy_280
    else:
        self._dbusservice['/Ac/Energy/Forward'] = 0
    #if not xxx is None: self._dbusservice['/Ac/Energy/Forward'] =  xxx

    if not power is None: self._dbusservice['/Ac/L1/Power'] = power
    if not current is None:
        self._dbusservice['/Ac/L1/Current'] = current
    else:
        if not power is None: self._dbusservice['/Ac/L1/Current'] = power/float(config["DEFAULT"]["voltage"])
    if not voltage is None:
        self._dbusservice['/Ac/L1/Voltage'] = voltage
    else:
        self._dbusservice['/Ac/L1/Voltage'] = float(config["DEFAULT"]["voltage"])
    if not frequency is None:
        self._dbusservice['/Ac/L1/Frequency'] = frequency
    else:
        self._dbusservice['/Ac/L1/Frequency'] = float(config["DEFAULT"]["frequency"])
    if not energy_280 is None:
        self._dbusservice['/Ac/L1/Energy/Forward'] = energy_280
    else:
        self._dbusservice['/Ac/L1/Energy/Forward'] = 0
    #if not xxx is None: self._dbusservice['/Ac/L1/Energy/Forward'] =  xxx

    # increment UpdateIndex - to show that new data is available
    index = self._dbusservice[path_UpdateIndex] + 1  # increment index
    if index > 255:   # maximum value of the index
      index = 0       # overflow from 255 to 0
    self._dbusservice[path_UpdateIndex] = index

    # is only displayed for Fronius inverters (product ID 0xA142) in GUI but displayed in VRM portal
    # if power above 10 W, set status code to 7 (running)
    # 0=Startup 0; 1=Startup 1; 2=Startup 2; 3=Startup 3; 4=Startup 4; 5=Startup 5; 6=Startup 6; 7=Running; 8=Standby; 9=Boot loading; 10=Error
    if not power is None:
        if self._dbusservice["/Ac/Power"] >= 10:
            if self._dbusservice["/StatusCode"] != 7:
                self._dbusservice["/StatusCode"] = 7
        # else set status code to 8 (standby)
        else:
            if self._dbusservice["/StatusCode"] != 8:
                self._dbusservice["/StatusCode"] = 8
    else:
        if self._dbusservice["/StatusCode"] != 8:
            self._dbusservice["/StatusCode"] = 8
    self._lastUpdate = time.time()

    return True

  def _handlechangedvalue(self, path, value):
    """Accept writes from DBus clients without rejecting the update."""
    logging.debug("someone else updated %s to %s" % (path, value))
    return True # accept the change


class EvChargerDummyService:
  """Victron EV charger DBus service wrapper.

  This service is optional and is only created when the ``[EVCHARGER]``
  section is enabled. It supports both JSON payloads and flat MQTT topic
  updates from the configured ``[EVCHARGER].topic`` root, then mirrors the
  resulting state into Victron EV charger paths.
  """
  def __init__(self, servicename, deviceinstance, productname, connection, topic_root, position, model, default_voltage, default_frequency, max_current):
    self._dbusservice = VeDbusService(servicename)
    self._topic_root = topic_root.rstrip("/")
    self._default_voltage = float(default_voltage)
    self._default_frequency = float(default_frequency)
    self._position = int(position)
    self._model = model

    self._power = None
    self._l1_power = None
    self._l2_power = None
    self._l3_power = None
    self._current = None
    self._max_current = float(max_current)
    self._set_current = float(max_current)
    self._energy_forward = None
    self._charging_time = 0
    self._session_time = 0
    self._session_energy = 0
    self._session_cost = 0
    self._auto_start = 1
    self._enable_display = 1
    self._mode = 1
    self._start_stop = 1
    self._status = None
    self._connected = 1
    self._update_index = 0

    # Standard management paths.
    self._dbusservice.add_path('/Mgmt/ProcessName', __file__)
    self._dbusservice.add_path('/Mgmt/ProcessVersion', 'Unkown version, and running on Python ' + platform.python_version())
    self._dbusservice.add_path('/Mgmt/Connection', connection)

    # Core device identity fields.
    self._dbusservice.add_path('/DeviceInstance', deviceinstance)
    self._dbusservice.add_path('/ProductId', 3800)
    self._dbusservice.add_path('/ProductName', productname)
    self._dbusservice.add_path('/FirmwareVersion', 0.1)
    self._dbusservice.add_path('/Connected', self._connected)
    self._dbusservice.add_path('/Latency', 0)
    self._dbusservice.add_path('/ErrorCode', 0)
    self._dbusservice.add_path('/Role', 'evcharger')
    self._dbusservice.add_path('/Position', self._position)
    self._dbusservice.add_path('/Model', self._model)

    # Session and control fields exposed by the Victron EV charger widget.
    self._dbusservice.add_path('/Status', 0)
    self._dbusservice.add_path('/AutoStart', self._auto_start)
    self._dbusservice.add_path('/ChargingTime', self._charging_time)
    self._dbusservice.add_path('/Session/Time', self._session_time)
    self._dbusservice.add_path('/Session/Energy', self._session_energy)
    self._dbusservice.add_path('/Session/Cost', self._session_cost)
    self._dbusservice.add_path('/EnableDisplay', self._enable_display)
    self._dbusservice.add_path('/Mode', self._mode)
    self._dbusservice.add_path('/StartStop', self._start_stop)
    self._dbusservice.add_path('/Current', self._current)
    self._dbusservice.add_path('/MaxCurrent', self._max_current)
    self._dbusservice.add_path('/SetCurrent', self._set_current)
    self._dbusservice.add_path('/Ac/Power', self._power)
    self._dbusservice.add_path('/Ac/L1/Power', self._l1_power)
    self._dbusservice.add_path('/Ac/L1/Current', self._current)
    self._dbusservice.add_path('/Ac/L1/Voltage', self._default_voltage)
    self._dbusservice.add_path('/Ac/L1/Frequency', self._default_frequency)
    self._dbusservice.add_path('/Ac/L1/Energy/Forward', self._energy_forward)
    self._dbusservice.add_path('/Ac/L2/Power', self._l2_power)
    self._dbusservice.add_path('/Ac/L2/Current', None)
    self._dbusservice.add_path('/Ac/L2/Voltage', self._default_voltage)
    self._dbusservice.add_path('/Ac/L2/Frequency', self._default_frequency)
    self._dbusservice.add_path('/Ac/L3/Power', self._l3_power)
    self._dbusservice.add_path('/Ac/L3/Current', None)
    self._dbusservice.add_path('/Ac/L3/Voltage', self._default_voltage)
    self._dbusservice.add_path('/Ac/L3/Frequency', self._default_frequency)
    self._dbusservice.add_path('/UpdateIndex', self._update_index)

    self._dbusservice.register()

  def ingest_mqtt(self, topic, payload_text):
    """Accept one MQTT message from the EV root topic or a child subtopic."""
    if topic == self._topic_root:
      try:
        payload = json.loads(payload_text)
        self._ingest_json(payload)
      except Exception:
        self._ingest_scalar(payload_text)
    elif topic.startswith(self._topic_root + "/"):
      suffix = topic[len(self._topic_root) + 1:]
      self._ingest_suffix(suffix, payload_text)

    self._update()

  def _ingest_json(self, payload):
    """Consume JSON payloads such as ``{"Ac": {"Power": ...}, "Current": ...}``."""
    ac = payload.get('Ac', {})
    if 'Power' in ac:
      self._power = _as_float(ac.get('Power'), self._power)
    if 'L1' in ac and isinstance(ac['L1'], dict):
      self._l1_power = _as_float(ac['L1'].get('Power'), self._l1_power)
    if 'L2' in ac and isinstance(ac['L2'], dict):
      self._l2_power = _as_float(ac['L2'].get('Power'), self._l2_power)
    if 'L3' in ac and isinstance(ac['L3'], dict):
      self._l3_power = _as_float(ac['L3'].get('Power'), self._l3_power)
    if 'Energy' in ac and isinstance(ac['Energy'], dict):
      self._energy_forward = _as_float(ac['Energy'].get('Forward'), self._energy_forward)

    if 'Current' in payload:
      self._current = _as_float(payload.get('Current'), self._current)
    if 'MaxCurrent' in payload:
      self._max_current = _as_float(payload.get('MaxCurrent'), self._max_current)
    if 'SetCurrent' in payload:
      self._set_current = _as_float(payload.get('SetCurrent'), self._set_current)
    if 'AutoStart' in payload:
      self._auto_start = _as_int(payload.get('AutoStart'), self._auto_start)
    if 'EnableDisplay' in payload:
      self._enable_display = _as_int(payload.get('EnableDisplay'), self._enable_display)
    if 'Mode' in payload:
      self._mode = _as_int(payload.get('Mode'), self._mode)
    if 'StartStop' in payload:
      self._start_stop = _as_int(payload.get('StartStop'), self._start_stop)
    if 'Status' in payload:
      self._status = _as_int(payload.get('Status'), self._status)
    if 'ChargingTime' in payload:
      self._charging_time = _as_int(payload.get('ChargingTime'), self._charging_time)
    if 'Session' in payload and isinstance(payload['Session'], dict):
      session = payload['Session']
      if 'Time' in session:
        self._session_time = _as_int(session.get('Time'), self._session_time)
      if 'Energy' in session:
        self._session_energy = _as_float(session.get('Energy'), self._session_energy)
      if 'Cost' in session:
        self._session_cost = _as_float(session.get('Cost'), self._session_cost)
    if 'Connected' in payload:
      self._connected = _as_int(payload.get('Connected'), self._connected)

  def _ingest_scalar(self, payload_text):
    """Fallback when the EV root topic carries a single numeric value."""
    value = _as_float(payload_text, None)
    if value is not None:
      self._power = value

  def _ingest_suffix(self, suffix, payload_text):
    """Map flat MQTT subtopics like ``power`` or ``session/energy`` into state."""
    suffix = suffix.lower()
    value_float = _as_float(payload_text, None)
    value_int = _as_int(payload_text, None)

    if suffix == 'power':
      self._power = value_float
    elif suffix == 'l1/power':
      self._l1_power = value_float
    elif suffix == 'l2/power':
      self._l2_power = value_float
    elif suffix == 'l3/power':
      self._l3_power = value_float
    elif suffix == 'current':
      self._current = value_float
    elif suffix == 'maxcurrent':
      self._max_current = value_float
    elif suffix == 'setcurrent':
      self._set_current = value_float
    elif suffix in ('ac/energy/forward', 'session/energy', 'energy_forward'):
      self._energy_forward = value_float
    elif suffix in ('chargingtime', 'session/time'):
      self._charging_time = value_int
    elif suffix == 'session/cost':
      self._session_cost = value_float
    elif suffix == 'autostart':
      self._auto_start = value_int
    elif suffix == 'enabledisplay':
      self._enable_display = value_int
    elif suffix == 'mode':
      self._mode = value_int
    elif suffix == 'startstop':
      self._start_stop = value_int
    elif suffix == 'status':
      self._status = value_int
    elif suffix == 'connected':
      self._connected = value_int

  def _infer_status(self, total_power, current_value):
    """Derive a simple charger status when MQTT does not provide one."""
    if self._status is not None:
      return self._status
    if self._start_stop == 0:
      return 0
    if (total_power is not None and total_power > 0) or (current_value is not None and current_value > 0):
      return 2
    return 1

  def _update(self):
    """Mirror the current EV charger model into Victron DBus."""
    total_power = self._power
    if total_power is None:
      # If the charger only publishes phase topics such as ``l1/power`` and
      # ``l2/power``, derive a total power value for the Victron UI.
      phase_values = [value for value in [self._l1_power, self._l2_power, self._l3_power] if value is not None]
      if phase_values:
        total_power = sum(phase_values)

    voltage = self._default_voltage
    if total_power is not None:
      self._dbusservice['/Ac/Power'] = total_power

    phase_l1 = self._l1_power if self._l1_power is not None else total_power
    phase_l2 = self._l2_power if self._l2_power is not None else 0
    phase_l3 = self._l3_power if self._l3_power is not None else 0

    if phase_l1 is not None:
      self._dbusservice['/Ac/L1/Power'] = phase_l1
    if phase_l2 is not None:
      self._dbusservice['/Ac/L2/Power'] = phase_l2
    if phase_l3 is not None:
      self._dbusservice['/Ac/L3/Power'] = phase_l3

    if self._current is not None:
      current_value = self._current
    elif total_power is not None:
      current_value = round(abs(total_power) / voltage, 2)
    else:
      current_value = None

    if current_value is not None:
      self._dbusservice['/Current'] = current_value
      self._dbusservice['/Ac/L1/Current'] = current_value if phase_l1 is not None else None
      if phase_l2 is not None:
        self._dbusservice['/Ac/L2/Current'] = round(abs(phase_l2) / voltage, 2)
      if phase_l3 is not None:
        self._dbusservice['/Ac/L3/Current'] = round(abs(phase_l3) / voltage, 2)

    self._dbusservice['/MaxCurrent'] = self._max_current
    self._dbusservice['/SetCurrent'] = self._set_current
    self._dbusservice['/AutoStart'] = self._auto_start
    self._dbusservice['/EnableDisplay'] = self._enable_display
    self._dbusservice['/Mode'] = self._mode
    self._dbusservice['/StartStop'] = self._start_stop
    self._dbusservice['/Position'] = self._position
    self._dbusservice['/Model'] = self._model
    self._dbusservice['/Connected'] = self._connected

    self._dbusservice['/Ac/L1/Voltage'] = voltage
    self._dbusservice['/Ac/L2/Voltage'] = voltage
    self._dbusservice['/Ac/L3/Voltage'] = voltage
    self._dbusservice['/Ac/L1/Frequency'] = self._default_frequency
    self._dbusservice['/Ac/L2/Frequency'] = self._default_frequency
    self._dbusservice['/Ac/L3/Frequency'] = self._default_frequency

    if self._energy_forward is not None:
      self._dbusservice['/Ac/Energy/Forward'] = self._energy_forward
      self._dbusservice['/Ac/L1/Energy/Forward'] = self._energy_forward
      self._dbusservice['/Session/Energy'] = self._energy_forward

    self._dbusservice['/ChargingTime'] = self._charging_time
    self._dbusservice['/Session/Time'] = self._session_time
    self._dbusservice['/Session/Cost'] = self._session_cost

    self._status = self._infer_status(total_power, current_value)
    self._dbusservice['/Status'] = self._status

    index = self._dbusservice['/UpdateIndex'] + 1
    if index > 255:
      index = 0
    self._dbusservice['/UpdateIndex'] = index

    return True

  def _handlechangedvalue(self, path, value):
    """Keep local EV charger state in sync when DBus clients write back."""
    if path == '/Current':
      self._current = _as_float(value, self._current)
    elif path == '/MaxCurrent':
      self._max_current = _as_float(value, self._max_current)
    elif path == '/SetCurrent':
      self._set_current = _as_float(value, self._set_current)
    elif path == '/AutoStart':
      self._auto_start = _as_int(value, self._auto_start)
    elif path == '/EnableDisplay':
      self._enable_display = _as_int(value, self._enable_display)
    elif path == '/Mode':
      self._mode = _as_int(value, self._mode)
    elif path == '/StartStop':
      self._start_stop = _as_int(value, self._start_stop)
    elif path == '/Status':
      self._status = _as_int(value, self._status)
    elif path == '/Connected':
      self._connected = _as_int(value, self._connected)
    elif path == '/Ac/Power':
      self._power = _as_float(value, self._power)
    elif path == '/Ac/L1/Power':
      self._l1_power = _as_float(value, self._l1_power)
    elif path == '/Ac/L2/Power':
      self._l2_power = _as_float(value, self._l2_power)
    elif path == '/Ac/L3/Power':
      self._l3_power = _as_float(value, self._l3_power)
    elif path == '/Ac/Energy/Forward':
      self._energy_forward = _as_float(value, self._energy_forward)
    return True

def main():
  """Create the Victron DBus services and start the MQTT event loop."""
  #logging.basicConfig(level=logging.INFO) # use .INFO for less, .DEBUG for more logging
  logging.basicConfig(
      format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
      datefmt="%Y-%m-%d %H:%M:%S",
      level=logging.INFO,
      # level=logging.DEBUG,
      handlers=[
          logging.FileHandler(f"{(os.path.dirname(os.path.realpath(__file__)))}/current.log"),
          logging.StreamHandler(),
      ],
  )
  thread.daemon = True # allow the program to quit

  from dbus.mainloop.glib import DBusGMainLoop
  # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
  DBusGMainLoop(set_as_default=True)

  # formatting
  def _kwh(p, v): return (str(round(v, 2)) + 'kWh')
  def _wh(p, v): return (str(round(v, 2)) + 'Wh')
  def _a(p, v): return (str(round(v, 2)) + 'A')
  def _w(p, v): return (str(int(round(v, 0))) + 'W')
  def _v(p, v): return (str(round(v, 1)) + 'V')
  def _hz(p, v): return (str(round(v, 2)) + 'Hz')
  def _n(p, v): return str("%i" % v)
  def _num(p, v): return str(round(v, 2))

  global dbusservice, evdbusservice, ev_enabled, ev_topic, ev_root_topic

  # Register the PV inverter service unconditionally so existing setups keep
  # working even when the EV charger feature is not enabled.
  dbusservice = DbusDummyService(
    servicename='com.victronenergy.pvinverter.mqtt_pv_' + str(config["DEFAULT"]["device_instance"]),
    deviceinstance=int(config["DEFAULT"]["device_instance"]),
    #customname=config["DEFAULT"]["device_name"],
    paths={
      '/Ac/Power': {'initial': None, 'textformat': _w},
      '/Ac/Current': {'initial': None, 'textformat': _a},
      '/Ac/Voltage': {'initial': None, 'textformat': _v},
      '/Ac/Energy/Forward': {'initial': None, 'textformat': _kwh},
      '/Ac/MaxPower': {'initial': int(config["PV"]["max"]), 'textformat': _w},
      '/Ac/Position': {'initial': int(config["PV"]["position"]), 'textformat': _n},
      '/Ac/StatusCode': {'initial': 0, 'textformat': _n},
      '/UpdateIndex': {'initial': 0, 'textformat': _n},
      '/Ac/L1/Power': {'initial': None, 'textformat': _w},
      '/Ac/L1/Current': {'initial': None, 'textformat': _a},
      '/Ac/L1/Voltage': {'initial': None, 'textformat': _v},
      '/Ac/L1/Frequency': {'initial': None, 'textformat': _hz},
      '/Ac/L1/Energy/Forward': {'initial': None, 'textformat': _kwh},
      #'/Ac/L2/Power': {'initial': None, 'textformat': _w},
      #'/Ac/L2/Current': {'initial': None, 'textformat': _a},
      #'/Ac/L2/Voltage': {'initial': None, 'textformat': _v},
      #'/Ac/L2/Frequency': {'initial': None, 'textformat': _hz},
      #'/Ac/L2/Energy/Forward': {'initial': None, 'textformat': _kwh},
      #'/Ac/L3/Power': {'initial': None, 'textformat': _w},
      #'/Ac/L3/Current': {'initial': None, 'textformat': _a},
      #'/Ac/L3/Voltage': {'initial': None, 'textformat': _v},
      #'/Ac/L3/Frequency': {'initial': None, 'textformat': _hz},
      #'/Ac/L3/Energy/Forward': {'initial': None, 'textformat': _kwh},
    })

  # The EV charger service is optional and controlled by config.ini.
  ev_enabled = config.has_section("EVCHARGER") and config.getboolean("EVCHARGER", "enabled", fallback=False)
  if ev_enabled:
    ev_root_topic = _config_option("EVCHARGER", "topic", "").rstrip("/")
    if not ev_root_topic:
      logging.warning("EVCHARGER is enabled but topic is empty. Skipping EV charger service.")
      ev_enabled = False
    else:
      ev_topic = ev_root_topic
      # This service uses a separate VRM instance so it can coexist with the
      # PV inverter without a bus name collision.
      evdbusservice = EvChargerDummyService(
        servicename='com.victronenergy.evcharger.mqtt_evcharger_' + str(_config_option("EVCHARGER", "device_instance", "11")),
        deviceinstance=_as_int(_config_option("EVCHARGER", "device_instance", "11"), 11),
        productname=_config_option("EVCHARGER", "device_name", "MQTT EV Charger"),
        connection=_config_option("EVCHARGER", "connection_name", config["MQTT"]["connection_name"]),
        topic_root=ev_root_topic,
        position=_as_int(_config_option("EVCHARGER", "position", "0"), 0),
        model=_config_option("EVCHARGER", "model", "MQTT EV Charger"),
        default_voltage=_as_float(_config_option("EVCHARGER", "voltage", config["DEFAULT"]["voltage"]), float(config["DEFAULT"]["voltage"])),
        default_frequency=_as_float(_config_option("EVCHARGER", "frequency", config["DEFAULT"]["frequency"]), float(config["DEFAULT"]["frequency"])),
        max_current=_as_float(_config_option("EVCHARGER", "max_current", "32"), 32),
      )

  logging.info('Connected to dbus, and switching over to gobject.MainLoop() (= event based)')

  # MQTT client setup stays inside main so the DBus services exist before the
  # first MQTT message can be processed.
  client = mqtt.Client(config["MQTT"]["mqtt_name"]) # create new instance
  if ((config["MQTT"]["broker_user"] != "") and (config["MQTT"]["broker_password"] != "")):
      client.username_pw_set(config["MQTT"]["broker_user"], config["MQTT"]["broker_password"])
  client.on_disconnect = on_disconnect
  client.on_connect = on_connect
  client.on_message = on_message
  client.connect(host=config["MQTT"]["broker_address"], port=int(config["MQTT"]["broker_port"]))  # connect to broker
  client.loop_start()

  mainloop = gobject.MainLoop()
  mainloop.run()

if __name__ == "__main__":
  main()
