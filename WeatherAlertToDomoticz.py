import os
import sys
import json
import logging
import requests
import time
from queue import Queue
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Optional, Tuple, List
from logging.handlers import RotatingFileHandler
import configparser
import threading

from shapely.geometry import Point, shape
import paho.mqtt.client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
import paho.mqtt.properties as properties

# =============================================================================
# Version Management
# Version 2.0.0 to 2.6.0: Base structure, polygon checks, and heartbeats
# Version 2.7.0 to 2.7.2: MQTT client in class, loop_forever threads, and unique client ID
# Version 2.7.3: Textual cleanup
# Version 2.7.4: Review implementation (ZoneInfo timezones, robust queue/finally,
#              clean Domoticz update, alarm spam prevention, and datetime comparison)
# Version 3.7.5: English translation corresponding to version 2.7.5
# =============================================================================
SCRIPT_VERSION = "3.7.5"

# =============================================================================
# ===================== Initialize Global Variables ==========================
# =============================================================================
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "WeatherAlertToDomoticz.cfg")
config.read(config_path)

SCRPT_NAME = os.path.basename(sys.argv[0])[:-3]
URL_DOMOTICZ = config['global']['URL_domoticz']
OUTPUTFILE = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), f"{SCRPT_NAME}.log")
LOG_LEVEL = config['global']['logLevel'].upper(
) if 'logLevel' in config['global'] else 'INFO'

BROKER_DOMAIN = config['MQTT_settings']['broker_domain']
CLIENT_ID = config['MQTT_settings']['client_id']
TOKEN = config['MQTT_settings']['token']
TOPIC = config['MQTT_settings']['topic']
PROTOCOL = mqtt_client.MQTTv5

API_KEY = config['dataplatform_knmi']['api_key']

AUTORISATIE_TOKEN = config['domoticz']['autorisatie_token']
DEVICE_ID = int(config['domoticz']['device_id'])
FALLBACK_LAT = float(config['domoticz']['fallback_latitude'])
FALLBACK_LON = float(config['domoticz']['fallback_longitude'])
WEEKDAYS = list(config['domoticz']['weekdays'].split(" "))

LAST_CONNECTION = time.monotonic()
File_Q: Queue[str] = Queue()

# =============================================================================
# ========================== Initialize Logging ===============================
# =============================================================================

numeric_log_level = getattr(logging, LOG_LEVEL, logging.INFO)

logging.basicConfig(
    format='%(asctime)s: %(levelname)-8s: %(funcName)-12s:  %(lineno)-4d: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=numeric_log_level,
)
logger = logging.getLogger(__name__)

fh = RotatingFileHandler(OUTPUTFILE, maxBytes=1048576, backupCount=2)
fh.setLevel(numeric_log_level)
ff = logging.Formatter(
    '%(asctime)s: %(levelname)-8s: %(funcName)-12s: %(message)s', '%Y-%m-%d %H:%M:%S')
fh.setFormatter(ff)
logger.addHandler(fh)

line = "=" * 52
text = f" {SCRPT_NAME} v{SCRIPT_VERSION} started ".center(52, '=')
logger.info(f"\n\n\n{line}\n{text}\n{line}\n\n")

del config

# =============================================================================
# ================================ Classes ====================================
# =============================================================================


class Domoticz:
    def __init__(self, url: Optional[str] = None, dev_id: Optional[int] = None) -> None:
        self.base_url = f"{URL_DOMOTICZ}/json.htm" if not url else url
        self.headers = {"Authorization": AUTORISATIE_TOKEN}
        self.id = dev_id
        self.type = 'command'

    def __get_data(self, url: str, params: Optional[dict[str, Any]] = None) -> Any:
        try:
            with requests.get(url, headers=self.headers, params=params, timeout=10) as r:
                r.raise_for_status()
                logger.debug(
                    f"\n=> Domoticz: {r.url}\n=> Status: {r.status_code}")
                return r.json()
        except Exception as err:
            logger.error(f"Error communicating with Domoticz \n{err}")
            return "ERROR"

    def status(self, dev_id: Optional[int] = None) -> Any:
        target_id = self.id if dev_id is None else dev_id
        return self.__get_data(self.base_url, {
            "type": self.type,
            "param": "getdevices",
            "rid": target_id
        })

    def update(self, dev_id: Optional[int] = None, nvalue: int = 0, svalue: str = "") -> Any:
        target_id = self.id if dev_id is None else dev_id
        return self.__get_data(self.base_url, {
            "type": self.type,
            "param": "udevice",
            "idx": target_id,
            "nvalue": nvalue,
            "svalue": svalue
        })

    def log(self, mess: str, level: int = 2) -> Any:
        return self.__get_data(self.base_url, {
            "type": self.type,
            "param": "addlogmessage",
            "message": mess,
            "level": level
        })

    def get_location(self) -> Tuple[float, float]:
        res = self.__get_data(self.base_url, {
            "type": "command",
            "param": "getsettings"
        })
        try:
            if isinstance(res, dict):
                location_dict = res.get("Location", {})
                if "Latitude" in location_dict and "Longitude" in location_dict:
                    lat = float(location_dict.get("Latitude", FALLBACK_LAT))
                    lon = float(location_dict.get("Longitude", FALLBACK_LON))
                    logger.debug(
                        f"Location successfully retrieved from Domoticz: Lat={lat}, Lon={lon}")
                    return lat, lon

        except (ValueError, TypeError, KeyError) as e:
            logger.warning(
                f"Could not parse location from Domoticz, using fallback. Error: {e}")

        logger.warning(
            f"No location found in Domoticz response, using fallback: Lat={FALLBACK_LAT}, Lon={FALLBACK_LON}")
        return FALLBACK_LAT, FALLBACK_LON


class OpenDataAPI:
    def __init__(self, api_token: str) -> None:
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1/datasets/KNMI-public-weather-warnings/versions/1.0/files"
        self.headers = {"Authorization": api_token}

    def __get_data(self, url: str, params: Optional[dict[str, Any]] = None) -> Any:
        with requests.get(url, headers=self.headers, params=params, timeout=15) as r:
            r.raise_for_status()
            return r.json()

    def get_file_url(self, temp_url: str) -> Any:
        return self.__get_data(temp_url)


class KNMIMqttClient:
    def __init__(self, broker: str, client_id: str, token: str, topic: str, file_queue: Queue) -> None:
        self.broker = broker
        self.client_id = client_id
        self.token = token
        self.topic = topic
        self.file_queue = file_queue

        self.client = mqtt_client.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            protocol=PROTOCOL,
            transport="websockets"
        )

        self.client.enable_logger(logger=logger)
        self.client.tls_set()
        self.client.username_pw_set("token", self.token)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe

    def _on_connect(self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Optional[Any] = None) -> None:
        logger.info(f"Connected to MQTT broker, client ID: {self.client_id}")
        logger.info(f"Session present: {str(flags.session_present)}")
        logger.info(f"Result code: {str(reason_code)}")
        client.subscribe(self.topic, qos=1)

    def _on_disconnect(self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Optional[Any] = None) -> None:
        logger.warning(f"MQTT connection lost. Reason: {reason_code}")

    def _on_subscribe(self, client: Any, userdata: Any, mid: int, granted_qos: Any, *other: Any) -> None:
        logger.info(f"Subscribed to MQTT topic: {self.topic}")

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        # Each incoming message or heartbeat directly refreshes the connection timer
        global LAST_CONNECTION
        LAST_CONNECTION = time.monotonic()

        try:
            load = json.loads(message.payload.decode('utf-8'))
            if 'data' in load and 'filename' in load['data']:
                if load['data']['filename'].endswith('.json'):
                    file_url = load['data']['url']
                    logger.debug(
                        f"JSON file found in MQTT message, adding to queue: {file_url}")
                    self.file_queue.put(file_url)
        except Exception as e:
            logger.error(f"Error processing MQTT message in class: {e}")

    def start(self) -> None:
        connect_properties = properties.Properties(PacketTypes.CONNECT)
        connect_properties.SessionExpiryInterval = 3600 * 24

        self.client.connect(
            host=self.broker,
            port=443,
            keepalive=60,
            clean_start=False,  # Production standard with fixed client_id
            properties=connect_properties
        )

        mqtt_thread = threading.Thread(
            target=self.client.loop_forever, daemon=True)
        mqtt_thread.start()
        logger.info("MQTT client loop_forever started in background thread.")

    def stop(self) -> None:
        self.client.disconnect()
        logger.info("MQTT client stopped.")

# =============================================================================
# ============================== Functions ====================================
# =============================================================================


def parse_GeoJSON_file(file_path: str, lat: float, lon: float) -> Tuple[int, str, str]:
    if not os.path.isfile(file_path):
        return 0, "No data available", "No data available"

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error opening JSON file: {e}")
        return 0, "Error reading data", f"Error reading data: {e}"

    metadata = data.get("metadata", {})
    if metadata.get("publication_type") == "heartbeat":
        global LAST_CONNECTION
        LAST_CONNECTION = time.monotonic()
        logger.info(
            "KNMI Heartbeat received. Connection timer refreshed in memory.")
        return 99, "Heartbeat received", "KNMI Heartbeat processed in the background."

    user_point = Point(lon, lat)
    warning_status = 0
    device_texts: List[str] = []
    log_texts: List[str] = []

    earliest_dt: Optional[datetime] = None
    latest_dt: Optional[datetime] = None

    level_mapping = {"yellow": 1, "amber": 2, "red": 3}
    warning_sets = data.get("warnings", [])

    for warning_set in warning_sets:
        phenomenon = warning_set.get(
            "phenomenon", "Weather warning").capitalize()
        parts = warning_set.get("parts", [])

        warning_applies_here = False

        for part in parts:
            area_data = part.get("area")
            if not area_data:
                continue

            try:
                polygon = shape(area_data)
                # intersects fully covers contains
                if polygon.intersects(user_point):
                    warning_applies_here = True

                    color_str = part.get(
                        "color", warning_set.get("color", "yellow")).lower()
                    current_level = level_mapping.get(color_str, 1)
                    if warning_status < current_level:
                        warning_status = current_level

                    part_start = part.get(
                        "onset", warning_set.get("onset", ""))
                    part_end = part.get(
                        "expires", warning_set.get("expires", ""))

                    if part_start:
                        p_start = datetime.fromisoformat(
                            part_start.replace("Z", "+00:00"))
                        if earliest_dt is None or p_start < earliest_dt:
                            earliest_dt = p_start

                    if part_end:
                        p_end = datetime.fromisoformat(
                            part_end.replace("Z", "+00:00"))
                        if latest_dt is None or p_end > latest_dt:
                            latest_dt = p_end

            except Exception as ex:
                logger.error(f"Error processing polygon in part area: {ex}")

        if warning_applies_here:
            texts_nl = warning_set.get("texts", {}).get("nl", {})
            headline = texts_nl.get("headline", "")
            description = texts_nl.get("description", "")
            detailed_desc = texts_nl.get("detailed_description", "")

            dev_block = f"{phenomenon}: {headline}" if headline else f"Warning for {phenomenon}"
            if dev_block not in device_texts:
                device_texts.append(dev_block)

            log_block_parts = [f"=== KNMI Weather Warning: {phenomenon} ==="]
            if headline:
                log_block_parts.append(f"Headline: {headline}")
            if description:
                log_block_parts.append(f"Description: {description}")
            if detailed_desc:
                log_block_parts.append(f"Details: {detailed_desc}")

            log_texts.append("\n".join(log_block_parts))

    time_str = ""
    if earliest_dt and latest_dt:
        try:
            # Convert to local time (Europe/Amsterdam)
            nl_tz = ZoneInfo("Europe/Amsterdam")
            start_local = earliest_dt.astimezone(nl_tz)
            end_local = latest_dt.astimezone(nl_tz)

            time_str = f"From {WEEKDAYS[start_local.weekday()]} {start_local.strftime('%H:%M')} to {WEEKDAYS[end_local.weekday()]} {end_local.strftime('%H:%M')}"
        except Exception as e:
            logger.error(f"Error formatting timezones: {e}")

    if device_texts:
        svalue_text = f"{time_str} : \n" + \
            " | ".join(device_texts) if time_str else " | ".join(device_texts)
        log_text = f"{time_str}\n" + \
            "\n".join(log_texts) if time_str else "\n".join(log_texts)
    else:
        svalue_text = "No warnings for your location"
        log_text = "No active warnings for your location."

    domoticz_nvalue = warning_status + 1 if warning_status > 0 else 1
    return int(domoticz_nvalue), str(svalue_text), str(log_text)


def process_queue_worker(file_queue: Queue, domoticz_client: Domoticz, lat: float, lon: float) -> None:
    """Worker thread that continuously reads and processes the file queue."""
    while True:
        download_url = file_queue.get()
        try:
            logger.info(
                f"Start processing file via URL: {download_url}")

            api = OpenDataAPI(api_token=API_KEY)
            response = api.get_file_url(download_url)
            temp_url = response['temporaryDownloadUrl']
            json_filename = os.path.join(os.path.dirname(
                os.path.abspath(__file__)), f"{SCRPT_NAME}.json")

            with requests.get(temp_url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(json_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            w_status, s_text, log_text = parse_GeoJSON_file(
                json_filename, lat, lon)

            if w_status == 99:
                logger.info(
                    "Received file was a heartbeat (status 99). Domoticz will not be updated.")
            else:
                domoticz_client.update(nvalue=w_status, svalue=s_text)
                domoticz_client.log(
                    f"{SCRPT_NAME} v{SCRIPT_VERSION}:\n{log_text}", level=2)
                logger.info(
                    f"Domoticz successfully updated with status nvalue={w_status}.")

        except Exception as e:
            logger.error(
                f"Error in queue worker thread during processing: {e}")
        finally:
            file_queue.task_done()


def run() -> None:
    logger.info(f"== PRODUCTION MODE v{SCRIPT_VERSION} started ==")

    domoticz = Domoticz(dev_id=DEVICE_ID)
    lat, lon = domoticz.get_location()
    logger.info(f"Active location from Domoticz -> Lat: {lat}, Lon: {lon}")

    worker_thread = threading.Thread(
        target=process_queue_worker,
        args=(File_Q, domoticz, lat, lon),
        daemon=True
    )
    worker_thread.start()

    mqtt_service = KNMIMqttClient(
        broker=BROKER_DOMAIN,
        client_id=CLIENT_ID,
        token=TOKEN,
        topic=TOPIC,
        file_queue=File_Q
    )
    mqtt_service.start()

    alarm_sent = False

    try:
        while True:
            time.sleep(10)
            elapsed = time.monotonic() - LAST_CONNECTION

            if elapsed > (25 * 3600):
                if not alarm_sent:
                    logger.error(
                        f"ALARM: No data/heartbeat received from KNMI for more than {elapsed / 3600:.1f} hours!")
                    alarm_sent = True
            else:
                alarm_sent = False

    except KeyboardInterrupt:
        logger.info("Program is being stopped manually...")
        mqtt_service.stop()

    logger.info(f"== PRODUCTION MODE v{SCRIPT_VERSION} READY ==")


if __name__ == "__main__":
    run()
