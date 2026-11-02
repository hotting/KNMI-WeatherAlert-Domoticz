# WeatherAlertToDomoticz

A modern, robust, and future-proof Python script to integrate official KNMI (Royal Netherlands Meteorological Institute) weather warnings into Domoticz using the official **KNMI Data Platform** (MQTT v5 & GeoJSON).

## 🚀 Summary / Introduction

As older KNMI RSS feeds and XML structures are being phased out, this script provides a modern alternative. Instead of broad provincial regions, it performs an **exact spatial polygon check** using the `shapely` library based on the GPS coordinates of your own Domoticz installation. 

### Key Features:
* **MQTT v5 over WebSockets (TLS):** Connects securely to the KNMI Notification Service using *Session Expiry Intervals* to ensure missed warnings are replayed after a temporary disconnect.
* **GeoJSON & Spatial Filtering:** Accurate point-in-polygon checks (`shapely`) to determine if a weather warning actually applies to your location.
* **Asynchronous Architecture:** Decoupled background thread (`loop_forever`) for network communication and a thread-safe `Queue` worker for file processing, ensuring Domoticz or the MQTT loop never blocks.
* **Timezone Aware:** Automatic conversion of UTC timestamps to local Dutch time (`Europe/Amsterdam`) including correct weekday formatting.
* **Robust Monitoring:** Built-in heartbeat monitoring and connection checks to handle disconnects gracefully without flooding your logs.

---

## 📦 Prerequisites & Installation Tips

### 1. Requirements
* **Domoticz** (with a Dummy Text device created to display warnings).
* **Python 3.9 or higher** installed on your system (tested on Python 3.13 / Raspberry Pi Linux).

### 2. Required Python Libraries
The script relies on a few essential external Python libraries for MQTT communication, spatial geometry, and HTTP requests. You can install them easily via pip:

```bash
pip install shapely paho-mqtt requests
```

---


## ⚙️ Configuration Guide (WeatherAlertToDomoticz.cfg)

### [global]
+ Waiting time between script executions in seconds (default: 300 / 5 min)
  
  `wait_execution = 300`

+ The URL for your Domoticz instance (include your specific IP and port)
  
  `URL_domoticz = http://xxx.xxx.xxx.xxx.:8081`

+ Log path: Full path to the log directory without the filename. 
Leave empty for the current script directory. (Default on Raspberry Pi: /var/log/)

  `logPath = /var/log/`     

+ Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL

  `logLevel = INFO`

### [MQTT_settings]
+ Official KNMI MQTT broker domain

  `broker_domain = mqtt.dataplatform.knmi.nl`

+ Static Client ID used to maintain session state on the broker so missed events can be replayed
Generate a unique UUID version 4 via: [https://www.uuidgenerator.net/version4](https://www.uuidgenerator.net/version4)

  `client_id = your-unique-uuid-v4-here`

+ Your personal KNMI Notification Service access token.
Obtain via: [https://developer.dataplatform.knmi.nl/notification-service#access](https://developer.dataplatform.knmi.nl/notification-service#access)

  `token = your-knmi-notification-token-here`
 
+ Subscription topic for KNMI public weather warnings

  `topic = dataplatform/file/v1/KNMI-public-weather-warnings/1.0/#`

+ Protocol used by mqtt client. Version 3.1.1 also supported

  `PROTOCOL = mqtt_client.MQTTv5`

### [dataplatform_knmi]
+ Your personal KNMI Open Data API key.
Obtain via: [https://developer.dataplatform.knmi.nl/open-data-api#token](https://developer.dataplatform.knmi.nl/open-data-api#token)

  `api_key = your-knmi-open-data-api-key-here`

### [domoticz]
+ Domoticz authorization token (Base64 encoded).
Format: user:password -> Basic <base64>
Example for user 'domoticz' and password 'class' -> Basic ZG9tb3RpY3o6Y2xhc3M=
See: [https://www.domoticz.com/wiki/Domoticz_API/JSON_URL%27s#Authorization](https://www.domoticz.com/wiki/Domoticz_API/JSON_URL%27s#Authorization)

  `autorisatie_token = Basic ZG9tb3RpY3o6Y2xhc3M=`

+ The device IDX number of your Dummy Text device in Domoticz

  `device_id = 999`

+ Fallback coordinates somewhere in the Netherlands. 
These are used as a fallback and will automatically be overwritten by your actual Domoticz location settings.

  `fallback_latitude = 52.0000`

  `fallback_longitude = 5.0000`

+ 7 localized names of the weekdays separated by spaces, starting with Monday.

  `weekdays = ma di wo do vr za zo`

## 🚀 Running the Script
To run the script manually or test it:

```Bash
python3 WeatherAlertToDomoticz.py
```

### Production / Service Setup (Raspberry Pi)
For 24/7 background execution on a Raspberry Pi, it is recommended to manage the script using a process manager like PM2 or a systemd service file.

## 📄 License
This project is open-source and available under the terms of the MIT License.