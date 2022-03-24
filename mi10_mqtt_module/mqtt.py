import json
import logging
from typing import List

from bson import json_util
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTMessage

from .utils import RepeatedTimer

logger = logging.getLogger('mi10-mqtt-module' + '.mqtt.py')


class MqttClient:
    def __init__(self,
                 host: str,
                 port: int = 1883,
                 username: str = None,
                 password: str = None,
                 topics: list = List,
                 module_name: str = None,
                 bind_address: str = '0.0.0.0',
                 presence_frequency: int = 10,
                 run_presence: bool = True,
                 presence_topic_name: str = 'mqtt_client') -> None:
        super().__init__()
        self.host = host
        self.port = port
        self.bind_address = bind_address
        self.module_type = module_name
        self._client = mqtt.Client(self.module_type)
        self._topics = topics
        self.presence_frequency = presence_frequency
        self.run_presence = run_presence
        self.presence_topic_name = presence_topic_name
        self.rt = None
        if username is not None and password is not None:
            self._client.username_pw_set(username=username,
                                         password=password)

    def connect(self) -> None:
        self.__setup_connection()
        self.start()

    def start(self) -> None:
        if self.run_presence:
            self.rt = RepeatedTimer(self.presence_frequency,
                                    self.publish,
                                    self.presence_topic_name,
                                    {'type': self.module_type})
        self._client.loop_start()

    def stop(self) -> None:
        self._client.loop_stop()
        if self.run_presence:
            self.rt.stop()
        self._client.disconnect()

    def __setup_connection(self) -> None:
        self._client.on_connect = self._on_connect
        self._client.connect(host=self.host, port=self.port, bind_address=self.bind_address)

    def _on_end(self, client, userdata, message: MQTTMessage) -> None:
        logger.debug(f"Received message: {str(message.payload)} on topic: {message.topic} with QoS: {str(message.qos)}")
        self._client.disconnect()

    def publish(self, topic: str, response: object, qos: int = 0) -> None:
        logger.debug(f'Publishing to topic: {topic} message: {response}')
        self._client.publish(topic, json_util.dumps(response), qos=qos)

    def _on_connect(self, client, userdata, flags, rc) -> None:
        logger.info(f'Client connected to host {self.host} on port: {self.port}')
        for topic in self._topics:
            if len(topic) > 1:
                self._client.message_callback_add(topic[0], topic[1])
        self._client.subscribe([(topic[0], 2) for topic in self._topics])
        self.__is_present()

    def __is_present(self) -> None:
        self._client.subscribe(self.presence_topic_name)
        self._client.publish(self.presence_topic_name, json_util.dumps({"type": self.module_type}))

    def _on_error(self) -> None:
        pass
