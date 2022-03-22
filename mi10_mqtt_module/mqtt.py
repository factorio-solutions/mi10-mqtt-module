import configparser
import json
import logging
from threading import Event

from bson import json_util
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTMessage

from .utils import RepeatedTimer

logger = logging.getLogger('mi10-mqtt-module' + '.mqtt.py')


class MqttClient:
    def __init__(self, host: str, port: int, username: str, password: str, pill2kill: Event, topics: list,
                 module_name: str = 'mod-module',
                 bind_address: str = '0.0.0.0', presence_frequency: int = 10):
        super().__init__()
        self.host = host
        self.pill2kill = pill2kill
        self.port = port
        self.bind_address = bind_address
        self.module_type = module_name
        self._client = mqtt.Client(self.module_type)
        self._topics = [['mod/discovery/init', self._on_init], ] + topics
        self.rt = None
        self.presence_frequency = presence_frequency
        if username is not None and password is not None:
            self._client.username_pw_set(username=username,
                                         password=password)

    def connect(self):
        self.__setup_connection()
        self.start()

    def start(self):
        self.rt = RepeatedTimer(self.presence_frequency, self.publish, 'mod/presence', {'type': self.module_type})
        self._client.loop_start()

    def stop(self):
        self._client.loop_stop()
        self.rt.stop()
        self._client.disconnect()

    def run(self) -> None:
        while not self.pill2kill.wait(0):
            self._client.loop()

    def __setup_connection(self):
        self._client.on_connect = self._on_connect
        self._client.connect(host=self.host, port=self.port, bind_address=self.bind_address)

    def _on_end(self, client, userdata, message: MQTTMessage):
        logger.debug(f"Received message: {str(message.payload)} on topic: {message.topic} with QoS: {str(message.qos)}")
        self._client.disconnect()
        self.pill2kill.set()

    def _on_init(self, client, userdata, message: MQTTMessage):
        logger.debug(f"Received message: {str(message.payload)} on topic: {message.topic} with QoS: {str(message.qos)}")
        self.publish(topic='mod/discovery/init-response', response={'loaded': True})

    def publish(self, topic, response, qos=0):
        logger.debug(f'Publishing to topic: {topic} message: {response}')
        self._client.publish(topic, json_util.dumps(response), qos=qos)

    def publish_queue(self, queue):
        self._client.publish('mod/discovery/data-output', json.dumps(queue))

    def _on_connect(self, client, userdata, flags, rc):
        logger.info(f'Client connected to host {self.host} on port: {self.port}')
        for topic in self._topics:
            if len(topic) > 1:
                self._client.message_callback_add(topic[0], topic[1])
            self._client.subscribe(topic[0])
        # self.__is_present()

    def __is_present(self):
        self._client.subscribe('mod/presence')
        self._client.publish('mod/presence', json_util.dumps({"type": self.module_type}))

    def _on_error(self) -> None:
        pass
