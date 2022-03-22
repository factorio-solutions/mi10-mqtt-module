from threading import Event

from mi10_mqtt_module import __version__
from mi10_mqtt_module import MqttClient


def test_version():
    assert __version__ == '2022.2.0+2'


def test_connection():
    client = MqttClient(host='localhost',
                        port=1883,
                        username=None,
                        password=None,
                        pill2kill=Event(),
                        topics=[],
                        module_name='test')
    client.connect()
    client.publish('test', True, qos=1)
