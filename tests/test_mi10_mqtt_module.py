
from mi10_mqtt_module import __version__
from mi10_mqtt_module import MqttClient


def test_version():
    assert __version__ == '2022.3.0'
    
def test_connection():
    client = MqttClient(host='10.35.16.77',
                        port=1883,
                        username=None,
                        password=None,
                        topics=[['mod/detection/status']],
                        module_name='test')
    client.connect()
    client.publish('mod/detection/status', True, qos=1)
