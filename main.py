
import asyncio
import signal
import traceback

from aiohttp import web
import json
from gmqtt import Client as MQTTClient

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# MQTT_HOST = "62.210.137.178"
MQTT_HOST = "127.0.0.1"

HTTP_PORT = 9093

STOP = asyncio.Event()

mqtt_client = MQTTClient("mqtt-bridge", clean_session=True)


def on_connect(client, flags, rc, properties):
    print('Connected')


async def on_request(request):
    payload = await request.json()
    try:
        mqtt_client.publish(
            payload['topic'],
            payload['message'],
            qos=payload.get("qos", 1)
        )
    except Exception as ex:
        status = 400
        response = {"error": str(ex), "traceback": traceback.format_exc()}
    else:
        status = 200
        response = {"result": True}
    return web.Response(text=json.dumps(response), content_type="application/json", status=status)


def on_exit(*args):
    STOP.set()
    asyncio.get_event_loop().stop()


async def start_mqtt_client(host):
    mqtt_client.on_connect = on_connect
    await mqtt_client.connect(host)
    await STOP.wait()
    await mqtt_client.disconnect()


def start_web_app(port):
    app = web.Application()
    app.add_routes([
        web.post('/publish/', on_request),
        web.post('/bridge/publish/', on_request)
    ])
    web.run_app(app, port=port)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    loop.add_signal_handler(signal.SIGINT, on_exit)
    loop.add_signal_handler(signal.SIGTERM, on_exit)

    loop.create_task(start_mqtt_client(MQTT_HOST))

    start_web_app(HTTP_PORT)
