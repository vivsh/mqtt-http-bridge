
import requests


def dispatch(i):
    response = requests.post("http://127.0.0.1:9093/publish/", json={
        "topic": "performers.__all__",
        "message": "Hello %s" % i,
        "qos": 1
    })
    print(response.text)
    response.raise_for_status()
    result = response.json()
    print(result)


def main():
    for i in range(100):
        dispatch(i)

if __name__ == '__main__':
    main()