import requests

for i in range(1, 10):
    response = requests.post(
        "http://localhost:8000/jobs",
        json={
            "type": "resize_image",
            "payload": {
                "file": f"test{i}.png"
            }
        }
    )

    print(response.json())
