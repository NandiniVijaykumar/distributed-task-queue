import requests

# Submit 5 low-priority jobs
for i in range(1, 6):
    response = requests.post(
        "http://localhost:8000/jobs",
        json={
            "type": "long_task",
            "payload": {
                "file": f"low{i}.png"
            },
            "priority": "low"
        }
    )
    print(response.json())

    
# Submit 5 high-priority jobs
for i in range(1, 6):
    response = requests.post(
        "http://localhost:8000/jobs",
        json={
            "type": "long_task",
            "payload": {
                "file": f"high{i}.png"
            },
            "priority": "high"
        }
    )
    print(response.json())