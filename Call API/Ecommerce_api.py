import requests
import json
import os
from datetime import datetime

# Create raw data folder
raw_path = r"C:/Users/Lokesh/PycharmProjects/batch-transformation/Input/"
os.makedirs(raw_path, exist_ok=True)

# API endpoints
endpoints = ["products", "users", "carts"]

for endpoint in endpoints:
    url = f"https://fakestoreapi.com/{endpoint}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # File name with timestamp
        file_name = os.path.join(raw_path, f"{endpoint}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

        # Save raw JSON
        with open(file_name, "w") as f:
            json.dump(data, f, indent=2)

        print(f" Saved {endpoint} data to {file_name}")
    else:
        print(f" Failed to fetch {endpoint}, status: {response.status_code}")
