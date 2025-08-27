import requests
import json
import os
from datetime import datetime

# Base raw data directory
raw_data_dir = "/Users/niranjankashyap/PycharmProjects/Batch-Transformation/data/raw"

# API endpoints
endpoints = ["products", "users", "carts"]

for endpoint in endpoints:
    url = f"https://fakestoreapi.com/{endpoint}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # Create subfolder for each endpoint
        endpoint_dir = os.path.join(raw_data_dir, endpoint)
        os.makedirs(endpoint_dir, exist_ok=True)  # ensures subfolder exists

        # File name with timestamp
        file_name = os.path.join(
            endpoint_dir, f"{endpoint}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        # Save raw JSON
        with open(file_name, "w") as f:
            json.dump(data, f, indent=2)

        print(f"✅ Saved {endpoint} data to {file_name}")
    else:
        print(f"❌ Failed to fetch {endpoint}, status: {response.status_code}")
