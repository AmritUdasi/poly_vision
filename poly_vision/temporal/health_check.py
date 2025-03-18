import asyncio

import requests
from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/health")
def health_check():
    """
    Endpoint to perform a health check.

    This endpoint sends a request to Google and returns the status of the response.

    Returns:
        dict: JSON response indicating the health status.
    """
    response = requests.get("http://www.google.com")
    if response.status_code == 200:
        return jsonify({"status": True})

    return jsonify({"status": False})


async def run_app():
    """
    Function to run the Flask app.

    Starts the Flask app on host '0.0.0.0' and port 5000.
    """
    # Start the Flask app
    app.run(host="0.0.0.0", port=5000)


async def main():
    """
    Main coroutine function.

    Starts the Flask app and Temporal worker concurrently.
    """
    # Start the Flask app and Temporal worker concurrently
    await asyncio.gather(run_app())


if __name__ == "__main__":
    asyncio.run(main())
