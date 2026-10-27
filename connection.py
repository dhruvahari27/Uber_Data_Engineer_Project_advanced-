import json
import logging
import os
from typing import Union, List, Dict, Any
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Pulling Data Generator Function
from data import generate_uber_ride_confirmation

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUBNAME = os.getenv("EVENT_HUBNAME", "ubertopic")


def send_to_event_hub(ride_data: Union[Dict[str, Any], List[Dict[str, Any]]] = None) -> bool:
    """
    Sends one or multiple ride confirmation events to Azure Event Hub.
    
    Args:
        ride_data: A single ride dictionary or a list of ride dictionaries.
        
    Returns:
        bool: True if successfully sent, False otherwise.
    """
    if not CONNECTION_STRING:
        logger.warning(
            "CONNECTION_STRING is not set in environment or .env file. "
            "Please configure Azure Event Hub credentials to stream live data."
        )
        return False

    if ride_data is None:
        ride_data = generate_uber_ride_confirmation()

    # Ensure ride_data is a list for uniform batch handling
    rides = [ride_data] if isinstance(ride_data, dict) else ride_data

    try:
        with EventHubProducerClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            eventhub_name=EVENT_HUBNAME
        ) as producer:
            event_batch = producer.create_batch()
            for r in rides:
                event_batch.add(EventData(json.dumps(r)))
            producer.send_batch(event_batch)

        logger.info(f"Successfully sent {len(rides)} event(s) to Event Hub: {EVENT_HUBNAME}")
        return True

    except Exception as e:
        logger.error(f"Error sending data to Event Hub: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    print("=" * 80)
    print("SAMPLE UBER RIDE CONFIRMATION EVENT")
    print("=" * 80)
    sample_ride = generate_uber_ride_confirmation()
    print(json.dumps(sample_ride, indent=2))

    print("\n" + "=" * 80)
    print("SENDING SAMPLE RIDE EVENT TO EVENT HUB")
    print("=" * 80)
    success = send_to_event_hub(sample_ride)
    print(f"Status: {'Success' if success else 'Skipped/Failed (Check credentials)'}")
