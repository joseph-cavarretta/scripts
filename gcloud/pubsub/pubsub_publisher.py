import json
from concurrent import futures
from datetime import datetime
from google.cloud import pubsub_v1
from random import randint
from random import choice

PROJECT_ID = "test-project"
TOPIC_ID = "strava_activities"

# init Publisher Client
publisher = pubsub_v1.PublisherClient()
# set publisher path to project and topic id
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
messages = []

def get_callback(publish_future, data):
    def callback(publish_future):
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

def create_random_message():
    id_ = randint(10000,99999)
    start_date_local = str(datetime.utcnow())
    type_ = choice(['Run', 'Ride', 'WeightTraining', 'BackcountrySki'])

    message_json = {
            'id': id_,
            'start_date_local': start_date_local,
            'type': type_
            }
    return message_json

if __name__ == '__main__':
    for i in range(10):
        message_json = create_random_message()
        data = json.dumps(message_json) # converts dict to a json string
        publish_future = publisher.publish(topic_path, data.encode("utf-8"))
        publish_future.add_done_callback(get_callback(publish_future, data))
        messages.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(messages, return_when=futures.ALL_COMPLETED)
    print(f"Published messages with error handler to {topic_path}.")
