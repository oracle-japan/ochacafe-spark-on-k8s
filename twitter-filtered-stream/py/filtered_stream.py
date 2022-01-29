import os
import json
import datetime
import signal
import argparse
import requests
import kafka_producer

bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(rules):
    # You can adjust the rules if needed
    #sample_rules = [
    #    {"value": "dog has:images", "tag": "dog pictures"},
    #    {"value": "cat has:images -grumpy", "tag": "cat pictures"},
    #]
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))

TERM_signal = False

def get_stream(set):
    global TERM_signal

    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            ts = datetime.datetime.now().astimezone().isoformat(timespec='milliseconds')
            json_response['ts'] = ts
            print('--------')
            print(json.dumps(json_response, indent=2, sort_keys=True))
            print(json_response['data']['text'])
            kafka_producer.send(json_response['data']['id'], json.dumps(json_response, ensure_ascii=False))
        if TERM_signal:
            return

def sig_handler(signum, frame) -> None:
    print('!! TERM_signal !!')
    global TERM_signal
    TERM_signal = True

def main():
    signal.signal(signal.SIGTERM, sig_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument("--twitter-bearer-token")
    parser.add_argument("--kafka-client-type", default="kafka", help="either kafka or oci-streaming")
    parser.add_argument("--kafka-config", help="configration file for kafka client")
    parser.add_argument("--filter-rules", help="twitter filter rules in json")
    args = parser.parse_args()

    kafka_producer.init(args.kafka_config, args.kafka_client_type)

    global bearer_token
    if args.twitter_bearer_token:
        bearer_token = args.twitter_bearer_token

    rules = json.loads(args.filter_rules)
    print(rules)

    current_rules = get_rules()
    delete_all_rules(current_rules)
    set = set_rules(rules)
    get_stream(set)


if __name__ == "__main__":
    main()