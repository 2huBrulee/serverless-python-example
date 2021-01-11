import json
from datetime import datetime,timezone

def main(event, context):
    now_utc = str(datetime.now(timezone.utc))

    body = {
        "currentTime": now_utc
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

if __name__ == "__main__":
    res = main('', '')
    print(res)