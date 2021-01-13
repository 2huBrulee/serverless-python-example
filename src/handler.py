import json
from functions import local_lambda

def main(event, context):
    local_lambda()

    body = {
        "currentTime": 'done'
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

if __name__ == "__main__":
    res = main('', '')
    print(res)