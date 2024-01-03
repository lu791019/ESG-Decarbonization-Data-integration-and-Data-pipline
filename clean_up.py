import base64
import json
import re

import requests

from config import Config

URL = 'https://harbor.wistron.com/api/v2.0'
PRODUCT_NAME = 'k8sprdwhqecossot2021'
REPOSITORY_NAME = 'decarb-etl'
USERNAME = 'robot$k8sprdwhqecossot2021+gitlab-runner'
TOKEN = 'wW3VmH7dxhYe6l1g7DpSJyiCA4bYWI72'

credentials = f'{USERNAME}:{TOKEN}'
encoded_credentials = base64.b64encode(
    credentials.encode('utf-8')).decode('utf-8')
headers = {'Authorization': f'Basic {encoded_credentials}'}


def remove_list_response_by_reference(ref):
    try:
        delete_endpoint = f'{URL}/projects/{PRODUCT_NAME}/repositories/{REPOSITORY_NAME}/artifacts/{ref}'
        remove_response = requests.delete(delete_endpoint, headers=headers,
                                          verify=Config.CA_BUNDLE, timeout=10)

        print(remove_response.text)
    except requests.Timeout:
        print("Request Time out")
    except requests.RequestException as error:
        print(f"An error occurred: {error}", )


def main():
    get_endpoint = f'{URL}/projects/{PRODUCT_NAME}/repositories/{REPOSITORY_NAME}/artifacts?page_size=100'

    list_response = requests.get(
        get_endpoint, headers=headers,
        verify=Config.CA_BUNDLE, timeout=10)

    data = json.loads(list_response.text)
    data = list(filter(lambda x: x['tags'] is not None, data))

    refs = list(
        map(
            lambda x: x['digest'],
            filter(lambda x:  any(
                re.match(r'.*-dev$', tags['name']) for tags in x['tags']), data),
        )
    )[3:]
    print(refs)

    for ref in refs:
        remove_list_response_by_reference(ref)


if __name__ == "__main__":
    main()
