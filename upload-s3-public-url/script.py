from botocore.config import Config
from multiprocessing import Pool
from requests.adapters import HTTPAdapter, Retry
from logger_tt import setup_logging, logger

import boto3
import logging
import os
import random
import re
import requests
import time

# environment variable
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', default='ap-southeast-3')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# global variable
BATCH_SIZE = int(os.getenv('BATCH_SIZE', default=1000))
MAX_RETRY = int(os.getenv('MAX_RETRY', default=5))
MAX_TIMEOUT = int(os.getenv('MAX_TIMEOUT', default=5))  # in seconds
NUM_OF_CHUNKSIZE = int(os.getenv('NUM_OF_CHUNKSIZE', default=4))  # number of worker pools per batch
NUM_OF_PROCESSES = int(os.getenv('NUM_OF_PROCESS', default=4))  # number of processor core
SLEEP_INTERVAL = float(os.getenv('SLEEP_INTERVAL', default=2))  # in minutes
URLS = os.getenv('URLS').split(',')

if None in (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, URLS):
    raise EnvironmentError("Some environment variable(s) is not set")


# filename
current_iteration_filename = "current-iteration.txt"
endpoints_filename = "endpoints.txt"
failed_url_filename = "failed-url.txt"


# config
config = Config(
    retries={
        'max_attempts': 5,
        'mode': 'standard'
    }
)

setup_logging(use_multiprocessing=True,
              suppress_level_below=logging.INFO,
              config_path='log-config.yaml',
              capture_print=True,
              guess_level=True
              )


def parse_url(message):
    url = re.findall(
        'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', message)
    return url[0]


def retry_session(retries=5, backoff_factor=0.3, session=None, status_forcelist=[404, 500, 502, 503, 504]):
    '''
    Retry mechanism
     - {backoff factor} * (2 ** ({number of total retries} - 1))
    '''
    session = session or requests.Session()
    retries = Retry(
        connect=retries,
        total=retries,
        backoff_factor=backoff_factor,
        allowed_methods=["HEAD", "GET"],
        status_forcelist=status_forcelist
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    return session


def sleep(minutes=5):
    logger.info(f'Sleeping for {minutes} minutes')
    time.sleep(minutes * 60)


def upload_to_s3(endpoint):
    try:
        session = retry_session(retries=MAX_RETRY)

        endpoint = endpoint.strip('/')
        url = random.choice(URLS)
        response = session.get(f"{url}/{endpoint}",
                               stream=True, timeout=MAX_TIMEOUT)
        key = endpoint  # endpoint as name of the object

        # raise error if file is not found
        if response.status_code == 404:
            response.raise_for_status()

        content_type = response.headers['Content-Type']

        s3 = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                          region_name=AWS_REGION,
                          config=config)

        s3.upload_fileobj(
            response.raw, BUCKET_NAME, key,
            ExtraArgs={'ContentType': content_type}
        )
        logger.info(f"Finished upload for '{key}'")

    except requests.exceptions.HTTPError as e:
        clean_url = parse_url(str(e))
        logger.info(
            f"HTTP Error on object/file {clean_url}")
        with open(failed_url_filename, "a") as file_object:
            file_object.write(f"{clean_url}\n")
    except requests.exceptions.ConnectionError as e:
        logger.info(
            f"Connection Error on object/file {e}")
        with open(failed_url_filename, "a") as file_object:
            file_object.write(f"{e}\n")

    except requests.exceptions.RetryError as e:
        logger.info(
            f"Retry Error on object/file {e}")
        with open(failed_url_filename, "a") as file_object:
            file_object.write(f"{e}\n")


if __name__ == '__main__':
    print(URLS)
    # retrive list of endpoints as array/lists
    with open(endpoints_filename, 'r') as file:
        endpoints = [endpoint.strip('\n') for endpoint in file]

    first, last = 0, len(endpoints)

    if os.path.isfile(current_iteration_filename):
        with open(current_iteration_filename, 'r') as file:
            first = int(file.read())

    try:
        for index in range(first, last, BATCH_SIZE):
            # write the current batch index to file
            with open(current_iteration_filename, 'w') as f:
                logger.info(
                    f"Write the current iteration to {current_iteration_filename}")
                f.write(f"{index}")
            with Pool(processes=NUM_OF_PROCESSES) as pool:
                pool.map(
                    upload_to_s3, endpoints[index:index+BATCH_SIZE], chunksize=NUM_OF_CHUNKSIZE)
            logger.info(f"Current Batch(es): {index+1} - {index+BATCH_SIZE}")
            sleep(SLEEP_INTERVAL)
        logger.info(f"Migration is completed successfully")
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
    except Exception as e:
        print(e)
        pool.terminate()
    finally:
        logger.info("Pool worker is closed")
        pool.close()
