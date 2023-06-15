import datetime
import logging
from .download_articles_demo_external import demo_main
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    demo_main()
    logging.info('Python timer trigger function ran at %s', utc_timestamp)

