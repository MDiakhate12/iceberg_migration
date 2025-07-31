#!/usr/bin/env python
# -*- coding: utf-8 -*-
import boto3
import botocore
import configparser
import io
import logging
import os
from datetime import datetime
from typing import List
from dataclasses import dataclass, fields
logging.basicConfig(level=logging.INFO)
logging.info("lambda-snowlake-check-time")
logger = logging.getLogger()
session = None
local_mode = None
force_exec = None


@dataclass
class TimeCheck:
    FIRSTDAY = "isFirstDayOfMonth"
    LASTDAY = "isLastDayOfMonth"
    WEEKEND = "isWeekEnd"
    AFTERNOON = "isAfternoon"
    WORKINGHOUR = "isWorkingHours"

    @classmethod
    def get_all(self) -> list:
        return [f.default for f in fields(TimeCheck)]


def check(types: List[TimeCheck]):
    now = datetime.now()
    result = {}
    for type in types:
        if type == TimeCheck.FIRSTDAY:
            result[type] = now.day == 1
        if type == TimeCheck.LASTDAY:
            next_day = now + datetime.timedelta(days=1)
            result[type] = next_day.day == 1
        if type == TimeCheck.WEEKEND:
            result[type] = now.weekday >= 5
        if type == TimeCheck.AFTERNOON:
            result[type] = now.hour > 12
        if type == TimeCheck.WORKINGHOUR:
            result[type] = now.hour >= 9 and now.hour <= 18

    logging.info(f"Evaluating date '{now}' for time checks : {types}")

    return result


def main(event, context):
    logger.info(f"Event : {event}")
    logger.info(f"Context : {context}")
    logger.info(f"Environnement : {env}")

    time_checks = event['types']
    result = check(time_checks)

    logger.info(f"Event in parameter: {event}")

    return result


if __name__ == "__main__":
    logging.warning("!! Execution en local !!")
    if os.getenv("FORCE_EXEC_LAMBDA") == 'true':
        logging.warning("!! Execution en mode force !!")

    event = {
        'types': [
                    TimeCheck.FIRSTDAY,
                    TimeCheck.AFTERNOON
                ]
    }

    main(event, None)

else:
    logging.info("Execution sur AWS Lambda !")
    buildinfo = "buildinfo.properties"

    env = os.getenv('env')

    session = boto3.Session()
