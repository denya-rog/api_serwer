from aiohttp import web
import json
import asyncio
import json
import logging
import re
import time

import motor.motor_asyncio
import config
import grequests

logging.basicConfig(level="DEBUG")
logger = logging.getLogger("server_run")


def drop_deprecated(di, depr_list):
    return {k: v for k, v in di.items() if k not in depr_list}


def clean_rows(di):
    return {(k if not k.startswith("sg_") else k[3:]): v for k, v in di.items()}


async def write_to_mongo(li_ev_clean):
    result = await db["new"].insert_many(li_ev_clean)


def clean_dom(di):
    return {k.upper(): v for k, v in di.items()}


async def handle(request):
    response_obj = {'status': 'success'}
    return web.Response(text=json.dumps(response_obj))


async def processing(request):
    li_ev_clean = []
    li_ev_to_send = []

    data = await request.read()
    message = data.decode("utf-8")
    code_obj = eval(message)
    logger.info("read request, eval")

    country_dom_dict = clean_dom(config.COUNTRY_DOM_DICT)
    error_dom = config.ERROR_DOM
    depr_event = config.DEPRECATED_EVENTS
    deprecated_fields = config.DEPRECATED_FIELDS


    if isinstance(code_obj, list):
        for i in code_obj:
            li_ev_clean.append(drop_deprecated(clean_rows(i), deprecated_fields))
            if "country" in i.keys() and i.get("event") not in depr_event:
                li_ev_to_send.append({"dom": country_dom_dict.get(i["country"], error_dom),
                                      "body": json.dumps(i, indent=4)
                                      })

    elif isinstance(code_obj, dict):
        li_ev_clean.append(drop_deprecated(clean_rows(code_obj), deprecated_fields))
        if "country" in code_obj.keys() and code_obj.get("event") not in depr_event:
            li_ev_to_send.append({"dom": country_dom_dict.get(aa["country"], error_dom),
                                  "body": json.dumps(code_obj, indent=4)
                                  }
                                 )
    logger.info("changed list to write")
    await write_to_mongo(li_ev_clean)
    rs = (grequests.post(i["dom"], data=i["body"]) for i in li_ev_to_send)
    logger.info("sent requests to specific domains")
    grequests.map(rs)
    return web.Response(text=json.dumps(True), status=200)


if __name__ == "__main__":
    logger.info("connecting to mongo")
    mongo_url = 'mongodb://admin:admin@mongodb:27017/some_db?authSource=admin'
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)
    db = client["some_db"]

    app = web.Application()
    app.router.add_get('/', handle)
    app.router.add_post('/', processing)
    app.router.add_put('/', handle)
    web.run_app(app, host="0.0.0.0", port=8080)
