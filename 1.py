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
    result = await db.test_collection.insert_many(li_ev_clean)


def clean_dom(di):
    return {k.upper(): v for k, v in di.items()}


async def handle(request):
    response_obj = {'status': 'success'}
    return web.Response(text=json.dumps(response_obj))


async def processing(request):
    li_ev_clean = []
    li_ev_to_send = []

    data = await request.read()

    # print(json.JSONDecoder(data))
    message = data.decode("utf-8")
    print(message)
    code_obj = eval(message)
    print(code_obj)
    country_dom_dict = clean_dom(config.COUNTRY_DOM_DICT)
    error_dom = config.ERROR_DOM
    depr_event = config.DEPRECATED_EVENTS
    deprecated_fields = config.DEPRECATED_FIELDS

    #if "ff" in code_obj.keys():
    #    await asyncio.sleep(15)

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


    await write_to_mongo(li_ev_clean)
    # await cl_socket(writer, ret_dict)
    rs = (grequests.post(i["dom"], data=i["body"]) for i in li_ev_to_send)
    grequests.map(rs)
    return web.Response(text=json.dumps(True), status=200)


if __name__ == "__main__":
    client = motor.motor_asyncio.AsyncIOMotorClient(host = 'mongodb', port = 27017)
    db = client.test_database

    app = web.Application()
    app.router.add_get('/', handle)
    app.router.add_post('/', processing)
    app.router.add_put('/', handle)
    web.run_app(app, host="0.0.0.0", port=8080)
