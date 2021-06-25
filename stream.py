import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import datetime
import asyncio
import config
import cryptowatch as cw
import time
from google.protobuf.json_format import MessageToJson
import json
import uuid

HOST = config.settings["host"]
MASTER_KEY = config.settings["master_key"]
DATABASE_ID = config.settings["database_id"]
CONTAINER_ID = config.settings["container_id"]

# Set your API Key
# cw.api_key = "123"

container = None

# What to do on each trade update
def handle_trades_update(trade_update):
    """
    trade_update follows Cryptowatch protocol buffer format:
    https://github.com/cryptowatch/proto/blob/master/public/markets/market.proto
    """
    market_msg = ">>> Market#{} Exchange#{} Pair#{}: {} New Trades".format(
        trade_update.marketUpdate.market.marketId,
        trade_update.marketUpdate.market.exchangeId,
        trade_update.marketUpdate.market.currencyPairId,
        len(trade_update.marketUpdate.tradesUpdate.trades),
    )
    print(market_msg)

    for trade in trade_update.marketUpdate.tradesUpdate.trades:
        trade_msg = "\tID:{} TIMESTAMP:{} TIMESTAMPNANO:{} PRICE:{} AMOUNT:{}".format(
            trade.externalId,
            trade.timestamp,
            trade.timestampNano,
            trade.priceStr,
            trade.amountStr,
        )
        print(trade_msg)

        item = json.loads(MessageToJson(trade))
        item["id"] = str(uuid.uuid4())

        global container
        container.create_item(body=item)


def read_items(container):
    print("\nReading all items in a container\n")

    # NOTE: Use MaxItemCount on Options to control how many items come back per trip to the server
    #       Important to handle throttles whenever you are doing operations such as this that might
    #       result in a 429 (throttled request)
    item_list = list(container.read_all_items(max_item_count=10))

    print("Found {0} items".format(item_list.__len__()))

    for doc in item_list:
        print("Item Id: {0}".format(doc.get("id")))


def scale_container(container):
    print("\nScaling Container\n")

    # You can scale the throughput (RU/s) of your container up and down to meet the needs of the workload. Learn more: https://aka.ms/cosmos-request-units
    try:
        offer = container.read_offer()
        print("Found Offer and its throughput is '{0}'".format(offer.offer_throughput))

        offer.offer_throughput += 100
        container.replace_throughput(offer.offer_throughput)

        print(
            "Replaced Offer. Offer Throughput is now '{0}'".format(
                offer.offer_throughput
            )
        )

    except exceptions.CosmosHttpResponseError as e:
        if e.status_code == 400:
            print("Cannot read container throuthput.")
            print(e.http_error_message)
        else:
            raise


def run_sample(subs, time_period):
    client = cosmos_client.CosmosClient(
        HOST,
        {"masterKey": MASTER_KEY},
        user_agent="CosmosDBPythonQuickstart",
        user_agent_overwrite=True,
    )
    try:
        # setup database for this sample
        try:
            db = client.create_database(id=DATABASE_ID)
            print("Database with id '{0}' created".format(DATABASE_ID))

        except exceptions.CosmosResourceExistsError:
            db = client.get_database_client(DATABASE_ID)
            print("Database with id '{0}' was found".format(DATABASE_ID))

        # setup container for this sample
        global container
        try:
            container = db.create_container(
                id=CONTAINER_ID, partition_key=PartitionKey(path="/partitionKey")
            )
            print("Container with id '{0}' created".format(CONTAINER_ID))

        except exceptions.CosmosResourceExistsError:
            container = db.get_container_client(CONTAINER_ID)
            print("Container with id '{0}' was found".format(CONTAINER_ID))

        scale_container(container)

        # Subscribe to resources (https://docs.cryptowat.ch/websocket-api/data-subscriptions#resources)
        cw.stream.subscriptions = subs

        cw.stream.on_trades_update = handle_trades_update
        # Start receiving
        cw.stream.connect()

        time.sleep(time_period)
        # create_items(container)

        # read_item(container, 'SalesOrder1', 'Account1')

        read_items(container)
        # query_items(container, 'Account1')
        # replace_item(container, 'SalesOrder1', 'Account1')
        # upsert_item(container, 'SalesOrder1', 'Account1')
        # delete_item(container, 'SalesOrder1', 'Account1')

        # cleanup database after sample
        # try:
        #     client.delete_database(db)

        # except exceptions.CosmosResourceNotFoundError:
        #     pass

    except exceptions.CosmosHttpResponseError as e:
        print("\nrun_sample has caught an error. {0}".format(e.message))
    finally:
        print("\nrun_sample done")
        # Call disconnect to close the stream connection
        cw.stream.disconnect()


if __name__ == "__main__":
    run_sample(["markets:*:trades"], 1)
