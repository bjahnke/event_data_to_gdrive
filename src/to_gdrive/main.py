from flask import Flask
import sqlalchemy

import src.utils.to_gdrive_utils as to_gdrive_utils
import pandas as pd
import env
from sqlalchemy import create_engine
import pymongo
import src.scalpyr as scalpyr
import numpy as np

app = Flask(__name__)


# from stats table, for each unique event id in stats get percent change in price from 1 week ago
def get_pct_change(_stats_view: pd.DataFrame, price_col, _event_id, from_date=None):
    event_stats = _stats_view[_stats_view.event_id == _event_id].copy()
    # get percent change in price from_date to now
    if from_date is not None:
        event_stats = event_stats.loc[event_stats.utc_read_time >= from_date]
    # calculate percent change of average price
    event_stats[f"{price_col}_pct"] = event_stats[price_col].pct_change()
    event_stats[f"{price_col}_log"] = np.log(event_stats[price_col]) - np.log(
        event_stats[price_col].shift(1)
    )
    event_stats[f"{price_col}_log_cumsum"] = event_stats[f"{price_col}_log"].cumsum()
    event_stats[f"{price_col}_pct_change"] = (
        event_stats[price_col].pct_change().cumsum()
    )
    return event_stats


@app.route("/", methods=["POST"])
def handle_request():
    """
    This is the main function that will be called by the cloud function.
    :return:
    """
    engine = create_engine(env.PLANETSCALE_URL)
    mongo_client = pymongo.MongoClient(env.MONGO_URL)
    sg_client = scalpyr.ScalpyrPro(env.SEATGEEK_CLIENT_ID)

    db = mongo_client["event-tracking"]
    collection = db["watchlist"]
    latest_entry = collection.find_one({"username": "bjahnke71"}, sort=[("_id", -1)])
    tracked_venues = latest_entry["venue_id"]

    # Create temporary table in planet scale with venue_id
    tracked_venues_df = pd.DataFrame({"venue_id": tracked_venues})
    tracked_venues_df["venue_id"] = tracked_venues_df["venue_id"].astype(int)
    tracked_venues_df.to_sql("tracked_venues", engine, if_exists="replace", index=False)

    # get date 3 days ago as string like 2023-09-14
    from_date = pd.Timestamp.utcnow() - pd.Timedelta(days=10)
    today_date_str = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    from_date_str = from_date.strftime("%Y-%m-%d")
    get_events_by_venue_query = (
        "select pev.event_id from performer_event_venue pev "
        f"where pev.datetime_utc >= '{today_date_str}' "
        "and pev.venue_id in (select tv.venue_id from tracked_venues tv)"
    )
    stats_query = (
        "select * from stat "
        f"where stat.event_id in ({get_events_by_venue_query}) "
        # f"and stat.utc_read_time >= '{from_date_str}' "
        "order by stat.utc_read_time asc "
    )

    stat = pd.read_sql(stats_query, engine)

    # drop the temporary table in planet scale
    with engine.connect() as con:
        con.execute(sqlalchemy.text("drop table tracked_venues"))

    stats_pct_change_views = []
    _price_col = "lowest_price"
    for event_id in stat.event_id.unique():
        res = get_pct_change(stat, _price_col, event_id)
        stats_pct_change_views.append(res)

    stats_pct_change_df = pd.concat(stats_pct_change_views)
    # select last row for each event_id by most recent utc_read_time
    scoreboard = (
        stats_pct_change_df.groupby("event_id")
        .agg({f"{_price_col}_pct_change": "last"})
        .reset_index()
        .rename(columns={"index": "event_id"})
    )
    # send request to get event data by event_id in batches of 100
    scoreboard.event_id = scoreboard.event_id.astype(str)
    event_data_list = []
    i = 0
    batch_size = 300
    while i < len(scoreboard):
        x = i + batch_size if i + batch_size < len(scoreboard) else len(scoreboard)
        event_ids = scoreboard.event_id.iloc[i:x]
        if len(event_ids) == 0:
            break
        i = x
        event_data = sg_client.get_by_id("events", event_ids)
        event_data_list.append(event_data)

    event_data = pd.concat(event_data_list)
    event_data.id = event_data.id.astype(str)

    # merge top event data with top scoreboard on event_id and id, drop id column
    event_data = event_data.merge(scoreboard, left_on="id", right_on="event_id").drop(
        columns=["id"]
    )
    # set venue to top_event_data.venue['name']
    event_data["venue"] = event_data.venue.apply(lambda x: x["name"])

    event_data = event_data.drop(columns=["access_method", "performers", "type"])

    stats_df = event_data.stats.apply(pd.Series).drop(
        columns=[
            "visible_listing_count",
            "dq_bucket_counts",
            "average_price",
            "median_price",
            "lowest_sg_base_price_good_deals",
            "lowest_sg_base_price",
            "lowest_price_good_deals",
            "highest_price",
        ]
    )
    event_data = pd.concat([event_data, stats_df], axis=1).drop(columns=["stats"])
    event_data = event_data.fillna(0)

    to_gdrive_utils.generate_and_save_csv(
        event_data.values.tolist(),
        env.GDRIVE_FOLDER_ID,
        {
            "type": env.GDRIVE_TYPE,
            "project_id": env.GDRIVE_PROJECT_ID,
            "private_key_id": env.GDRIVE_PRIVATE_KEY_ID,
            "private_key": env.GDRIVE_PRIVATE_KEY,
            "client_email": env.GDRIVE_CLIENT_EMAIL,
            "client_id": env.GDRIVE_CLIENT_ID,
            "auth_uri": env.GDRIVE_AUTH_URI,
            "token_uri": env.GDRIVE_TOKEN_URI,
            "auth_provider_x509_cert_url": env.GDRIVE_AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": env.GDRIVE_CLIENT_X509_CERT_URL,
            "universe_domain": env.GDRIVE_UNIVERSE_DOMAIN,
        },
    )
    return "Service executed with no errors"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
