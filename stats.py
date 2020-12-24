import json
import time
import statistics
import re
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import defaultdict

from plumbum import cli
import requests
import diskcache
import pandas as pd
from tqdm import tqdm


def get(*args, **kwargs):
    if "headers" not in kwargs:
        kwargs["headers"] = {}
    kwargs["headers"].update(
        {"User-Agent": "AOE Stats Tool", "From": "dan@danwink.com"}
    )
    return requests.get(*args, **kwargs)


_cache = None


def get_cache():
    global _cache
    if _cache is None:
        _cache = diskcache.Cache("cache", size_limit=int(8e9))
    return _cache


def get_strings():
    key = f"strings"

    cache = get_cache()
    if key in cache:
        return cache[key]
    else:
        print(f"requesting {key}")
        value = get(f"https://aoe2.net/api/strings?game=aoe2de&language=en").json()
        cache[key] = value
        return value


def get_games(start_time, sleep_secs_if_not_cached=0.5):
    count = 1000
    key = f"matches-{int(start_time)}-{count}"

    cache = get_cache()
    if key in cache:
        return cache[key]
    else:
        print(f"requesting {key}")
        value = get(
            f"https://aoe2.net/api/matches?game=aoe2de&count={count}&since={int(start_time)}"
        ).json()
        cache[key] = value

        time.sleep(sleep_secs_if_not_cached)

        return value


def same_day(a, b):
    return a.year == b.year and a.month == b.month and a.day == b.day


def cache_games_for_day(year, month, day):
    dt_start = datetime(year, month, day)
    games = []

    next_time = int(dt_start.timestamp())
    while True:
        print(datetime.fromtimestamp(next_time))
        req_games = get_games(next_time)
        req_games.sort(key=lambda game: game["started"])

        if same_day(datetime.fromtimestamp(int(req_games[-1]["started"])), dt_start):
            games += req_games
            next_time = max(int(req_games[-1]["started"]) + 1, next_time + 1)
            continue
        else:
            # Could do a binary search here but fuck it
            games += [
                g
                for g in tqdm(req_games)
                if same_day(datetime.fromtimestamp(int(g["started"])), dt_start)
            ]
            break

    get_cache()[dt_to_day_key(dt_start)] = games


def dt_to_day_key(dt):
    return f"day-{dt.year}-{dt.month}-{dt.day}"


@dataclass
class Game:
    pass


def get_bulk_games(start_time, count):
    games = []
    total_games = 0
    last_timestamp = start_time
    while len(games) < count:
        games += get_games(last_timestamp)
        last_timestamp = games[-1]["started"] + 1
        print(len(games))
    return games


def get_mean_rating(players):
    ratings = [p["rating"] for p in players]

    ratings = [r if r is not None else 1000 for r in ratings]
    # ratings = [r for r in ratings if r is not None]
    if len(players) == 0:
        return 0
    return statistics.mean(ratings)


def games_to_df(games):
    def none_to_neg_one(v):
        if v is None:
            return -1
        return v

    return pd.DataFrame(
        [
            {
                "match_id": game["match_id"],
                "started": game["started"],
                "win_civ_set": tuple(
                    sorted(
                        [none_to_neg_one(p["civ"]) for p in game["players"] if p["won"]]
                    )
                ),
                "lose_civ_set": tuple(
                    sorted(
                        [
                            none_to_neg_one(p["civ"])
                            for p in game["players"]
                            if not p["won"]
                        ]
                    )
                ),
                "win_mean_rating": get_mean_rating(
                    [p for p in game["players"] if p["won"]]
                ),
                "lose_mean_rating": get_mean_rating(
                    [p for p in game["players"] if not p["won"]]
                ),
                "win_pid_set": tuple(
                    sorted(
                        [
                            none_to_neg_one(p["profile_id"])
                            for p in game["players"]
                            if p["won"]
                        ]
                    )
                ),
                "lose_pid_set": tuple(
                    sorted(
                        [
                            none_to_neg_one(p["profile_id"])
                            for p in game["players"]
                            if not p["won"]
                        ]
                    )
                ),
                "ranked": game["ranked"],
            }
            for game in tqdm(games)
        ]
    ).drop_duplicates()


class Main(cli.Application):
    def main(self):
        if self.nested_command:
            return

        time = datetime(2020, 11, 1)

        wins_by_civ_combo = defaultdict(int)

        game_count = 3_000_000
        games = get_bulk_games(time.timestamp(), game_count)

        df = games_to_df(games)

        df.to_pickle(f"games_{game_count}.pkl")

        # for game in games:
        #     if game['ranked']:
        #         players = game['players']
        #         win_civs = frozenset([p['civ'] for p in players if p['won']])
        #         wins_by_civ_combo[win_civs] += 1

        # print("sorting")
        # sorted_wins_by_civ_combo = dict(sorted(wins_by_civ_combo.items(), key=lambda item: -item[1]))

        # strings = get_strings()
        # civ_map = {c['id']: c['string'] for c in strings['civ']}
        # for civs, count in sorted_wins_by_civ_combo.items():
        #     print([civ_map[v] for v in civs], count)


@Main.subcommand("strings")
class StringsCommand(cli.Application):
    """
    Prints the json strings
    """

    def main(self):
        strings = get_strings()
        with open("strings.json", "w") as f:
            f.write(json.dumps(strings))


@Main.subcommand("getday")
class GetDayCommand(cli.Application):
    """
    Caches games for a single day into a daily container cache
    """

    def main(self, year, month, day, day_count=1):
        dt = datetime(int(year), int(month), int(day))

        cache = get_cache()
        for i in range(int(day_count)):
            cache_games_for_day(dt.year, dt.month, dt.day)

            games_to_df(cache[dt_to_day_key(dt)]).to_pickle(
                f"days/games-{dt.year}-{dt.month}-{dt.day}.pkl"
            )

            dt += timedelta(days=1)


@Main.subcommand("allcachedgames")
class AllCachedGamesCommand(cli.Application):
    """
    Converts all cached raw batch games to a df pickle
    """

    def main(self):
        games = []
        cache = get_cache()
        for key in tqdm(cache):
            if "matches" in key:
                games += cache[key]

        df = games_to_df(games)

        df.to_pickle("games_all.pkl")


@Main.subcommand("alldailygames")
class AllDailyGamesCommand(cli.Application):
    """
    Converts all daily games to a df pickle
    """

    def main(self):
        games = []
        cache = get_cache()
        for key in tqdm(cache):
            if "day" in key:
                games += cache[key]

        df = games_to_df(games)

        df.to_pickle("games_all_days.pkl")


@Main.subcommand("keys")
class KeysCommand(cli.Application):
    """
    Prints all cached keys
    """

    def main(self):
        cache = get_cache()
        for key in tqdm(sorted(list(cache))):
            print(key)


@Main.subcommand("cache")
class CacheCommand(cli.Application):
    """
    Get value for given key
    """

    def main(self, key):
        cache = get_cache()
        print(cache[key])


@Main.subcommand("rawtodaily")
class RawToDailyCommand(cli.Application):
    """
    Converts all raw batch games to their daily container
    """

    def main(self):
        matches_regex = r"matches-(?P<timestamp>\d+)-\d+"
        p = re.compile(matches_regex)

        cache = get_cache()
        games_by_day = defaultdict(list)
        for key in tqdm(cache):
            m = p.search(key)
            if m:
                dt = datetime.fromtimestamp(int(m.group("timestamp")))
                games = cache[key]
                # TODO: this could be sped up by finding where the day intersection is and putting all games before into first day
                #       and all games after into second day
                for game in games:
                    games_by_day[
                        dt_to_day_key(datetime.fromtimestamp(int(game["started"])))
                    ] += [game]

        for key, games in tqdm(games_by_day.items()):
            cache[key] = games


if __name__ == "__main__":
    Main.run()
