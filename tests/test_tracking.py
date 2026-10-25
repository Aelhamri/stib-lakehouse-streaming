import pandas as pd

from src.tracking import build_tracklets


def obs(rows):
    return pd.DataFrame(rows, columns=["ts_s", "s_m"])


def test_two_buses_followed_over_three_polls():
    o = obs([(0, 100), (0, 2000), (30, 400), (30, 2300), (60, 700), (60, 2600)])
    ids = build_tracklets(o)
    assert len(set(ids)) == 2
    assert ids[0] == ids[2] == ids[4]
    assert ids[1] == ids[3] == ids[5]


def test_backward_move_opens_new_tracklet():
    ids = build_tracklets(obs([(0, 1000), (30, 800)]))   # recul de 200 m
    assert ids[0] != ids[1]


def test_too_fast_opens_new_tracklet():
    ids = build_tracklets(obs([(0, 0), (30, 2000)]))     # 66 m/s
    assert ids[0] != ids[1]


def test_gap_longer_than_window_opens_new_tracklet():
    ids = build_tracklets(obs([(0, 0), (400, 100)]))     # 400 s > 300 s
    assert ids[0] != ids[1]


def test_two_observations_of_one_poll_never_share_a_tracklet():
    ids = build_tracklets(obs([(0, 100), (30, 150), (30, 160)]))
    assert ids[1] != ids[2]


def test_greedy_picks_nearest():
    # deux bus à 100 m et 120 m ; l'observation à 130 m est plus proche du second
    ids = build_tracklets(obs([(0, 100), (0, 120), (30, 130)]))
    assert ids[2] == ids[1]
    assert ids[2] != ids[0]