import numpy as np
import pandas as pd


def build_tracklets(
    obs: pd.DataFrame,
    window_s: float = 300.0,
    max_speed_mps: float = 30.0,
    max_backward_m: float = 50.0,
    timeout_s: float = 1800.0,
    max_duration_s: float = 7200.0,
) -> list[int]:
    """obs : une seule (ligne, direction), colonnes `ts_s` (secondes) et `s_m` (mètres).

    Renvoie un identifiant de tracklet par ligne de `obs`, dans le même ordre.
    """
    ts = obs["ts_s"].to_numpy(dtype=float)
    s = obs["s_m"].to_numpy(dtype=float)
    n = len(obs)

    ids = [-1] * n
    open_trips: dict[int, dict] = {}   # tracklet_id -> {first_ts, last_ts, last_s}
    next_id = 0

    order = np.argsort(ts, kind="stable")
    i = 0
    while i < n:
        # --- un « batch » = toutes les observations d'un même poll ---
        t = ts[order[i]]
        j = i
        while j < n and ts[order[j]] == t:
            j += 1
        batch = order[i:j]
        i = j

        # 1. fermer les tracklets inactifs depuis trop longtemps
        for tid in [tid for tid, tr in open_trips.items() if t - tr["last_ts"] > timeout_s]:
            del open_trips[tid]

        # 2. candidats valides (observation, tracklet), notés par |Δs|
        candidates = []
        for k in batch:
            for tid, tr in open_trips.items():
                age = t - tr["last_ts"]
                if age <= 0 or age > window_s:
                    continue
                if t - tr["first_ts"] > max_duration_s:
                    continue
                delta = s[k] - tr["last_s"]
                if delta < -max_backward_m:
                    continue
                if abs(delta) / age > max_speed_mps:
                    continue
                candidates.append((abs(delta), k, tid))

        # 3. appariement glouton : le plus proche d'abord, 1 match par obs et par tracklet
        candidates.sort()
        used_obs: set[int] = set()
        used_trips: set[int] = set()
        for _, k, tid in candidates:
            if k in used_obs or tid in used_trips:
                continue
            ids[k] = tid
            used_obs.add(k)
            used_trips.add(tid)
            open_trips[tid]["last_ts"] = t
            open_trips[tid]["last_s"] = s[k]

        # 4. les observations sans candidat ouvrent un nouveau tracklet
        for k in batch:
            if ids[k] == -1:
                ids[k] = next_id
                open_trips[next_id] = {"first_ts": t, "last_ts": t, "last_s": s[k]}
                next_id += 1

    return ids