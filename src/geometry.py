import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import LineString, Point

from src import config

OPPOSITE = {"City": "Suburb", "Suburb": "City"}


def _read_static(static_dir):
    shapes = pd.read_json(static_dir / "line_shapes.jsonl.gz", lines=True, dtype=False)
    line_stops = pd.read_json(static_dir / "line_stops.jsonl.gz", lines=True, dtype=False)
    stops = pd.read_json(static_dir / "stops.jsonl.gz", lines=True, dtype=False)
    stops["lat"] = stops["lat"].astype(float)
    stops["lon"] = stops["lon"].astype(float)
    return shapes, line_stops, stops


def _resolve_line(shape_wkt, lineid, direction, tr):
    """Renvoie (LineString en Lambert 72, source) ou (None, None) si aucun tracé."""
    key = (lineid, direction)
    if key in shape_wkt:
        coords = list(wkt.loads(shape_wkt[key]).coords)
        source = "OWN"
    elif (lineid, OPPOSITE[direction]) in shape_wkt:
        coords = list(wkt.loads(shape_wkt[(lineid, OPPOSITE[direction])]).coords)[::-1]
        source = "REVERSED_OPPOSITE"
    else:
        return None, None
    xs, ys = tr.transform([c[0] for c in coords], [c[1] for c in coords])
    return LineString(list(zip(xs, ys))), source


def build_shapes_m(static_dir) -> dict:
    """(ligne, direction) -> LineString en Lambert 72, pour toutes les paires de line_stops."""
    shapes, line_stops, _ = _read_static(static_dir)
    tr = Transformer.from_crs(config.CRS_WGS84, config.CRS_METRIC, always_xy=True)
    shape_wkt = {(r.lineid, r.direction): r.geom_wkt for r in shapes.itertuples()}
    out = {}
    for t in line_stops[["lineid", "direction"]].drop_duplicates().itertuples():
        line, _ = _resolve_line(shape_wkt, t.lineid, t.direction, tr)
        if line is not None:
            out[(t.lineid, t.direction)] = line
    return out


def build_stop_offsets(static_dir) -> pd.DataFrame:
    """Pour chaque (ligne, direction, arrêt) : s0, distance au tracé, écart jusqu'à l'arrêt suivant."""
    shapes, line_stops, stops = _read_static(static_dir)
    tr = Transformer.from_crs(config.CRS_WGS84, config.CRS_METRIC, always_xy=True)
    shape_wkt = {(r.lineid, r.direction): r.geom_wkt for r in shapes.itertuples()}

    parts = []
    for t in line_stops[["lineid", "direction"]].drop_duplicates().itertuples():
        line, source = _resolve_line(shape_wkt, t.lineid, t.direction, tr)
        if line is None:
            continue

        ls = (
            line_stops[(line_stops.lineid == t.lineid) & (line_stops.direction == t.direction)]
            .sort_values("sequence_idx")
            .merge(stops[["pointid", "lat", "lon"]], on="pointid", how="left")
            .dropna(subset=["lat"])
        )
        if ls.empty:
            continue

        px, py = tr.transform(ls.lon.tolist(), ls.lat.tolist())
        pts = [Point(x, y) for x, y in zip(px, py)]
        out = pd.DataFrame({
            "lineid": t.lineid,
            "direction": t.direction,
            "sequence_idx": ls.sequence_idx.values,
            "pointid": ls.pointid.values,
            "stop_base": ls.pointid.str.replace(r"[A-Za-z]+$", "", regex=True).values,
            "s0_m": [line.project(p) for p in pts],
            "snap_m": [line.distance(p) for p in pts],
            "shape_length_m": line.length,
            "shape_source": source,
        })
        out["next_gap_m"] = out["s0_m"].shift(-1) - out["s0_m"]
        parts.append(out)

    return pd.concat(parts, ignore_index=True)