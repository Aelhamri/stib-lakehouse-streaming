import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import LineString, Point

from src import config

LINE, DIRECTION = "12", "City"

shapes = pd.read_json(config.STATIC_DIR / "line_shapes.jsonl.gz", lines=True, dtype=False)
line_stops = pd.read_json(config.STATIC_DIR / "line_stops.jsonl.gz", lines=True, dtype=False)
stops = pd.read_json(config.STATIC_DIR / "stops.jsonl.gz", lines=True, dtype=False)
stops["lat"] = stops["lat"].astype(float)
stops["lon"] = stops["lon"].astype(float)

tr = Transformer.from_crs(config.CRS_WGS84, config.CRS_METRIC, always_xy=True)

shape_wgs = wkt.loads(
    shapes[(shapes.lineid == LINE) & (shapes.direction == DIRECTION)].geom_wkt.iloc[0]
)
xs, ys = tr.transform([c[0] for c in shape_wgs.coords], [c[1] for c in shape_wgs.coords])
line = LineString(list(zip(xs, ys)))

ls = (
    line_stops[(line_stops.lineid == LINE) & (line_stops.direction == DIRECTION)]
    .sort_values("sequence_idx")
    .merge(stops[["pointid", "stop_name_fr", "lat", "lon"]], on="pointid", how="left")
)
print("Arrêts sans coordonnées :", ls.lat.isna().sum())
ls = ls.dropna(subset=["lat"])

px, py = tr.transform(ls.lon.tolist(), ls.lat.tolist())
ls["s0"] = [line.project(Point(x, y)) for x, y in zip(px, py)]
ls["snap_m"] = [line.distance(Point(x, y)) for x, y in zip(px, py)]
ls["gap_m"] = ls["s0"].diff()

print(ls[["sequence_idx", "pointid", "stop_name_fr", "s0", "gap_m", "snap_m"]].round(0).to_string(index=False))
print("Écart max entre arrêts consécutifs :", round(ls.gap_m.max()), "m")
print("s0 croissant le long de la séquence :", ls.s0.is_monotonic_increasing)