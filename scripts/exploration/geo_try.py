import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.ops import transform

from src import config

shapes = pd.read_json(config.STATIC_DIR / "line_shapes.jsonl.gz", lines=True, dtype=False)
print(shapes[["lineid", "direction", "variant", "mode"]].head())

print("--- Nombre de tracés (variantes) par (ligne, direction) ---")
print(shapes.groupby(["lineid", "direction"]).size().value_counts().sort_index())

to_m = Transformer.from_crs(config.CRS_WGS84, config.CRS_METRIC, always_xy=True).transform

print("--- Ligne 12 : longueur de chaque tracé ---")
for _, r in shapes[shapes.lineid == "12"].iterrows():
    g = transform(to_m, wkt.loads(r.geom_wkt))
    print(r.direction, r.variant, round(g.length), "m")