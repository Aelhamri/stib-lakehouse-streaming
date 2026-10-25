from src import config
from src.geometry import build_stop_offsets

df = build_stop_offsets(config.STATIC_DIR)

print(f"{len(df):,} lignes (ligne, direction, arrêt)")
print("snap > 50 m :", (df.snap_m > 50).sum())
print("s0 non croissant (next_gap < 0) :", (df.next_gap_m < 0).sum())
print(df.next_gap_m.describe(percentiles=[0.5, 0.9, 0.99]).round(0))

config.STOP_OFFSETS_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(config.STOP_OFFSETS_PATH, index=False)
print("écrit :", config.STOP_OFFSETS_PATH)
print(df.groupby("shape_source").size())