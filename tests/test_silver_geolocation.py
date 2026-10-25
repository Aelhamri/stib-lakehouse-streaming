from shapely.geometry import LineString

from src.spark_jobs.silver_geolocation import add_coordinates, locate_on_segment


def make_inputs(spark):
    silver = spark.createDataFrame(
        [
            ("12", "City", "p1", 100.0),  # dans le tronçon
            ("12", "City", "p1", 450.0),  # dans la tolérance (400 + 100), dépasse le tracé -> clip
            ("12", "City", "p1", 900.0),  # au-delà du tronçon, tracé propre -> quarantaine
            ("13", "City", "p1", 900.0),  # tracé approximatif -> gardé, non rejeté
            ("14", "City", "p9", 100.0),  # arrêt inconnu -> NO_SEGMENT
        ],
        ["lineid", "direction", "pointId", "distance_m"],
    )
    offsets = spark.createDataFrame(
        [
            ("12", "City", "p1", 0.0, 400.0, 1.0, "OWN"),
            ("13", "City", "p1", 0.0, 400.0, 1.0, "REVERSED_OPPOSITE"),
        ],
        ["lineid", "direction", "stop_base", "s0_m", "seg_len_m", "snap_m", "shape_source"],
    )
    return silver, offsets


def test_locate_on_segment(spark):
    silver, offsets = make_inputs(spark)
    positioned, quarantine = locate_on_segment(silver, offsets)

    assert quarantine.count() == 1
    assert quarantine.collect()[0]["rejection_reason"] == "BEYOND_SEGMENT"
    assert positioned.count() + quarantine.count() == 5

    status = {(r["lineid"], r["distance_m"]): r["geo_status"] for r in positioned.collect()}
    assert status[("12", 100.0)] == "LOCATABLE"
    assert status[("13", 900.0)] == "LOCATABLE_APPROX"
    assert status[("14", 100.0)] == "NO_SEGMENT"


def test_add_coordinates_clips_and_keeps_unlocated(spark):
    silver, offsets = make_inputs(spark)
    positioned, _ = locate_on_segment(silver, offsets)
    shapes = {
        ("12", "City"): LineString([(0, 0), (300, 0)]),
        ("13", "City"): LineString([(0, 0), (300, 0)]),
    }
    located = add_coordinates(positioned, shapes)

    got = {(r["lineid"], r["distance_m"]): r["x_m"] for r in located.collect()}
    assert located.count() == 4
    assert round(got[("12", 100.0)], 6) == 100.0
    assert round(got[("12", 450.0)], 6) == 300.0   # clip à la fin du tracé
    assert round(got[("13", 900.0)], 6) == 300.0
    assert got[("14", 100.0)] is None              # sans tronçon : pas de coordonnées