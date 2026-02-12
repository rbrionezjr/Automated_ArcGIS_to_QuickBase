import os
import json
import logging
from typing import Dict, List, Any, Tuple

from collections import defaultdict
import datetime as dt
import time

import requests
from arcgis.gis import GIS
from arcgis.geometry import project as arc_project

import sys
import traceback

import csv
from pathlib import Path


# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("active_cab_qb_sync")

# ----------------------------
# Metrics (PAD / Teams)
# ----------------------------
METRICS = defaultdict(int)
RUN_INFO = {"started_utc": None, "ended_utc": None, "duration_sec": None}


def metrics_start():
    RUN_INFO["started_utc"] = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    RUN_INFO["_t0"] = time.time()


def metrics_end():
    RUN_INFO["ended_utc"] = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    RUN_INFO["duration_sec"] = int(round(time.time() - RUN_INFO["_t0"]))


def emit_pad_summary():
    payload = {
        "status": "OK" if METRICS["errors"] == 0 else "WARN",
        "started_utc": RUN_INFO["started_utc"],
        "ended_utc": RUN_INFO["ended_utc"],
        "duration_sec": RUN_INFO["duration_sec"],

        "active_cabinets": {
            "usable": METRICS["usable"],
            "qb_indexed": METRICS["qb_indexed"],
            "updates": METRICS["updates"],
            "unmatched": METRICS["unmatched"]
        },

        "errors": METRICS["errors"]
    }

    print("PAD_SUMMARY=" + json.dumps(payload, separators=(",", ":")))


# ----------------------------
# EXPORT CONFIG
# ----------------------------

EXPORT_UNMATCHED_PATH_ENV = "UNMATCHED_EXPORT_PATH"

def export_unmatched_cabinets(cabinet_ids: List[str]) -> None:
    export_path = os.getenv(EXPORT_UNMATCHED_PATH_ENV)

    if export_path == "DEFAULT":
        export_path = str(Path.home() / "Documents" / "unmatched_cabinets.csv")

    if not export_path:
        return

    # Append timestamp to filename, e.g. unmatched_cabinets_20260209_153045.csv
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base, ext = os.path.splitext(export_path)
    export_path = f"{base}_{ts}{ext or '.csv'}"

    if not cabinet_ids:
        log.info("No unmatched cabinets to export.")
        return

    export_dir = os.path.dirname(export_path)
    if export_dir:
        os.makedirs(export_dir, exist_ok=True)

    with open(export_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["CabinetID"])
        for cab_id in cabinet_ids:
            writer.writerow([cab_id])

    log.info("Unmatched cabinet IDs exported to: %s", export_path)


# ----------------------------
# CONFIG
# ----------------------------
QB_REALM = "omnifiber.quickbase.com"

# ArcGIS Active Cabinets (Feature Layer Item ID)
ACTIVE_CAB_LAYER_ITEM_ID = "8a42d8a5d7b649109101b15647a2235d"

# Quickbase Active Cabinets table
QB_ACTIVE_TABLE_ID = "bts8av3cw"

# Quickbase field IDs
QB_RECORD_ID_FID = 3
QB_CABINET_ID_FID = 6
QB_LAT_FID = 9
QB_LON_FID = 10

# Cabinet ID field candidates in ArcGIS attributes
ARC_CABINET_ID_FIELDS = ("CabinetID", "cab_id", "Cabinet_ID")

# Batch sizes
QB_UPDATE_BATCH_SIZE = 200
PROJECT_BATCH_SIZE = 200


# ----------------------------
# Quickbase helpers (shared)
# ----------------------------
def qb_headers(qb_token: str) -> Dict[str, str]:
    return {
        "QB-Realm-Hostname": QB_REALM,
        "Authorization": f"QB-USER-TOKEN {qb_token}",
        "Content-Type": "application/json",
    }


def qb_post(qb_token: str, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(url, headers=qb_headers(qb_token), data=json.dumps(payload))
    r.raise_for_status()
    return r.json()


def qb_fetch_cabinet_lookup(qb_token: str) -> Dict[str, int]:
    """
    Build a lookup: CabinetID (string) -> Quickbase RecordID (int)
    """
    log.info("Fetching Quickbase cabinet records for lookup...")

    payload = {
        "from": QB_ACTIVE_TABLE_ID,
        "select": [QB_RECORD_ID_FID, QB_CABINET_ID_FID],
    }

    data = qb_post(qb_token, "https://api.quickbase.com/v1/records/query", payload)
    records = data.get("data", [])

    lookup: Dict[str, int] = {}
    for rec in records:
        cab_val = rec.get(str(QB_CABINET_ID_FID), {}).get("value")
        rec_id = rec.get(str(QB_RECORD_ID_FID), {}).get("value")
        if cab_val is None or rec_id is None:
            continue
        key = str(cab_val).strip()
        if key:
            lookup[key] = rec_id

    log.info("Quickbase cabinets indexed: %s", len(lookup))
    return lookup


def qb_update_records(qb_token: str, updates: List[Dict[str, Any]], batch_size: int = QB_UPDATE_BATCH_SIZE) -> None:
    """
    Send QB updates in batches to /v1/records
    """
    if not updates:
        log.info("No Quickbase updates to send.")
        return

    for i in range(0, len(updates), batch_size):
        chunk = updates[i:i + batch_size]
        payload = {"to": QB_ACTIVE_TABLE_ID, "data": chunk}

        r = requests.post(
            "https://api.quickbase.com/v1/records",
            headers=qb_headers(qb_token),
            data=json.dumps(payload),
        )

        if r.status_code >= 300:
            log.error("QB batch failed (%s-%s): %s %s", i + 1, i + len(chunk), r.status_code, r.text)
            # Continue to next batch (unattended script should keep going)
        else:
            log.info("QB updated %s-%s", i + 1, i + len(chunk))


# ----------------------------
# ArcGIS helpers (shared)
# ----------------------------
def get_required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Missing required environment variable: {name}")
    return v


def get_gis_from_env() -> GIS:
    portal_url = get_required_env("ARCGIS_PORTAL_URL")
    user = get_required_env("OMNI_GIS_USER")
    pw = get_required_env("OMNI_GIS_PASS")

    gis = GIS(portal_url, user, pw)
    me = gis.users.me
    log.info("ArcGIS login OK as: %s", getattr(me, "username", None))
    return gis


def extract_cabinet_id(attrs: Dict[str, Any]) -> str | None:
    for f in ARC_CABINET_ID_FIELDS:
        v = attrs.get(f)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def project_points_to_wgs84(gis: GIS, in_wkid: int, points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Uses the ArcGIS geometry service to project points from in_wkid -> 4326.
    points: list of {"x":..., "y":..., "spatialReference": {"wkid": in_wkid}}
    Returns list of projected geometries in same order.
    """
    if not points:
        return []

    projected: List[Dict[str, Any]] = []

    for i in range(0, len(points), PROJECT_BATCH_SIZE):
        chunk = points[i:i + PROJECT_BATCH_SIZE]
        # arcgis.geometry.project returns list of geometries
        out = arc_project(geometries=chunk, in_sr=in_wkid, out_sr=4326, gis=gis)
        projected.extend(out)

    return projected


# ----------------------------
# Main runner
# ----------------------------
def run_active_cabinet_sync(gis: GIS, qb_token: str) -> None:
    log.info("==== Active Cabinet Sync starting ====")

    item = gis.content.get(ACTIVE_CAB_LAYER_ITEM_ID)
    if not item:
        raise SystemExit(f"Could not find Active Cabinet layer item: {ACTIVE_CAB_LAYER_ITEM_ID}")

    layer = item.layers[0]

    # Determine input WKID
    try:
        in_wkid = layer.properties.extent["spatialReference"]["wkid"]
    except Exception:
        in_wkid = None

    if not in_wkid:
        raise SystemExit("Could not determine layer WKID for projection.")

    # Query all active cabinet features with geometry
    feats = layer.query(where="1=1", return_geometry=True).features
    log.info("Active cabinet features retrieved: %s", len(feats))

    # Build list of points + cabinet IDs (keeping order aligned)
    cab_ids: List[str] = []
    points: List[Dict[str, Any]] = []

    skipped_no_geom = 0
    skipped_no_id = 0

    for f in feats:
        attrs = f.attributes or {}
        geom = f.geometry or {}

        x = geom.get("x")
        y = geom.get("y")
        if x is None or y is None:
            skipped_no_geom += 1
            continue

        cab_id = extract_cabinet_id(attrs)
        if not cab_id:
            skipped_no_id += 1
            continue

        cab_ids.append(cab_id)
        points.append({"x": x, "y": y, "spatialReference": {"wkid": in_wkid}})

    usable_count = len(points)
    METRICS["usable"] = usable_count
    log.info("Cabinets usable: %s | skipped no-geom=%s | skipped no-id=%s",
             usable_count, skipped_no_geom, skipped_no_id)

    # Project points to WGS84 (4326)
    projected = project_points_to_wgs84(gis, in_wkid, points)

    if len(projected) != len(cab_ids):
        raise SystemExit("Projection mismatch: projected points count != cabinet IDs count.")

    # Quickbase lookup (cabinet id -> record id)
    qb_lookup = qb_fetch_cabinet_lookup(qb_token)
    qb_indexed_count = len(qb_lookup)
    METRICS["qb_indexed"] = qb_indexed_count

    # Build QB updates
    updates: List[Dict[str, Any]] = []
    unmatched_count = 0  # <-- RENAMED (was 'unmatched')
    unmatched_ids: List[str] = [] # <-- Used for manual export when needed.

    for cab_id, g in zip(cab_ids, projected):
        rec_id = qb_lookup.get(cab_id)
        if not rec_id:
            unmatched_count += 1
            METRICS["unmatched"] += 1
            unmatched_ids.append(cab_id)
            continue

        lon = g.get("x")
        lat = g.get("y")
        if lat is None or lon is None:
            continue

        updates.append({
            str(QB_RECORD_ID_FID): {"value": rec_id},
            str(QB_LAT_FID): {"value": round(float(lat), 6)},
            str(QB_LON_FID): {"value": round(float(lon), 6)},
        })

    updates_count = len(updates)
    METRICS["updates"] = updates_count
    log.info("QB updates prepared: %s | unmatched cabinets: %s", updates_count, unmatched_count)

    # Push updates to Quickbase
    qb_update_records(qb_token, updates, batch_size=QB_UPDATE_BATCH_SIZE)

    # Export the unmatched IDs
    export_unmatched_cabinets(unmatched_ids)

    # ---- PAD one-line result (easy to parse) ----
    print(
        f"PAD_RESULT|status=SUCCESS"
        f"|usable={usable_count}"
        f"|qb_indexed={qb_indexed_count}"
        f"|updates={updates_count}"
        f"|unmatched={unmatched_count}"
    )

    log.info("==== Active Cabinet Sync finished ====")


def main():
    metrics_start()

    qb_token = get_required_env("QB_TOKEN")
    gis = get_gis_from_env()

    run_active_cabinet_sync(gis, qb_token)

    metrics_end()
    emit_pad_summary()

    log.info("✅ All done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        METRICS["errors"] += 1
        metrics_end()
        emit_pad_summary()
        traceback.print_exc()
        sys.exit(1)
