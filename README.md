# ArcGIS → Quickbase Active Cabinet Sync Pipeline

Automated Python pipeline that queries ArcGIS cabinet features, projects coordinates to WGS84 (lat/lon), and updates matching Quickbase records using controlled, batch-safe API writes.

Designed for unattended scheduled execution using **Power Automate Cloud + Power Automate Desktop**, with structured logging and Microsoft Teams run summaries.

---

## 🚀 Overview

This project implements an automated integration pipeline from **ArcGIS Enterprise** to **Quickbase** for active cabinet records.

The script:
- Queries ArcGIS feature layer cabinet features
- Extracts cabinet IDs and geometries
- Projects coordinates to EPSG:4326 (WGS84 latitude/longitude)
- Cross-references Quickbase records by Cabinet ID
- Builds batch update payloads
- Updates Quickbase latitude/longitude fields
- Exports unmatched cabinet IDs for review
- Emits structured run metrics for automation reporting

---

## 🧩 Architecture

**ArcGIS Feature Layer → Python Sync Script → Quickbase Table**  
&nbsp;&nbsp;&nbsp;&nbsp;↑  
**Power Automate Cloud** (scheduled trigger)  
&nbsp;&nbsp;&nbsp;&nbsp;↓  
**Power Automate Desktop** (runs Python + posts Teams message)

High-level flow:
1. **Power Automate Cloud** triggers on schedule
2. Launches **Power Automate Desktop**
3. Desktop flow runs the Python script
4. Script authenticates to:
   - ArcGIS Portal / Enterprise
   - Quickbase API
5. ArcGIS cabinet features are queried
6. Cabinet IDs and geometries are validated and extracted
7. Coordinates are projected to WGS84 (lat/lon)
8. Quickbase records are indexed by Cabinet ID
9. Matching records receive coordinate updates in batches
10. Unmatched IDs are exported to CSV
11. Run summary is posted to **Microsoft Teams**

---

## 🗺 What Gets Synced

### Source: ArcGIS
- Cabinet features from a configured Feature Layer
- Cabinet identifier (from one of several supported attribute fields)
- Geometry (used to compute WGS84 latitude/longitude)

### Target: Quickbase
- Cabinet table
- Matching record determined by Cabinet ID
- Latitude/Longitude fields updated for matched cabinets

---

## 🧠 Key Features

- ✅ ArcGIS Python API feature layer queries
- ✅ Geometry projection to EPSG:4326
- ✅ Multi-field Cabinet ID detection
- ✅ Cross-system record matching (Cabinet ID → Quickbase Record ID)
- ✅ Batch-safe Quickbase updates
- ✅ Unmatched cabinet export to CSV
- ✅ Automation-friendly logging and summary output
- ✅ Unattended/scheduled execution design

---

## 📊 Automation Output

The script emits automation-friendly summary lines intended for **Power Automate Desktop** parsing and Teams posting.

Typical outputs include:
- Cabinets usable
- Quickbase records indexed
- Updates applied
- Unmatched cabinets exported
- Status + duration

(Exact format can be customized in your PAD flow depending on how you parse log lines.)

---

## 📁 Unmatched Cabinet Export

If a cabinet exists in ArcGIS but no matching Quickbase record is found, the Cabinet ID is included in a CSV export for review.

The export location is controlled by an environment variable such as:
- `UNMATCHED_EXPORT_PATH` (example pattern)

---

## 🔐 Authentication

Credentials are expected via environment variables (recommended for automation).

### ArcGIS
- `ARCGIS_PORTAL_URL`
- `OMNI_GIS_USER`
- `OMNI_GIS_PASS`

### Quickbase
- `QB_TOKEN`

**Do not store credentials in code or in the repo.**

---

## ▶️ Running Locally

With required environment variables set:

```bash
python ActiveCab_to_QuickBase.py
