from pathlib import Path
import pandas as pd

STATE = "TX"

RAW_PATH = Path("data/raw/echo/cwa_facilities_tx.parquet")
OUT_PATH = Path("data/processed/echo/cwa_facilities_tx_clean.parquet")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_parquet(RAW_PATH)

# Keep only columns we know exist (based on your printout)
keep = [c for c in [
    "SourceID",                 # join key across other ECHO pulls
    "CWPName", "CWPState", "CWPCity", "CWPStateDistrict",
    "FacLat", "FacLong",        # lat/lon (FacLat may exist; we’ll handle if missing)
    "CWPTotalDesignFlowNmbr",   # flow feature
    "CWPEffectiveDate",
    "AcsPopulationDensity",
    "PercentPeopleOfColor",
    "FacStdCountyName",
    "Statute",
    "FacFederalAgencyName",
    "CWPIndianCntryFlg",
    "FacIndianSpatialFlg",
] if c in df.columns]

out = df[keep].copy()

# Normalize names
rename = {
    "SourceID": "source_id",
    "CWPName": "facility_name",
    "CWPState": "state",
    "CWPCity": "city",
    "CWPStateDistrict": "state_district",
    "FacStdCountyName": "county",
    "FacLat": "lat",
    "FacLong": "lon",
    "CWPTotalDesignFlowNmbr": "design_flow",
    "CWPEffectiveDate": "permit_effective_date",
    "AcsPopulationDensity": "acs_pop_density",
    "PercentPeopleOfColor": "pct_people_of_color",
    "FacFederalAgencyName": "federal_agency",
    "CWPIndianCntryFlg": "in_indian_country_flag",
    "FacIndianSpatialFlg": "indian_spatial_flag",
}
out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})

# Types
for c in ["lat", "lon", "design_flow", "acs_pop_density", "pct_people_of_color"]:
    if c in out.columns:
        out[c] = pd.to_numeric(out[c], errors="coerce")

for c in ["source_id", "facility_name", "state", "city", "county", "state_district", "statute", "federal_agency"]:
    if c in out.columns:
        out[c] = out[c].astype("string")

if "permit_effective_date" in out.columns:
    out["permit_effective_date"] = pd.to_datetime(out["permit_effective_date"], errors="coerce")

# Ensure state present
if "state" not in out.columns:
    out["state"] = STATE

# Drop rows without geo (we need this for HUC joins)
if "lat" in out.columns and "lon" in out.columns:
    out = out.dropna(subset=["lat", "lon"])

# Basic de-dupe
out = out.drop_duplicates()

out.to_parquet(OUT_PATH, index=False)

print("Saved:", OUT_PATH)
print("Rows:", len(out))
print("Columns:", out.columns.tolist())