from pathlib import Path
import geopandas as gpd


def load_huc4(gdb_path: str) -> gpd.GeoDataFrame:
    layer = "WBDHU4"
    gdf = gpd.read_file(gdb_path, layer=layer)

    keep_cols = [c for c in ["huc4", "name", "states", "geometry"] if c in gdf.columns]
    gdf = gdf[keep_cols].copy()

    if "name" in gdf.columns:
        gdf = gdf.rename(columns={"name": "watershed_name"})

    if "huc4" in gdf.columns:
        gdf["huc4"] = gdf["huc4"].astype(str).str.zfill(4)

    return gdf


def enrich_huc4(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()

    gdf_area = gdf.to_crs("EPSG:5070")
    gdf["area_km2"] = gdf_area.geometry.area / 1_000_000.0

    # Centroids: compute in projected CRS, then convert to lat/lon
    gdf_proj = gdf.to_crs("EPSG:5070")
    cent_proj = gdf_proj.geometry.centroid
    cent_ll = gpd.GeoSeries(cent_proj, crs="EPSG:5070").to_crs("EPSG:4326")

    gdf["centroid_lon"] = cent_ll.x
    gdf["centroid_lat"] = cent_ll.y

    return gdf


def save_outputs(gdf: gpd.GeoDataFrame, out_dir: str = "data/processed/wbd") -> None:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(Path(out_dir) / "huc4.parquet", index=False)
    gdf.to_file(Path(out_dir) / "huc4.geojson", driver="GeoJSON")


if __name__ == "__main__":
    gdb_path = "data/external/wbd/WBD_National_GDB.gdb"

    gdf = load_huc4(gdb_path)
    gdf = enrich_huc4(gdf)
    save_outputs(gdf)

    print(f"Saved {len(gdf):,} HUC4 watersheds.")