import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import geopandas as gpd
from shapely.geometry import Point
from sklearn.neighbors import BallTree

INPUT_FILE = "VF_Hackathon_Dataset_India_Large.xlsx"

LAT_COL = "latitude"
LON_COL = "longitude"
SPECIALTY_COL = "specialties"
NAME_COL = "name"

st.set_page_config(layout="wide")
st.title("India Healthcare Coverage Map")

# -----------------------------
# Load data
# -----------------------------
df = pd.read_excel(INPUT_FILE)

df = df.dropna(subset=[LAT_COL, LON_COL])
df = df[
    (df[LAT_COL].between(6, 38)) &
    (df[LON_COL].between(68, 98))
].copy()

df[SPECIALTY_COL] = df[SPECIALTY_COL].fillna("").astype(str)

# -----------------------------
# Load India boundary
# -----------------------------
world_url = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"
world = gpd.read_file(world_url)

india = world[world["NAME"] == "India"].to_crs("EPSG:4326")
india_polygon = india.geometry.iloc[0]

# -----------------------------
# Specialty dropdown
# -----------------------------
all_specialties = sorted(
    set(
        s.strip()
        for cell in df[SPECIALTY_COL]
        for s in cell.replace("[", "").replace("]", "").replace('"', "").split(",")
        if s.strip()
    )
)

selected_specialty = st.selectbox("Select medical specialty", all_specialties)

filtered = df[df[SPECIALTY_COL].str.contains(selected_specialty, case=False, na=False)]

st.write(f"Facilities with **{selected_specialty}**: {len(filtered)}")

if len(filtered) == 0:
    st.warning("No facilities found for this specialty.")
    st.stop()

# -----------------------------
# Create grid ONLY inside India
# -----------------------------
lat_grid = np.linspace(6, 38, 170)
lon_grid = np.linspace(68, 98, 170)

grid_points = []

for lat in lat_grid:
    for lon in lon_grid:
        point = Point(lon, lat)  # shapely uses lon, lat
        if india_polygon.contains(point):
            grid_points.append([lat, lon])

grid_points = np.array(grid_points)

# -----------------------------
# Distance to nearest facility
# -----------------------------
earth_radius_km = 6371

facility_rad = np.radians(filtered[[LAT_COL, LON_COL]].values)
grid_rad = np.radians(grid_points)

tree = BallTree(facility_rad, metric="haversine")
distances, _ = tree.query(grid_rad, k=1)

grid_df = pd.DataFrame({
    "latitude": grid_points[:, 0],
    "longitude": grid_points[:, 1],
    "nearest_distance_km": distances[:, 0] * earth_radius_km
})

max_distance = np.percentile(grid_df["nearest_distance_km"], 95)
grid_df["distance_clipped"] = grid_df["nearest_distance_km"].clip(0, max_distance)

# -----------------------------
# Plot
# -----------------------------
fig = go.Figure()

fig.add_trace(go.Scattermapbox(
    lat=grid_df["latitude"],
    lon=grid_df["longitude"],
    mode="markers",
    marker=dict(
        size=7,
        color=grid_df["distance_clipped"],
        colorscale=[
            [0.0, "green"],
            [0.5, "yellow"],
            [1.0, "red"],
        ],
        opacity=0.6,
        colorbar=dict(title="Distance [km]")
    ),
    text=grid_df["nearest_distance_km"].round(1).astype(str) + " km",
    hovertemplate="Distance to nearest facility: %{text}<extra></extra>",
    name="Coverage distance"
))

fig.add_trace(go.Scattermapbox(
    lat=filtered[LAT_COL],
    lon=filtered[LON_COL],
    mode="markers",
    marker=dict(size=7, color="blue"),
    text=filtered[NAME_COL] if NAME_COL in filtered.columns else None,
    hovertemplate="%{text}<extra></extra>",
    name="Matching facilities"
))

# India border outline
for geom in india.geometry:
    if geom.geom_type == "Polygon":
        x, y = geom.exterior.xy
        fig.add_trace(go.Scattermapbox(
            lon=list(x),
            lat=list(y),
            mode="lines",
            line=dict(width=2, color="black"),
            name="India border"
        ))

    elif geom.geom_type == "MultiPolygon":
        for poly in geom.geoms:
            x, y = poly.exterior.xy
            fig.add_trace(go.Scattermapbox(
                lon=list(x),
                lat=list(y),
                mode="lines",
                line=dict(width=2, color="black"),
                name="India border"
            ))

fig.update_layout(
    mapbox=dict(
        style="carto-positron",
        center=dict(lat=22.5, lon=79),
        zoom=4
    ),
    height=850,
    margin=dict(l=0, r=0, t=50, b=0),
    title=f"Distance to nearest facility with: {selected_specialty}"
)

st.plotly_chart(fig, use_container_width=True)