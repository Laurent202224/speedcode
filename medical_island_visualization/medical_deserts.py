import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import geopandas as gpd
from shapely.geometry import Point
from sklearn.neighbors import BallTree
from collections import Counter

INPUT_FILE = "VF_Hackathon_Dataset_India_Large_scored.xlsx"

LAT_COL = "latitude"
LON_COL = "longitude"
SPECIALTY_COL = "specialties"
NAME_COL = "name"

MAX_DISTANCE = 150  # km, fixed color scale

st.set_page_config(layout="wide")
st.title("India Healthcare Coverage Map")
tab1, tab2 = st.tabs([
    "Coverage by Specialty",
    "Regional Trust Score"
])


df = pd.read_excel(INPUT_FILE)

df = df.dropna(subset=[LAT_COL, LON_COL])
df = df[
        (df[LAT_COL].between(6, 38)) &
        (df[LON_COL].between(68, 98))
    ].copy()

df[SPECIALTY_COL] = df[SPECIALTY_COL].fillna("").astype(str)

world_url = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"
world = gpd.read_file(world_url)

india = world[world["NAME"] == "India"].to_crs("EPSG:4326")
india_polygon = india.geometry.iloc[0]



with tab1:

    def parse_specialties(cell):
        return [
            s.strip()
            for s in str(cell)
            .replace("[", "")
            .replace("]", "")
            .replace('"', "")
            .replace("'", "")
            .split(",")
            if s.strip()
        ]


    specialty_counter = Counter()
    for cell in df[SPECIALTY_COL]:
        specialty_counter.update(parse_specialties(cell))

    specialty_options = [
        f"{specialty} ({count})"
        for specialty, count in specialty_counter.most_common()
    ]

    selected_specialty_label = st.selectbox(
        "Select medical specialty",
        specialty_options
    )

    selected_specialty = selected_specialty_label.rsplit(" (", 1)[0]

    filtered = df[df[SPECIALTY_COL].str.contains(selected_specialty, case=False, na=False)]

    st.write(f"Facilities with **{selected_specialty}**: {len(filtered)}")

    if len(filtered) == 0:
        st.warning("No facilities found for this specialty.")
        st.stop()

    lat_grid = np.linspace(6, 38, 170)
    lon_grid = np.linspace(68, 98, 170)

    grid_points = []

    for lat in lat_grid:
        for lon in lon_grid:
            point = Point(lon, lat)
            if india_polygon.contains(point):
                grid_points.append([lat, lon])

    grid_points = np.array(grid_points)

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

    grid_df["distance_clipped"] = grid_df["nearest_distance_km"].clip(0, MAX_DISTANCE)

    fig = go.Figure()

    fig.add_trace(go.Scattermapbox(
        lat=grid_df["latitude"],
        lon=grid_df["longitude"],
        mode="markers",
        marker=dict(
            size=7,
            color=grid_df["distance_clipped"],
            cmin=0,
            cmax=MAX_DISTANCE,
            colorscale=[
                [0.0, "green"],
                [0.35, "yellow"],
                [1.0, "red"],
            ],
            opacity=0.6,
            colorbar=dict(
                title="Distance [km]",
                tickmode="array",
                tickvals=[0, 25, 50, 75, 100, 125, 150],
                ticktext=["0", "25", "50", "75", "100", "125", "150+"],
            )
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

    for geom in india.geometry:
        if geom.geom_type == "Polygon":
            x, y = geom.exterior.xy
            fig.add_trace(go.Scattermapbox(
                lon=list(x),
                lat=list(y),
                mode="lines",
                line=dict(width=2, color="black"),
                showlegend=False,
                hoverinfo="skip"
            ))

        elif geom.geom_type == "MultiPolygon":
            for poly in geom.geoms:
                x, y = poly.exterior.xy
                fig.add_trace(go.Scattermapbox(
                    lon=list(x),
                    lat=list(y),
                    mode="lines",
                    line=dict(width=2, color="black"),
                    showlegend=False,
                    hoverinfo="skip"
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

with tab2:
    # -----------------------------
# Trust score regional view
# -----------------------------

    TRUST_COL = "trust_score"

    st.subheader("Regional Trust Score Map")

    if TRUST_COL not in df.columns:
        st.error(f"Column '{TRUST_COL}' not found in dataset.")
        st.stop()

    trust_df = df.dropna(subset=[LAT_COL, LON_COL, TRUST_COL]).copy()

    trust_df[TRUST_COL] = pd.to_numeric(trust_df[TRUST_COL], errors="coerce")
    trust_df = trust_df.dropna(subset=[TRUST_COL])

    st.write(f"Facilities with trust score: {len(trust_df)}")

    if len(trust_df) == 0:
        st.warning("No valid trust scores found.")
        st.stop()

    # Grid only inside India
    lat_grid = np.linspace(6, 38, 170)
    lon_grid = np.linspace(68, 98, 170)

    grid_points = []

    for lat in lat_grid:
        for lon in lon_grid:
            point = Point(lon, lat)
            if india_polygon.contains(point):
                grid_points.append([lat, lon])

    grid_points = np.array(grid_points)

    # Find nearby facilities for each grid point
    earth_radius_km = 6371

    facility_rad = np.radians(trust_df[[LAT_COL, LON_COL]].values)
    grid_rad = np.radians(grid_points)

    tree = BallTree(facility_rad, metric="haversine")

    K_NEIGHBORS = 10
    distances, indices = tree.query(
        grid_rad,
        k=min(K_NEIGHBORS, len(trust_df))
    )

    distances_km = distances * earth_radius_km

    trust_values = trust_df[TRUST_COL].values
    neighbor_scores = trust_values[indices]

    # Distance-weighted trust score
    weights = 1 / (distances_km + 1)
    weighted_trust = (neighbor_scores * weights).sum(axis=1) / weights.sum(axis=1)

    trust_grid_df = pd.DataFrame({
        "latitude": grid_points[:, 0],
        "longitude": grid_points[:, 1],
        "regional_trust_score": weighted_trust,
        "nearest_facility_distance_km": distances_km[:, 0]
    })

    fig_trust = go.Figure()

    fig_trust.add_trace(go.Scattermapbox(
        lat=trust_grid_df["latitude"],
        lon=trust_grid_df["longitude"],
        mode="markers",
        marker=dict(
            size=7,
            color=trust_grid_df["regional_trust_score"],
            cmin=0,
            cmax=10,
            colorscale=[
                [0.0, "red"],
                [0.5, "yellow"],
                [1.0, "green"],
            ],
            opacity=0.6,
            colorbar=dict(
                title="Regional Trust Score",
                tickmode="array",
                tickvals=[0, 2, 4, 6, 8, 10],
                ticktext=["0", "2", "4", "6", "8", "10"],
            )
        ),
        text=[
            f"Regional trust score: {score:.2f}<br>"
            f"Nearest facility: {dist:.1f} km"
            for score, dist in zip(
                trust_grid_df["regional_trust_score"],
                trust_grid_df["nearest_facility_distance_km"]
            )
        ],
        hovertemplate="%{text}<extra></extra>",
        name="Regional trust score"
    ))

    fig_trust.add_trace(go.Scattermapbox(
        lat=trust_df[LAT_COL],
        lon=trust_df[LON_COL],
        mode="markers",
        marker=dict(
            size=5,
            color=trust_df[TRUST_COL],
            cmin=0,
            cmax=10,
            colorscale=[
                [0.0, "red"],
                [0.5, "yellow"],
                [1.0, "green"],
            ],
            opacity=0.8,
        ),
        text=trust_df[NAME_COL] if NAME_COL in trust_df.columns else None,
        hovertemplate="%{text}<extra></extra>",
        name="Facilities"
    ))

    # India border
    for geom in india.geometry:
        if geom.geom_type == "Polygon":
            x, y = geom.exterior.xy
            fig_trust.add_trace(go.Scattermapbox(
                lon=list(x),
                lat=list(y),
                mode="lines",
                line=dict(width=2, color="black"),
                showlegend=False,
                hoverinfo="skip"
            ))

        elif geom.geom_type == "MultiPolygon":
            for poly in geom.geoms:
                x, y = poly.exterior.xy
                fig_trust.add_trace(go.Scattermapbox(
                    lon=list(x),
                    lat=list(y),
                    mode="lines",
                    line=dict(width=2, color="black"),
                    showlegend=False,
                    hoverinfo="skip"
                ))

    fig_trust.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=22.5, lon=79),
            zoom=4
        ),
        height=850,
        margin=dict(l=0, r=0, t=50, b=0),
        title="Regional Healthcare Trust Score"
    )

    st.plotly_chart(fig_trust, use_container_width=True)