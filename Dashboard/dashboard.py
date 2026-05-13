"""
dashboard.py — Enterprise Aviation Analytics Platform Dashboard
Execution: streamlit run Dashboard/dashboard.py
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px

# Configure professional wide-screen workspace layout
st.set_page_config(page_title="Aviation Data Platform BI", layout="wide")

# Map paths relative to the project directory structures
BASE_DIR  = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR  = os.path.join(BASE_DIR, "data", "processed")

@st.cache_data
def load_data():
    flights  = pd.read_csv(os.path.join(DATA_DIR, "fact_flights.csv"))
    airlines = pd.read_csv(os.path.join(DATA_DIR, "dim_airline.csv"))
    routes   = pd.read_csv(os.path.join(DATA_DIR, "dim_route.csv"))
    airports = pd.read_csv(os.path.join(DATA_DIR, "dim_airport.csv"))
    return flights, airlines, routes, airports

try:
    df_f, df_al, df_rt, df_ap = load_data()

    # --- HUMAN ENGINEERING: SIDEBAR INTERACTIVE FILTERS ---
    st.sidebar.header("Data Filter Console")
    
    # Generate unique carrier choices for drop-down matrix filtering
    carrier_list = sorted(df_f["airline_code"].dropna().unique().tolist())
    selected_carrier = st.sidebar.selectbox("Select Transport Carrier", ["All Carriers"] + carrier_list)
    
    # Apply dynamic filter constraints based on user choice
    if selected_carrier != "All Carriers":
        filtered_f = df_f[df_f["airline_code"] == selected_carrier]
        filtered_rt = df_rt[((df_rt["departure_airport"].isin(filtered_f["departure_airport"])) | 
                             (df_rt["arrival_airport"].isin(filtered_f["arrival_airport"])))]
    else:
        filtered_f = df_f
        filtered_rt = df_rt

    # --- CLEAN CORPORATE HEADERS (NO AI ICONS) ---
    st.title("Global Aviation Platform — Business Intelligence Suite")
    st.markdown("##### Core Infrastructure Layer: PySpark Data Compute Engine | DuckDB Storage Architecture | Streamlit UI")
    st.markdown("---")

    # ------------------ DYNAMIC KPI SCORECARDS ------------------
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Flights Processed", f"{len(filtered_f):,}")
    with c2:
        st.metric("Total Ticket Revenue Gen", f"${filtered_f['ticket_revenue_usd'].sum():,.2f}")
    with c3:
        st.metric("Total Passengers Boarded", f"{filtered_f['passengers_boarded'].sum():,}")
    with c4:
        st.metric("System Mean Delay (min)", f"{filtered_f['delay_minutes'].mean():.2f}m")
    
    st.markdown("---")

    # ------------------ VISUALIZATION ROWS ------------------
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Delay Category Disruption Footprint")
        delay_counts = filtered_f["delay_category"].value_counts().reset_index()
        delay_counts.columns = ["Delay Tier", "Flight Records"]
        
        fig_pie = px.pie(
            delay_counts, names="Delay Tier", values="Flight Records",
            hole=0.4, color_discrete_sequence=px.colors.sequential.YlOrRd_r,
            template="plotly_dark"
        )
        fig_pie.update_layout(margin=dict(t=20, b=20, l=20, r=20), height=350)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader("Performance Lead Board: Top 10 Carriers by Revenue")
        # Keep carrier overview clear and sorted cleanly
        top_al = df_al.sort_values("total_revenue_usd", ascending=False).head(10)
        fig_carrier = px.bar(
            top_al, x="airline_code", y="total_revenue_usd",
            color="avg_delay_min", color_continuous_scale="Viridis",
            labels={"airline_code": "Carrier ID", "total_revenue_usd": "Gross Yield ($)", "avg_delay_min": "Avg Delay"},
            template="plotly_dark"
        )
        fig_carrier.update_layout(margin=dict(t=20, b=20, l=20, r=20), height=350)
        st.plotly_chart(fig_carrier, use_container_width=True)

    # ------------------ GRID ANALYTICAL TABLES ------------------
    st.markdown("---")
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("Regional Hub Node Operations: Top 10 Busiest Terminals")
        top_ap = df_ap.sort_values("departures", ascending=False).head(10)
        st.dataframe(
            top_ap[["departure_airport", "dep_city", "dep_country", "departures", "total_passengers_departed"]],
            use_container_width=True, hide_index=True
        )

    with col4:
        st.subheader("Enterprise Transit Efficiency Matrix: Top 10 High-Traffic Sectors")
        top_rt = filtered_rt.sort_values("flight_count", ascending=False).head(10)
        st.dataframe(
            top_rt[["departure_airport", "arrival_airport", "dep_city", "arr_city", "flight_count", "avg_distance_km", "avg_revenue_usd"]],
            use_container_width=True, hide_index=True
        )

except FileNotFoundError as e:
    st.error(f"Structural Platform Error — Analytical Assets Missing: {e}")