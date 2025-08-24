import streamlit as st
import pandas as pd
import duckdb
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os

# Page configuration
st.set_page_config(
    page_title="Gas Price Dashboard",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .data-source {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load data from DuckDB database"""
    try:
        conn = duckdb.connect('gas_prices.duckdb')
        
        # Get latest data for each source
        latest_data = conn.execute('''
            SELECT 
                source,
                fuel_type,
                price,
                timestamp,
                region,
                consensus,
                surprise,
                scraped_at
            FROM gas_prices 
            WHERE scraped_at = (
                SELECT MAX(scraped_at) 
                FROM gas_prices p2 
                WHERE p2.source = gas_prices.source
            )
            ORDER BY scraped_at DESC
        ''').fetchdf()
        
        # Get historical data for charts
        historical_data = conn.execute('''
            SELECT 
                source,
                fuel_type,
                price,
                timestamp,
                region,
                consensus,
                surprise,
                scraped_at
            FROM gas_prices 
            WHERE scraped_at >= CURRENT_DATE - INTERVAL 30 DAY
            ORDER BY scraped_at ASC
        ''').fetchdf()
        
        conn.close()
        return latest_data, historical_data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None

def main():
    st.markdown('<h1 class="main-header">⛽ Gas Price Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    latest_data, historical_data = load_data()
    
    if latest_data is None:
        st.error("Failed to load data. Please check if the database exists and contains data.")
        return
    
    # Sidebar filters
    st.sidebar.header("📊 Dashboard Controls")
    
    # Time range filter
    time_range = st.sidebar.selectbox(
        "Time Range for Charts",
        ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
        index=1
    )
    
    # Latest data overview - 6 clean cards
    st.header("📊 Latest Data Overview")
    
    # Create 6 clean metric cards in 2 rows of 3
    row1_cols = st.columns(3)
    row2_cols = st.columns(3)
    
    # First row: GasBuddy, AAA, RBOB
    sources_row1 = ['gasbuddy_fuel_insights', 'aaa_gas_prices', 'marketwatch_rbob_futures']
    for idx, source in enumerate(sources_row1):
        source_data = latest_data[latest_data['source'] == source]
        if not source_data.empty:
            row = source_data.iloc[0]
            with row1_cols[idx]:
                try:
                    updated_time = row['scraped_at'].strftime('%H:%M') if not pd.isna(row['scraped_at']) else "N/A"
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{source.replace('_', ' ').title()}</h4>
                        <p><strong>Price:</strong> ${row['price']:.3f}</p>
                        <p><strong>Date:</strong> {row['timestamp']}</p>
                        <p><strong>Updated:</strong> {updated_time}</p>
                    </div>
                    """, unsafe_allow_html=True)
                except Exception as e:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{source.replace('_', ' ').title()}</h4>
                        <p><strong>Price:</strong> ${row['price']:.3f}</p>
                        <p><strong>Date:</strong> {row['timestamp']}</p>
                        <p><strong>Updated:</strong> Error</p>
                    </div>
                    """, unsafe_allow_html=True)
    
    # Second row: WTI, Gasoline Stocks, Refinery Runs
    sources_row2 = ['marketwatch_wti_futures', 'tradingeconomics_gasoline_stocks', 'tradingeconomics_refinery_runs']
    for idx, source in enumerate(sources_row2):
        source_data = latest_data[latest_data['source'] == source]
        if not source_data.empty:
            row = source_data.iloc[0]
            with row2_cols[idx]:
                try:
                    updated_time = row['scraped_at'].strftime('%H:%M') if not pd.isna(row['scraped_at']) else "N/A"
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{source.replace('_', ' ').title()}</h4>
                        <p><strong>Price:</strong> ${row['price']:.3f}</p>
                        <p><strong>Date:</strong> {row['timestamp']}</p>
                        <p><strong>Updated:</strong> {updated_time}</p>
                    </div>
                    """, unsafe_allow_html=True)
                except Exception as e:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{source.replace('_', ' ').title()}</h4>
                        <p><strong>Price:</strong> ${row['price']:.3f}</p>
                        <p><strong>Date:</strong> {row['timestamp']}</p>
                        <p><strong>Updated:</strong> Error</p>
                    </div>
                    """, unsafe_allow_html=True)
    
    # Download CSV button for full database
    st.markdown("---")
    st.subheader("📥 Download Data")
    
    if st.button("📊 Download Full Database as CSV"):
        try:
            conn = duckdb.connect('gas_prices.duckdb')
            all_data = conn.execute('''
                SELECT source, price, timestamp, region, fuel_type, consensus, surprise, scraped_at
                FROM gas_prices 
                ORDER BY scraped_at DESC
            ''').fetchdf()
            conn.close()
            
            csv = all_data.to_csv(index=False)
            st.download_button(
                label="💾 Download CSV",
                data=csv,
                file_name=f"gas_prices_database_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.error(f"Error downloading data: {e}")
    
    # Summary metrics
    st.markdown("---")
    st.subheader("📈 Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Data Sources", len(latest_data))
    
    with col2:
        st.metric("Total Records", len(historical_data))
    
    with col3:
        st.metric("Database Size", f"{os.path.getsize('gas_prices.duckdb') / 1024:.1f} KB")
    
    with col4:
        try:
            last_update = latest_data['scraped_at'].max()
            if pd.isna(last_update):
                st.metric("Last Update", "No data")
            else:
                st.metric("Last Update", last_update.strftime('%H:%M'))
        except Exception as e:
            st.metric("Last Update", "Error")
    
    # GasBuddy Chart (most frequent updates)
    st.header("📈 GasBuddy Price Trend")
    gasbuddy_data = historical_data[historical_data['source'] == 'gasbuddy_fuel_insights']
    if not gasbuddy_data.empty:
        fig = px.line(gasbuddy_data, x='scraped_at', y='price', 
                     title='GasBuddy Fuel Insights - Price Trend Over Time', 
                     markers=True,
                     labels={'price': 'Price ($)', 'scraped_at': 'Time'})
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Price ($)",
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No GasBuddy data available for charting")
    
    # All Sources Overview
    st.header("📊 All Sources Data")
    
    # Show data for all sources
    for source in latest_data['source'].unique():
        source_data = historical_data[historical_data['source'] == source]
        if not source_data.empty:
            st.subheader(f"{source.replace('_', ' ').title()}")
            
            # Show data table
            display_cols = ['price', 'timestamp', 'region', 'scraped_at']
            if 'consensus' in source_data.columns and 'surprise' in source_data.columns:
                display_cols.extend(['consensus', 'surprise'])
            
            st.dataframe(source_data[display_cols].tail(10))
            
            # Show source-specific chart if multiple data points
            if len(source_data) > 1:
                fig = px.line(source_data, x='scraped_at', y='price', 
                             title=f'{source.replace("_", " ").title()} - Price Trend',
                             markers=True)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No data available for {source}")
    
    # Footer
    st.markdown("---")
    st.markdown("Dashboard updated every 5 minutes | Data from multiple sources including GasBuddy, AAA, MarketWatch, and Trading Economics")

if __name__ == "__main__":
    main()
