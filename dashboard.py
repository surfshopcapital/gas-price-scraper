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
        height: 200px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
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

def get_next_update_info():
    """Get information about next scheduled updates"""
    now = datetime.now()
    
    # GasBuddy: every 10 minutes
    next_gasbuddy = now + timedelta(minutes=10 - (now.minute % 10))
    next_gasbuddy = next_gasbuddy.replace(second=0, microsecond=0)
    
    # AAA: daily at 12:01 AM Pacific (3:01 AM UTC)
    next_aaa = now.replace(hour=3, minute=1, second=0, microsecond=0)
    if next_aaa <= now:
        next_aaa += timedelta(days=1)
    
    # RBOB & WTI: every 2 hours Mon-Fri during market hours
    if now.weekday() < 5:  # Monday to Friday
        next_rbob_wti = now.replace(minute=0, second=0, microsecond=0)
        if next_rbob_wti <= now:
            next_rbob_wti += timedelta(hours=2)
    else:
        next_rbob_wti = "Weekend - No updates"
    
    # Gasoline Stocks & Refinery Runs: daily at 10:35 AM EST (15:35 UTC)
    next_eia = now.replace(hour=15, minute=35, second=0, microsecond=0)
    if next_eia <= now:
        next_eia += timedelta(days=1)
    
    return {
        'gasbuddy': next_gasbuddy,
        'aaa': next_aaa,
        'rbob_wti': next_rbob_wti,
        'eia': next_eia
    }

def main():
    st.markdown('<h1 class="main-header">Gas Price Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    latest_data, historical_data = load_data()
    
    if latest_data is None:
        st.error("Failed to load data. Please check if the database exists and contains data.")
        return
    
    # Sidebar filters
    st.sidebar.header("Dashboard Controls")
    
    # Time range filter
    time_range = st.sidebar.selectbox(
        "Time Range for Charts",
        ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
        index=1
    )
    
    # Latest data overview - 6 cards in single row
    st.header("Latest Data Overview")
    
    # Create 6 equal-width columns for all sources
    cols = st.columns(6)
    
    # Define source mappings with new names and data sources
    source_configs = [
        {
            'source': 'gasbuddy_fuel_insights',
            'name': 'GasBuddy National Average (Live Feed)',
            'value_field': 'price',
            'display_name': 'GasBuddy'
        },
        {
            'source': 'aaa_gas_prices',
            'name': 'AAA Gas National Average (Current)',
            'value_field': 'price',
            'display_name': 'AAA'
        },
        {
            'source': 'marketwatch_rbob_futures',
            'name': 'RBOB Futures',
            'value_field': 'price',
            'display_name': 'RBOB'
        },
        {
            'source': 'marketwatch_wti_futures',
            'name': 'WTI Futures',
            'value_field': 'price',
            'display_name': 'WTI'
        },
        {
            'source': 'tradingeconomics_gasoline_stocks',
            'name': 'Gasoline Stocks Change',
            'value_field': 'surprise',  # Use surprise instead of price
            'display_name': 'Gasoline Stocks'
        },
        {
            'source': 'tradingeconomics_refinery_runs',
            'name': 'Refinery Runs Change',
            'value_field': 'price',
            'display_name': 'Refinery Runs'
        }
    ]
    
    # Display each card
    for idx, config in enumerate(source_configs):
        source_data = latest_data[latest_data['source'] == config['source']]
        if not source_data.empty:
            row = source_data.iloc[0]
            with cols[idx]:
                try:
                    updated_time = row['scraped_at'].strftime('%H:%M') if not pd.isna(row['scraped_at']) else "N/A"
                    
                    # Get the appropriate value based on config
                    if config['value_field'] == 'surprise' and 'surprise' in row and not pd.isna(row['surprise']):
                        value = row['surprise']
                        value_label = "Surprise"
                    else:
                        value = row['price']
                        value_label = "Value"
                    
                    # Format the value display
                    if config['source'] == 'tradingeconomics_gasoline_stocks':
                        value_display = f"{value:.3f}M"
                    elif config['source'] == 'tradingeconomics_refinery_runs':
                        value_display = f"{value:.3f}M"
                    elif config['source'] in ['marketwatch_wti_futures']:
                        value_display = f"${value:.2f}"
                    else:
                        value_display = f"${value:.3f}"
                    
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{config['name']}</h4>
                        <p><strong>{value_label}:</strong> {value_display}</p>
                        <p><strong>Date:</strong> {row['timestamp']}</p>
                        <p><strong>Updated:</strong> {updated_time}</p>
                    </div>
                    """, unsafe_allow_html=True)
                except Exception as e:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{config['name']}</h4>
                        <p><strong>Value:</strong> Error</p>
                        <p><strong>Date:</strong> Error</p>
                        <p><strong>Updated:</strong> Error</p>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            with cols[idx]:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>{config['name']}</h4>
                    <p><strong>Value:</strong> No data</p>
                    <p><strong>Date:</strong> No data</p>
                    <p><strong>Updated:</strong> No data</p>
                </div>
                """, unsafe_allow_html=True)
    
    # Next Update Information
    st.markdown("---")
    st.subheader("Next Update Schedule")
    
    next_updates = get_next_update_info()
    
    update_cols = st.columns(4)
    
    with update_cols[0]:
        st.metric("GasBuddy", f"Every 10 min\nNext: {next_updates['gasbuddy'].strftime('%H:%M') if isinstance(next_updates['gasbuddy'], datetime) else next_updates['gasbuddy']}")
    
    with update_cols[1]:
        st.metric("AAA", f"Daily 12:01 AM PT\nNext: {next_updates['aaa'].strftime('%m/%d %H:%M') if isinstance(next_updates['aaa'], datetime) else next_updates['aaa']}")
    
    with update_cols[2]:
        if isinstance(next_updates['rbob_wti'], datetime):
            st.metric("RBOB & WTI", f"Every 2h Mon-Fri\nNext: {next_updates['rbob_wti'].strftime('%H:%M')}")
        else:
            st.metric("RBOB & WTI", next_updates['rbob_wti'])
    
    with update_cols[3]:
        st.metric("EIA Data", f"Daily 10:35 AM EST\nNext: {next_updates['eia'].strftime('%m/%d %H:%M') if isinstance(next_updates['eia'], datetime) else next_updates['eia']}")
    
    # Download CSV button for full database
    st.markdown("---")
    st.subheader("Download Data")
    
    if st.button("Download Full Database as CSV"):
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
                label="Download CSV",
                data=csv,
                file_name=f"gas_prices_database_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.error(f"Error downloading data: {e}")
    
    # Summary metrics
    st.markdown("---")
    st.subheader("Summary Statistics")
    
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
    
    # Footer
    st.markdown("---")
    st.markdown("Dashboard updated every 5 minutes | Data from multiple sources including GasBuddy, AAA, MarketWatch, and Trading Economics")

if __name__ == "__main__":
    main()
