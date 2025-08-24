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
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1.2rem;
        border-radius: 0.8rem;
        border-left: 4px solid #1f77b4;
        height: 220px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: transform 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }
    .metric-card h4 {
        font-size: 0.9rem;
        margin-bottom: 0.8rem;
        color: #2c3e50;
        line-height: 1.2;
    }
    .metric-card p {
        margin: 0.3rem 0;
        font-size: 0.85rem;
        line-height: 1.3;
    }
    .metric-card .value-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.5rem;
    }
    .metric-card .current-value {
        font-weight: bold;
        color: #1f77b4;
        font-size: 1.1rem;
    }
    .metric-card .previous-value {
        font-size: 0.8rem;
        color: #6c757d;
        font-style: italic;
    }
    .data-source {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .stApp {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    .main .block-container {
        background: rgba(255, 255, 255, 0.95);
        border-radius: 1rem;
        padding: 2rem;
        margin: 1rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        backdrop-filter: blur(10px);
    }
    .update-schedule {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 0.8rem;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .update-schedule h3 {
        color: #2c3e50;
        margin-bottom: 1rem;
    }
    .update-metric {
        text-align: center;
    }
    .update-metric .metric-label {
        font-size: 1.1rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .update-metric .metric-value {
        font-size: 0.85rem;
        color: #6c757d;
        line-height: 1.3;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load data from DuckDB database"""
    try:
        conn = duckdb.connect('gas_prices.duckdb')
        
        # Get latest data for each source
        # Special handling for AAA to get "Current" instead of "Yesterday"
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
            WHERE (source != 'aaa_gas_prices' AND scraped_at = (
                SELECT MAX(scraped_at) 
                FROM gas_prices p2 
                WHERE p2.source = gas_prices.source
            ))
            OR (source = 'aaa_gas_prices' AND timestamp LIKE '%Current%')
            ORDER BY scraped_at DESC
        ''').fetchdf()
        
        # Get previous data for each source (second most recent)
        previous_data = conn.execute('''
            SELECT 
                source,
                price,
                timestamp,
                consensus,
                surprise,
                scraped_at
            FROM gas_prices p1
            WHERE (source != 'aaa_gas_prices' AND scraped_at = (
                SELECT MAX(scraped_at) 
                FROM gas_prices p2 
                WHERE p2.source = p1.source 
                AND p2.scraped_at < (
                    SELECT MAX(scraped_at) 
                    FROM gas_prices p3 
                    WHERE p3.source = p1.source
                )
            ))
            OR (source = 'aaa_gas_prices' AND timestamp LIKE '%Yesterday%')
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
        return latest_data, previous_data, historical_data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

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
    latest_data, previous_data, historical_data = load_data()
    
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
        previous_source_data = previous_data[previous_data['source'] == config['source']]
        
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
                    
                    # Get previous value
                    previous_value = None
                    if not previous_source_data.empty:
                        prev_row = previous_source_data.iloc[0]
                        if config['value_field'] == 'surprise' and 'surprise' in prev_row and not pd.isna(prev_row['surprise']):
                            previous_value = prev_row['surprise']
                        else:
                            previous_value = prev_row['price']
                    
                    # Format the value display
                    if config['source'] == 'tradingeconomics_gasoline_stocks':
                        value_display = f"{value:.3f}M"
                        if previous_value is not None:
                            previous_display = f"{previous_value:.3f}M"
                        else:
                            previous_display = ""
                    elif config['source'] == 'tradingeconomics_refinery_runs':
                        value_display = f"{value:.3f}M"
                        if previous_value is not None:
                            previous_display = f"{previous_value:.3f}M"
                        else:
                            previous_display = ""
                    elif config['source'] in ['marketwatch_wti_futures']:
                        value_display = f"${value:.2f}"
                        if previous_value is not None:
                            previous_display = f"${previous_value:.2f}"
                        else:
                            previous_display = ""
                    else:
                        value_display = f"${value:.3f}"
                        if previous_value is not None:
                            previous_display = f"${previous_value:.3f}"
                        else:
                            previous_display = ""
                    
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{config['name']}</h4>
                        <div class="value-row">
                            <span><strong>{value_label}:</strong></span>
                            <span class="current-value">{value_display}</span>
                        </div>
                        <div class="value-row">
                            <span class="previous-value">Previous: {previous_display if previous_display else ''}</span>
                        </div>
                        <p><strong>Date:</strong> {row['timestamp']}</p>
                        <p><strong>Updated:</strong> {updated_time}</p>
                    </div>
                    """, unsafe_allow_html=True)
                except Exception as e:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{config['name']}</h4>
                        <div class="value-row">
                            <span><strong>Value:</strong></span>
                            <span class="current-value">Error</span>
                        </div>
                        <div class="value-row">
                            <span class="previous-value">Previous: </span>
                        </div>
                        <p><strong>Date:</strong> Error</p>
                        <p><strong>Updated:</strong> Error</p>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            with cols[idx]:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>{config['name']}</h4>
                    <div class="value-row">
                        <span><strong>Value:</strong></span>
                        <span class="current-value">No data</span>
                    </div>
                    <div class="value-row">
                        <span class="previous-value">Previous: </span>
                    </div>
                    <p><strong>Date:</strong> No data</p>
                    <p><strong>Updated:</strong> No data</p>
                </div>
                """, unsafe_allow_html=True)
    
    # Next Update Information
    st.markdown("---")
    st.markdown('<div class="update-schedule">', unsafe_allow_html=True)
    st.subheader("Next Update Schedule")
    
    next_updates = get_next_update_info()
    
    update_cols = st.columns(4)
    
    with update_cols[0]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">GasBuddy</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">Every 10 min<br>Next: {next_updates["gasbuddy"].strftime("%H:%M") if isinstance(next_updates["gasbuddy"], datetime) else next_updates["gasbuddy"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with update_cols[1]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">AAA</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">Daily 12:01 AM PT<br>Next: {next_updates["aaa"].strftime("%m/%d %H:%M") if isinstance(next_updates["aaa"], datetime) else next_updates["aaa"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with update_cols[2]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">RBOB & WTI</div>', unsafe_allow_html=True)
        if isinstance(next_updates['rbob_wti'], datetime):
            st.markdown(f'<div class="metric-value">Every 2h Mon-Fri<br>Next: {next_updates["rbob_wti"].strftime("%H:%M")}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="metric-value">{next_updates["rbob_wti"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with update_cols[3]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">EIA Data</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">Daily 10:35 AM EST<br>Next: {next_updates["eia"].strftime("%m/%d %H:%M") if isinstance(next_updates["eia"], datetime) else next_updates["eia"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
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
