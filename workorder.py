
import streamlit as st
import pandas as pd
import plotly.express as px

@st.cache_resource()
def prepare_data():
    # Load data
    data = pd.read_csv('data_dump.csv')

    # Filter properties
    properties = ['Cairo BU Office','Shanghai CPS Office/Plant (Zizhu)','Istanbul KO Building Office', 'Brussels Office', 'Wimpole Building']
    data = data[data.client_property_name.isin(properties)]

    # Replace property names
    data.client_property_name = data.client_property_name.replace({
        'Cairo BU Office': 'Loc A', 
        'Shanghai CPS Office/Plant (Zizhu)': 'Loc B', 
        'Istanbul KO Building Office': 'Loc C',  
        'Brussels Office': 'Loc D',  
        'Wimpole Building': 'Loc E'
    })

    # Convert date columns to datetime format
    data['local_workorder_creation_date_ts'] = pd.to_datetime(data['local_workorder_creation_date_ts'])
    data['local_target_completed_ts'] = pd.to_datetime(data['local_target_completed_ts'])
    data['local_actual_completed_ts'] = pd.to_datetime(data['local_actual_completed_ts'])
    data['year'] = data['local_workorder_creation_date_ts'].dt.year
    data['month'] = data['local_workorder_creation_date_ts'].dt.month
    data['day'] = data['local_workorder_creation_date_ts'].dt.day

    # Extract year and month from the completion date
    data['year_month'] = data['local_actual_completed_ts'].dt.to_period('M')

    # Group by client property name and count work orders
    # grouped_df = data.groupby('client_property_name')['workorder_number'].count().reset_index()
    # grouped_df = grouped_df.sort_values(by='workorder_number', ascending=False)
    return data

# Function to calculate month-over-month completion rate
def mom_completion_rate(data):
    monthly_completion_rate = data.groupby('year_month')['local_actual_completed_ts'].count()
    #return monthly_completion_rate
    st.header("Month-over-Month Completion Rate")
    st.line_chart(monthly_completion_rate)

# Function to count open and closed work orders
def open_closed_count(data):
    open_workorders = data['local_actual_completed_ts'].isna().sum()
    closed_workorders = data['local_actual_completed_ts'].notna().sum()
    #return open_workorders, closed_workorders
    st.header("Open vs Closed Work Orders")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="Open Work Orders", value=open_workorders)
    with col2:
        st.metric(label="Closed Work Orders", value=closed_workorders)
    
    # Alternative visualization with a pie chart
    chart_data = pd.DataFrame({
        'Status': ['Open', 'Closed'],
        'Count': [open_workorders, closed_workorders]
    })
    fig = px.pie(chart_data, values='Count', names='Status')
    st.plotly_chart(fig)

# Function to identify work orders that have status open even after local_target_completed_ts
def open_elapsed_WOs(data):
    open_after_target = data[data['local_actual_completed_ts'].isna() & (data['local_target_completed_ts'] < pd.Timestamp.now())]
    open_elapsed_WOs_df = open_after_target[['priority_code', 'workorder_number', 'client_property_name']]
    st.header("Open Work Orders After Target Completion Date")
    st.dataframe(open_elapsed_WOs_df)

# Function to calculate the count of work orders for each time period
def past_WO_counts(data):
    time_periods = {
        'last_30_days': 30,
        'last_90_days': 90,
        'last_180_days': 180,
        'last_year': 365
    }
    counts = {}
    current_date = pd.Timestamp.now()
    for period_name, days in time_periods.items():
        start_date = current_date - pd.Timedelta(days=days)
        counts[period_name] = data[data['local_workorder_creation_date_ts'] >= start_date].shape[0]
    df = pd.DataFrame(list(counts.items()), columns=['Time Period', 'Work Order Count'])
    #return df
    
    st.header("Work Order Counts in Past Time Periods")
    fig = px.bar(df, x='Time Period', y='Work Order Count', 
                 color='Time Period', text='Work Order Count')
    fig.update_layout(xaxis_title='Time Period', 
                     yaxis_title='Count',
                     showlegend=False)
    st.plotly_chart(fig)

# Function to compare work orders between years
def compare_WOs(data):
    current_year = data[data['year'] == pd.Timestamp.now().year]
    last_year = data[data['year'] == (pd.Timestamp.now().year - 1)]
    previous_year = data[data['year'] == (pd.Timestamp.now().year - 2)]
    current_year_counts = current_year.groupby('month')['local_workorder_creation_date_ts'].count()
    last_year_counts = last_year.groupby('month')['local_workorder_creation_date_ts'].count()
    previous_year_counts = previous_year.groupby('month')['local_workorder_creation_date_ts'].count()
    comparison_df = pd.DataFrame({
        'Year 2025': current_year_counts,
        'Year 2024': last_year_counts,
        'Year 2023': previous_year_counts
    }).fillna(0)
    #return comparison_df

    st.header("Comparison of Work Orders Between Years")
        # Style the dataframe with custom formatting
    styled_df = comparison_df.style.format('{:.0f}').set_properties(**{
        'background-color': '#f0f2f6',
        'border': '1px solid #d3d3d3',
        'text-align': 'center'
    }).set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#4e73df'), 
                                    ('color', 'white'),
                                    ('font-weight', 'bold'),
                                    ('text-align', 'center')]},
        {'selector': 'tr:hover', 'props': [('background-color', '#e6e9f0')]}
    ])
    
    st.dataframe(styled_df, use_container_width=True)
        
    # Alternative: Create a bar chart for visual comparison
    # Convert DataFrame to long format for Plotly
    comparison_df_melted = comparison_df.reset_index().melt(id_vars='month', var_name='Year', value_name='Count')
    fig = px.bar(comparison_df_melted, x='month', y='Count', color='Year')
    fig.update_layout(
        title="Monthly Work Order Comparison",
        xaxis=dict(title="Month"),
        yaxis=dict(title="Number of Work Orders"),
        legend_title="Year"
    )
    st.plotly_chart(fig, use_container_width=True)

# Function to analyze work order completion trends
def WO_completion_trends(data):
    monthly_completion_rate = data.groupby('year_month')['local_actual_completed_ts'].count()
    data['completion_time_diff'] = (data['local_actual_completed_ts'] - data['local_target_completed_ts']).dt.days
    monthly_completion_diff = data.groupby('year_month')['completion_time_diff'].mean()
    completion_trends_df = pd.DataFrame({
        'Work Orders Completed': monthly_completion_rate,
        'Average Completion Time Difference': monthly_completion_diff
    }).fillna(0)
    #return monthly_completion_rate, monthly_completion_diff, completion_trends_df

    #monthly_completion_rate, monthly_completion_diff, completion_trends_df = WO_completion_trends(data)
    st.header("Work Order Completion Trends")
        
    # Convert Period index to datetime for better plotting
    completion_trends_df = completion_trends_df.reset_index()
    completion_trends_df['year_month'] = completion_trends_df['year_month'].dt.to_timestamp()

    # Create more stylized charts with Plotly
    fig1 = px.line(
        completion_trends_df, 
        x='year_month', 
        y='Work Orders Completed',
        markers=True,
        title="Monthly Work Order Completion Count"
    )
    fig1.update_layout(
        xaxis_title="Month",
        yaxis_title="Number of Work Orders",
        template="plotly_white"
    )
    st.plotly_chart(fig1, use_container_width=True)

    fig2 = px.line(
        completion_trends_df, 
        x='year_month', 
        y='Average Completion Time Difference',
        markers=True,
        title="Average Days Difference Between Target and Actual Completion"
    )
    fig2.update_layout(
        xaxis_title="Month",
        yaxis_title="Days (Negative = Early, Positive = Late)",
        template="plotly_white"
    )
    fig2.add_hline(y=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig2, use_container_width=True)

    # Style the dataframe without gradient
    st.subheader("Detailed Monthly Data")
    st.dataframe(
        completion_trends_df.set_index('year_month')
        .style.format({
            'Work Orders Completed': '{:.0f}',
            'Average Completion Time Difference': '{:.1f} days'
        }),
        use_container_width=True
    )

# Streamlit app
# st.header("Grouped Work Orders by Property")
# st.dataframe(grouped_df)