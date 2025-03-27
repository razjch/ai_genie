from dotenv import load_dotenv
import streamlit as st
import os
from openai import AzureOpenAI
import psycopg2
import streamlit as st
import pandas as pd
from chronos import chronos_api
from static_query import *
from crypto import decrypt_text
import pandas as pd
from workorder import *

load_dotenv() ## load all the environemnt variables

endpoint = os.getenv("ENDPOINT_URL")   
deployment = os.getenv("DEPLOYMENT_NAME")   
subscription_key = os.getenv("AZURE_OPENAI_API_KEY")

func_list = ['mom_completion_rate', 'compare_WOs', 'open_closed_count', 'open_elapsed_WOs', 'past_WO_counts', 'WO_completion_trends']

## Define Your Prompt
PROMPT = """
    -if the user input is not related to jobs/clients/service_line/source_system do not prepare sql queries.
        **Rules**
        - You are gpt bot.
        - Use your knowledge to answer the question.
        - Use search engines to find the answer.
        - Do not use SQL queries to answer the question.
        - If you are not sure about the answer, suggest some resources where to find it".
        
        **Example 1**:
        **User Input**:     who is the prime minister of India
        **AI Response**:    Narendra Modi.
        
        **Example 2**:
        **User Input**:     whats the area of nepal
        **AI Response**:    1,47,181 km².
        
    - if the user input related to work orders, then return the following functions.
        - functions: 
            -  WO_completion_trends     - mom_completion_rate   - compare_WOs
            - open_closed_count         - open_elapsed_WOs      - past_WO_counts

        **Example 1**:
            **User Input**:     show me the month-over-month or mom completion rate
            **AI Response**:    mom_completion_rate
        
        **Example 2**:
            **User Input**:     show me the open and closed work orders or show me the count of open and closed work orders or show me the open vs closed work orders
            **AI Response**:    open_closed_count
        
        **Example 3**:
            **User Input**:     show me count of work orders for each time period or show me the work order counts in past time periods or show me the work order counts year wise
            **AI Response**:    past_WO_counts
        

    -if the user input is related to jobs/clients/service_line/source_system return the SQL query.
        **Rules**
        - You are an expert in writing PostgreSQL query.
        - write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks.
        - Do not append 'sql' word in the beginning of the query or in the end of the query.
        - You must use the given database names, schema names, table names and column names while writing the SQL query.
        - You should use the table gws_job_error_log_info for failed jobs and for long running jobs.
        - You should use the table microservices_status_info for job status information.
        - you should use the table gws_clients_info for client/service_line/source_system information.
        

    **Two Databases**
        1. GWS_CUSTOM_INGESTION 
        - Schema Name: common
        - Table Name: gws_job_error_log_info
        - Columns:
            - edp_update_ts          - status                - table_name
            - err                    - failed_ts_or_last_updated_ts
            - connection_name        - config_file           - error_categorization
            - query                  - possible_fix          - start_ts
            - last_updated_duration  - last_updated_time     - running_from
            - job_name               - rowcount

        2. ENTERPRISE_DATA_PLATFORM
        - Schema Name: common
        - Table Name: microservices_status_info
        - Columns:
            - start_time            - end_time              - config
            - status               - errorlog              - task_info
            - service_name         - read_count            - write_count
            - error_count          - lob_name             - record_ts
            - write_count_output   - error_count_output   - job_name
            
        - Table Name: gws_clients_info
        - Columns:
            - client_id             - client_name           - edp_client_id
            - edp_client_name       - service_line
            - src_system_name       - is_active             - client_db
            - server_config         - src_system_config     - client_dm_f
            
    
    **Examples for writing query from user inputs related to jobs/clients/service_line/source_system**:
    
    **Example 1**:
        **User Input**:     List out all failed jobs for today
        **AI Response**:    select distinct on (job_name) job_name, err,failed_ts_or_last_updated_ts as failed_ts from common.gws_job_error_log_info
                where status = 'Failed' and edp_update_ts > current_date order by job_name, edp_update_ts desc;

    **Example 2**:
        **User Input**:     List of long running jobs
        **AI Response**:    select job_name,max(split_part(running_from,' ',1)::numeric) as running_from_hrs from common.gws_job_error_log_info 
                where status = 'Long Running'
                and edp_update_ts > current_date 
                group by job_name
                order by job_name, max(split_part(running_from,' ',1)::numeric) desc;
    
    **Example 3**:
        **User Input**:     give me the list of jobs which are running from more than 10 hours 
        **AI Response**:    select job_name,max(split_part(running_from,' ',1)::numeric) as running_from_hrs from common.gws_job_error_log_info 
                where status = 'Long Running'
                and edp_update_ts > current_date 
                and (split_part(running_from,' ',1)::numeric) > 10
                group by job_name
                order by max(split_part(running_from,' ',1)::numeric) desc;
                
    **Example 4**:
        **User Input**:  Give me the current status of the job micro-gws-pjm-kahua-U_DM_Fact_Finance_Client
        **AI Response**:    select job_name, status, start_time, end_time from common.microservices_status_info
                where job_name = 'micro-gws-pjm-kahua-U_DM_Fact_Finance_Client' and start_time > current_date order by start_time desc limit 1;
    
    **Example 5**:
        **User Input**: Jobs failed due to foreign key issues today
        **AI Response**: select distinct on (job_name) job_name, err, edp_update_ts from common.gws_job_error_log_info
                 where status = 'Failed' and err ilike '%foreign key%' and edp_update_ts > current_date order by job_name, err, edp_update_ts desc;

    **Example 5a**:
        **User Input**: which client opted for most of the source system, also give me the list of source system
        **AI Response**: select client_name, count(distinct src_system_name) as source_system_count, array_agg(distinct src_system_name) as source_systems from common.gws_clients_info 
                        where is_active='Y' group by client_name order by source_system_count desc limit 1;

    
    **Example 6**:
        **User Input**: whats the average completion time of job micro-gws-data-monitoring-framework-dm_validation_output
        **AI Response**: select job_name,avg(completion_time) as avg_completion_time from (
                select job_name,start_time::date as start_date,max(end_time) ,min(start_time), (max(end_time) - min(start_time)) as completion_time
                from common.microservices_status_info msi 
                where job_name in ('micro-gws-data-monitoring-framework-dm_validation_output')
                and start_time > current_date-7
                and config ilike '/opt/app-configs%'
                and status = 'Success'
                group by job_name,start_time::date
                ) as t
                group by job_name;
                
    **Example 7**:
        **User Input**: how may jobs failed yestreday
        **AI Response**: select count(distinct job_name) from common.gws_job_error_log_info where status = 'Failed' and edp_update_ts > current_date - 1 and edp_update_ts < current_date;
    
    **Example 7a**:
        **User Input**: what is the status of sequentra job
        **AI Response**: select job_name, status, start_time, end_time, end_time-start_time as time_taken from common.microservices_status_info where job_name ilike 'micro-%sequentra%' and start_time > current_date and config ilike '/opt/app-configs%' order by end_time desc;
        
    **Example 7b**:
        **User Input**: how many clients are there in the system
        **AI Response**: select count(distinct client_name) from common.gws_clients_info where is_active = 'Y';
    
    **Example 7c**:
        **User Input**: how many source systems are enabled for facebook client
        **AI Response**: select client_name,edp_client_id,src_system_name from common.gws_clients_info where client_name ilike '%facebook%' and is_active = 'Y';
    
    **Example 7d**:
        **User Input**: how many clients are enabled for si7
        **AI Response**: select client_name,edp_client_id,src_system_name from common.gws_clients_info where src_system_name ilike 'si7' and is_active = 'Y';
    
    **Example 8**:
        **User Input**:     stop or terminate or kill or hault or start this job 
        **AI Response**:    select 'stop' as action, Null as job_name;
            
    **Example 9**:
        - Rule : - if job name is not passed by the user then it should be null
                - if job name not start with micro- then return Invalid job name
        **User Input**:     stop or terminate or kill or hault this job micro-gws-pjm-kahua-U_DM_Fact_Finance_Client
        **AI Response**:    select 'stop' as action, 'micro-gws-pjm-kahua-U_DM_Fact_Finance_Client' as job_name;
        
    **Example 10**:
        - Rule :  - if job name is not passed by the user then it should be null
                  - if job name not start with micro- then return Invalid job name
        **User Input**:     start or run or trigger this job micro-gws-pjm-kahua-U_DM_Fact_Finance_Client
        **AI Response**:    select 'start' as action, 'micro-gws-pjm-kahua-U_DM_Fact_Finance_Client' as job_name;
    
    **Example 11**:
        - Rule : Return the query exactly same as below only if the user input is show the AWS job status report.
             Return complete query as it is without any changes. 
        **User Input**:    show the AWS job status report
        **AI Response**:   select 'show' as action, 'aws_job_status_report' as job_name;
        
    **Example 12**:
        - Rule : Return the query exactly same as below only if the user input is show the EDP health status report or edp health report.
             Return complete query as it is without any changes. 
        **User Input**:     show the AWS job status report
        **AI Response**:   select 'show' as action, 'edp_health_report' as job_name;
        
    **Example 13**:
        - Rule : Return the query exactly same as below only if the user input is show the AWS job status report with clients.
             Return complete query as it is without any changes. 
        **User Input**:     show the AWS job status report
        **AI Response**:   select 'show' as action, 'aws_job_status_report_with_clients' as job_name;
        
    **Example 14**:
        - Rule : if question related to refreshed or data loaded today then must include single_tenant_copy in the query
        **User Input**: Is si7 or sequentra or kahua or jde or any source system refreshed today
        **AI Response**: select job_name, min(start_time) as start_time, max(end_time) as end_time from common.microservices_status_info where job_name ilike '%-si7%' and job_name ilike '%single_tenant_copy' and start_time > current_date group by job_name order by end_time desc;
    
    **Example 15**:
        **User Input**: yesterday's job success rate or success vs failed rate of jobs for today
        **AI Response**: select status,count(1) from common.microservices_status_info where start_time > current_date - 1 and start_time < current_date group by status;
"""

# Database connection
def get_db_connection(db: str):
    if db == 'EDP':
        conn = psycopg2.connect(
            dbname=os.getenv("EDP_DB_NAME"),
            user=os.getenv("EDP_DB_USER"),
            password=decrypt_text(os.getenv("EDP_DB_PASSWORD")),
            host=os.getenv("EDP_DB_HOST"),
            port=os.getenv("EDP_DB_PORT")
        )
    elif db == 'BI':
        conn = psycopg2.connect(
            dbname=os.getenv("BI_DB_NAME"),
            user=os.getenv("BI_DB_USER"),
            password=decrypt_text(os.getenv("BI_DB_PASSWORD")),
            host=os.getenv("BI_DB_HOST"),
            port=os.getenv("BI_DB_PORT")
        )
    else:
        conn = psycopg2.connect(
            dbname=os.getenv("GWS_DB_NAME"),
            user=os.getenv("GWS_DB_USER"),
            password=decrypt_text(os.getenv("GWS_DB_PASSWORD")),
            host=os.getenv("GWS_DB_HOST"),
            port=os.getenv("GWS_DB_PORT")
        )
    return conn

# Function to fetch data from PostgreSQL
def fetch_data_from_db(query,cursor):
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(list(result))
    if len(df) != 0:
        df.columns = [desc[0] for desc in cursor.description]
        df.columns = [col.upper() for col in df.columns]
    else:
        df = pd.DataFrame()
    return df


# Initialize Azure OpenAI Service client with key-based authentication     
client = AzureOpenAI(   
    azure_endpoint=endpoint,   
    api_key=subscription_key,   
    api_version="2024-05-01-preview"
) 

# Generate the SQL query from natural language
def generate_sql_query(client, deployment, prompt=None, user_input=None):
    # Use provided prompt or default
    if prompt is None:
        prompt = "Convert this question to SQL:"
    
    # Use provided user input or get it from console
    if user_input is None:
        user_input = input("Enter your question: ")
    
    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": f"{prompt}"},
            {"role": "user", "content": f"{user_input}"}
        ]
    )
    return response


@st.cache_resource()
def get_connection():
    """
    Establishes and returns database connections to EDP and GWS databases.
    
    Returns:
        tuple: Connection objects for EDP and GWS databases
    """
    print("Connecting to databases...")
    edp_conn = get_db_connection("EDP")
    gws_conn = get_db_connection("GWS")
    #bi_conn = get_db_connection("BI")
    print("Connected to databases.")
    return (edp_conn, gws_conn)

# Streamlit app
def main():
    """
    Main function to run the streamlit app
    """
    
    st.set_page_config(page_title="AI Genie", layout="wide")
    
    # Custom CSS for background and styling
    st.markdown("""
        <style>
        .stApp {
            background-color: #f0fcf1;  /* background */
        }
        h1 {
            color: #1e3d59;
            text-align: center;
            padding: 20px;
            background-color: #012A2D;  /* Green header background */
            border-radius: 10px;
            margin-bottom: 20px;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<h1 style='color: white; text-align: center; padding: 20px; background-color: #012A2D; border-radius: 10px; margin-bottom: 20px;'>AI Genie</h1>", unsafe_allow_html=True)
    
    # Initialize database connections
    EDP_conn, GWS_conn = get_connection()
    EDP_cursor = EDP_conn.cursor()
    GWS_cursor = GWS_conn.cursor()
    #BI_cursor = BI_conn.cursor()
    
    wo_data = prepare_data()

    
    # Style the text input with a custom look
    st.markdown("""
        <style>
        .stTextInput > div > div > input {
            background-color: #f8f9fa;
            color: #333;
            border: 2px solid #012A2D;
            border-radius: 8px;
            padding: 10px;
            font-size: 16px;
        }
        .stTextInput > div > div > input:focus {
            border-color: #38b000;
            box-shadow: 0 0 5px rgba(56, 176, 0, 0.5);
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Create the text input with a placeholder
    user_input = st.text_input("Enter your question:", 
                               placeholder="Ask me about jobs, clients, or service lines...",
                               key="query_input")
    
    # Handle Enter key press
    st.session_state.enter_pressed = False
    if user_input and user_input != st.session_state.get('previous_input', ''):
        st.session_state.enter_pressed = True
        st.session_state.previous_input = user_input
    user_input = user_input.replace('kill','stop')
    # Create a styled button with custom CSS
    st.markdown("""
        <style>
        .stButton button {
            background-color: #012A2D;
            color: white;
            font-weight: bold;
            border-radius: 8px;
            padding: 10px 20px;
            transition: all 0.3s ease;
        }
        .stButton button:hover {
            background-color: #38b000;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }
        .error-message {
            color: #d62828;
            font-weight: bold;
            padding: 10px;
            border-radius: 5px;
            background-color: #f8d7da;
            margin: 10px 0;
        }
        .success-message {
            color: #38b000;
            font-weight: bold;
            padding: 10px;
            border-radius: 5px;
            background-color: #d4edda;
            margin: 10px 0;
        }
        .table-container {
            border-radius: 8px;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            padding: 10px;
            margin: 15px 0;
            background-color: white;
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th {
            background-color: #012A2D;
            color: white;
            padding: 12px;
            text-align: centre;
        }
        td {
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        tr:hover {
            background-color: #e0f7fa;
        }
        </style>
    """, unsafe_allow_html=True)
    
    if st.button("Ask your question", key="ask_button") or (user_input and st.session_state.get('enter_pressed', True)):
        with st.spinner("Fetching..."):
            response = generate_sql_query(client, deployment, PROMPT, user_input)
            response = response.choices[0].message.content.strip()
            print("*"*100)
            print(response)
            print("*"*100)
            cursor = GWS_cursor
            
            if response:
                if response in func_list:
                    # call the function
                    eval(response)(wo_data)
                    return
                    
                if not response.startswith('select'):
                    st.markdown(f"<div class='success-message'>{response}</div>", unsafe_allow_html=True)
                    return
                
                if "common.microservices_status_info" in response or "common.gws_clients_info" in response:
                    print("switch to EDP")
                    cursor = EDP_cursor
                
                if "all_accounts_data.fm_fact_workorder" in response:
                    print("switch to BI")
                    #cursor = BI_cursor
                
                print("Running Query...")
                try:
                    df = fetch_data_from_db(response, cursor)
                    print(df)
                    
                    if df.empty:
                        st.markdown("<div class='error-message'>No data found.</div>", unsafe_allow_html=True)
                        return
                    
                    # Display the data in a styled table
                    st.markdown("<div class='table-container'>" + 
                                df.to_html(escape=False, index=False, classes='styled-table') + 
                                "</div>", unsafe_allow_html=True)
                    
                    if 'ACTION' in df.columns and 'JOB_NAME' in df.columns and df['JOB_NAME'].values[0] is not None:
                        action = df['ACTION'].values[0]
                        job_name = df['JOB_NAME'].values[0]
                        
                        if job_name.startswith('micro-'):
                            #st.markdown("<hr style='margin: 20px 0;'>", unsafe_allow_html=True)
                            #col1, col2 = st.columns([1, 16])
                            # with col1:
                            #     st.markdown("<span style='color: #d62828; font-weight: bold; font-size: 18px;'>Are you sure?</span>", unsafe_allow_html=True)
                            # with col2:
                            #     if st.button("Yes", key='yes_button'):
                            #         confirm_button = True
                            #         print("clicked", confirm_button)
                            
                            # Create a confirmation button
                            if True:
                                with st.spinner(f"{action.capitalize()}ing job {job_name}..."):
                                    response = chronos_api(action, job_name)
                                    st.markdown("<p style='font-weight: bold; color: #012A2D; margin-top: 15px;'>Chrono API Response:</p>", unsafe_allow_html=True)
                                    st.markdown("<div class='table-container'>" + 
                                                pd.DataFrame(response).to_html(index=False, classes='styled-table') + 
                                                "</div>", unsafe_allow_html=True)
                        
                        if job_name.startswith('aws_job_status_report') or job_name.startswith('edp_health_report'):
                            with st.spinner("Generating report..."):
                                df = fetch_data_from_db(static_queries[job_name], EDP_cursor)
                                st.markdown("<div class='table-container'>" + 
                                            df.to_html(index=False, classes='styled-table') + 
                                            "</div>", unsafe_allow_html=True)
                except Exception as e:
                    st.markdown(f"<div class='error-message'>Error executing query: {str(e)}</div>", unsafe_allow_html=True)
            else:
                st.markdown("<div class='error-message'>No valid query generated.</div>", unsafe_allow_html=True)

if __name__ == '__main__':
    main()
    
