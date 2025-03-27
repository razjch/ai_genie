from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import os
from crypto import decrypt_text

load_dotenv() ## load all the environemnt variables

server = os.getenv("MESOS_SERVER")
username = os.getenv("MESOS_USERNAME")
password = decrypt_text(os.getenv("MESOS_PASSWORD"))


def chronos_api(action, job_name):
    try:
        if action.lower() == "start":
            status = start_mesos_container(job_name)
            return pd.DataFrame({'job_name': [job_name], 'status': [status]})
        elif action.lower() == "stop":
            status = 'Kill job not implemented'
            return pd.DataFrame({'job_name': [job_name], 'status': [status]})
        else:
            return pd.DataFrame({'job_name': [job_name], 'status': ['Invalid action']})
    except Exception as err:
        return pd.DataFrame({'job_name': [job_name], 'status': [err]})


def start_mesos_container(app):
    try:
        url = f"{server}:4400/scheduler/job/{app}"
        method = "PUT"
        print(url)
        response = requests.request(method, url, auth=HTTPBasicAuth(username, password), timeout=10)
        if response.status_code not in [200, 204]:
            return f"Failed to start job: {app}, Error: {response.text}, Status Code: {response.status_code}"
        return "Started successfully."
    
    except requests.RequestException as err:
        return f"HTTP request failed., err: {str(err)}"
    
    
if __name__ == '__main__':
    print(chronos_api("start", "micro-gws-common-list_jobs"))