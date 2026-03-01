# pg_settings_to_json.py
import psycopg2
import json
import os
import re
import copy
# fetch_pg_param_description.py
import pandas as pd
import requests
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin

skipped_knobs = [
    # >> cannot be changed
    'transaction_read_only', 
    'transaction_isolation', 
    'transaction_deferrable', 
    # cannot be changed <<
    # >> Formatting / Output related
    'bonjour', 
    'bonjour_name',
    'IntervalStyle',
    'xmlbinary',
    'xmloption',
    'bytea_output',
    'extra_float_digits',
    # Formatting / Output related <<
    # >> Logging & Debugging related
    'debug_pretty_print',
    'debug_print_parse',
    'debug_print_plan',
    'debug_print_rewritten',
    'client_min_messages',
    'syslog_facility',
    'syslog_sequence_numbers',
    'syslog_split_messages',
    # Logging & Debugging related <<
    # >> Security & Authentication related
    'password_encryption',
    'db_user_namespace',
    'krb_caseins_users',
    'row_security',
    'session_replication_role',
    'ssl', 
    'ssl_min_protocol_version', 
    'ssl_max_protocol_version', 
    'ssl_prefer_server_ciphers', 
    'ssl_passphrase_command_supports_reload',
    # Security & Authentication related <<
    # >> Process / Display
    'update_process_title',
    'unix_socket_permissions',
    'port',
    'superuser_reserved_connections',
    # Process / Display <<
    # >> Monitoring / Stats
    'track_activities', 
    'track_activity_query_size', 
    'track_commit_timestamp', 
    'track_counts', 
    'track_functions', 
    'track_io_timing',
    # Monitoring / Stats <<
    # >> Miscellaneous “static” knobs
    'check_function_bodies',
    'default_transaction_isolation', 
    'default_transaction_deferrable', 
    'default_transaction_read_only',
    'plan_cache_mode',
    'force_parallel_mode',
    'dynamic_shared_memory_type', 
    'shared_memory_type',
    'huge_pages',
    'old_snapshot_threshold',
    # Miscellaneous “static” knobs <<

    'geqo',
]

focuesed_categories = [
    'Resource Usage / Memory',
    'Resource Usage / Disk', 
    'Resource Usage / Kernel Resource Usage',
    'Resource Usage / Cost-Based Vacuum Delay',
    'Resource Usage / Background Writer',
    'Resource Usage / Asynchronous Behavior',
    'Write-Ahead Log / Settings',
    'Write-Ahead Log / Checkpoints',
    'Write-Ahead Log / Archiving',
    'Replication / Sending Servers',
    'Replication / Master Server',
    'Replication / Standby Servers',
    'Query Tuning / Planner Cost Constants',
    'Query Tuning / Genetic Query Optimizer',
    'Query Tuning / Other Planner Options',
    'Autovacuum',
    'Connections and Authentication / Connection Settings',
]

def export_pg_settings_to_json(host, dbname, user, password, port=5432, output_file='pg_settings.json'):
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            name, 
            vartype as type,
            category,
            short_desc AS short,
            boot_val AS default,
            unit, 
            context, 
            min_val as min,
            max_val as max,
            enumvals as values
        FROM pg_settings
    """)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    settings = []

    for row in rows:
        item = dict(zip(columns, row))
        if item['type'] == 'string':
            continue

        if item['name'].startswith('log'): # skip log related parameters
            continue

        if item['category'] in ['Preset Options', 'Developer Options', 'Error Handling']:
            continue

        if 'Version and Platform Compatibility' in item['category']:
            continue

        if 'Error Reporting and Logging' in item['category']:
            continue
        
        if item['name'] in skipped_knobs:
            continue

        # if item['category'] not in focuesed_categories or item['name'] in skipped_knobs:
        #     continue
        settings.append(item)

    data = {
        'version': '13',
        'params': settings
    }
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Exported {len(settings)} parameters to {output_file}")
    cursor.close()
    conn.close()

def export_pg_settings_to_json(host, dbname, user, password, port=5432, output_file='pg_settings.json'):
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            *
        FROM pg_settings
    """)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    settings = []

    data = {}
    for row in rows:
        item = dict(zip(columns, row))
        # if item['vartype'] == 'string':
        #     continue

        # if item['name'].startswith('log'): # skip log related parameters
        #     continue

        # if item['category'] in ['Preset Options', 'Developer Options', 'Error Handling']:
        #     continue

        # if 'Version and Platform Compatibility' in item['category']:
        #     continue

        # if 'Error Reporting and Logging' in item['category']:
        #     continue
        
        # if item['name'] in skipped_knobs:
        #     continue

        # if item['category'] not in focuesed_categories or item['name'] in skipped_knobs:
        #     continue
        settings.append(item)
        data[item['name']] = item

    # data = {
    #     'version': '13',
    #     'params': settings
    # }
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Exported {len(settings)} parameters to {output_file}")
    cursor.close()
    conn.close()

def fetch_parameter_description(param_name):
    url = 'https://www.postgresql.org/docs/current/runtime-config.html'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    anchor = soup.find('a', attrs={'name': param_name})
    if not anchor:
        print(f"Parameter '{param_name}' not found.")
        return

    parent_section = anchor.find_parent('dt')
    description = ""
    if parent_section:
        dd = parent_section.find_next_sibling('dd')
        if dd:
            description = dd.get_text(separator='\n').strip()

    print(f"\nFull Description for '{param_name}':\n")
    print(description)

visited_urls = set() # record the urls in extracting order
visited_urls_lock = threading.Lock()

def extract_text_from_url(url, doc_name, output_file='./official_document_pg13_all.json'):
    if url in visited_urls or '#' in url:
        return  # Skip if the URL has already been visited or it is third-level
    visited_urls.add(url)

    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract text from the current page
        doc_content = copy.deepcopy(soup.find(id=re.compile("docContent", re.I)))

        content = doc_content.find('div', class_=re.compile("sect1", re.I))

        if content is not None:
            subcontents = content.find_all('div', class_=re.compile("sect2", re.I))
            variables_contents = []
            if len(subcontents) == 0: # no subsection for parameters
                variables_contents = [child for child in content.children if child.name == 'div' and 'variablelist' in child.get("class")]
            else:
                for subcontent in subcontents:
                    temp_contents = [child for child in subcontent.children if child.name == 'div' and 'variablelist' in child.get("class")]
                    variables_contents.extend(temp_contents)

            official_document = json.load(open(output_file, 'r'))
            knob_info = official_document['params']

            for variables_content in variables_contents:
                variable_description = _extract_parameter_decription(variables_content)
                for var, desp in variable_description.items():   
                    for knob in knob_info:
                        if var == knob['name']:
                            knob.update({'description':desp})
                    
            json.dump(official_document, open(output_file, 'w'), indent=2)

        # Find table of contents in doc
        TOC = soup.find('div', class_=re.compile("TOC", re.I))
        if TOC is None:
            # print("No Table of Contents in ", url)
            return
        
        # Extract and follow links on the current page
        links = TOC.find_all('a', href=True)
        if links is None:
            print("No more links in ", url)
            return
        
        for link in links:
            next_url = urljoin(url, link['href'])
            print("next_url", next_url)
            extract_text_from_url(next_url, doc_name, output_file)
    else:
        print(f"Failed to retrieve {url}. Status code: {response.status_code}")

def _extract_parameter_decription(variables_content):
    var_desp = {}
    variable_names = []
    for dt in variables_content.find_all('dt'):
        varname_content = dt.find('code', class_=re.compile("varname", re.I))
        if varname_content is not None:
            name = varname_content.get_text()
            variable_names.append(name)
    # varibale_descriptions_content = variables_content.find_all('dd')
    varibale_descriptions_content = []
    dl = variables_content.find('dl', class_=re.compile("variablelist", re.I))
    dd_elements = [child for child in dl.children if child.name == 'dd']
    for dd in dd_elements:
        ps = dd.find_all('p')
        ps = [ str(p) for p in ps]
        varibale_descriptions_content.append('\n'.join(ps))

    if len(variable_names) != len(varibale_descriptions_content):
        # print("=============mismatch:", url)
        print(len(variable_names), len(varibale_descriptions_content))
        exit()

    for i in range(len(variable_names)):
        variable_name = variable_names[i]
        description = varibale_descriptions_content[i]
        var_desp[variable_name] = description
    return var_desp


if __name__ == '__main__':
    # # Example usage
    # fetch_parameter_description('archive_timeout')

    # Example usage
    # Replace these with your actual PostgreSQL credentials
    output_file = './system_view_pg13.json'
    export_pg_settings_to_json(
        host='localhost',
        dbname='workload',
        user='postgres',
        password='postgres',
        output_file=output_file
    )

    # url = "https://www.postgresql.org/docs/13/runtime-config.html"
    # extract_text_from_url(url, 'test', output_file=output_file)