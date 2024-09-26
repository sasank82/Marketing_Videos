import subprocess
import time
import json
import yaml
import argparse

def build_and_push_image(image_name, tag='latest'):
    """
    Build and push the Docker image to GCR.
    
    :param image_name: Name of the image (e.g., gcr.io/your-project/generate_marketing_videos).
    :param tag: Tag for the Docker image.
    """
    try:
        # Build the Docker image with the full GCR tag
        full_image_name = f'{image_name}:{tag}'
        print(f"Building Docker image: {full_image_name}")
        subprocess.run(['docker', 'build', '-t', full_image_name, '.'], check=True)

        # Push the image to GCR
        print(f"Pushing image to GCR: {full_image_name}")
        subprocess.run(['docker', 'push', full_image_name], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Error during build or push: {e.stderr}")
        raise


def load_job_yaml(job_name, region, yaml_file='job.yaml'):
    """
    Load the existing job YAML by exporting it from Cloud Run.
    
    :param job_name: Name of the Cloud Run job.
    :param region: Region where the job is deployed.
    :param yaml_file: Path to the YAML file to save.
    """
    print(f"Exporting Cloud Run job {job_name} to YAML")
    with open(yaml_file, 'w') as file:
        subprocess.run(['gcloud', 'run', 'jobs', 'describe', job_name, '--region', region, '--format', 'yaml'], check=True, shell=True, stdout=file)

def modify_job_yaml(yaml_file, task_configs, image_name):
    """
    Modify the job YAML file to update task count and container configurations.
    """
    with open(yaml_file, 'r') as file:
        job_config = yaml.safe_load(file)

    # Set the task count
    job_config['spec']['template']['spec']['taskCount'] = len(task_configs)

    # Set maxRetries under the task-level configuration
    job_config['spec']['template']['spec']['template']['spec']['maxRetries'] = 0

    # Set the task timeout to 10800 seconds (180 minutes)
    job_config['spec']['template']['spec']['template']['spec']['timeoutSeconds'] = 3600

    # Configure containers
    job_config['spec']['template']['spec']['template']['spec']['containers'] = []
    for idx, task_config in enumerate(task_configs):
        job_config['spec']['template']['spec']['template']['spec']['containers'].append({
            'name': f'generate-marketing-videos-{idx+1}',
            'image': f'{image_name}:latest',
            'env': [
                {'name': 'START_ROW', 'value': str(task_config['start_row'])},
                {'name': 'END_ROW', 'value': str(task_config['end_row'])}
            ],
            'resources': {
                'limits': {
                    'cpu': '2',
                    'memory': '8Gi'
                }
            }
        })

    with open(yaml_file, 'w') as file:
        yaml.safe_dump(job_config, file)

    print(f"Updated YAML content:\n{yaml.safe_dump(job_config)}")

def update_cloud_run_job_with_yaml(yaml_file, region):
    """
    Update the Cloud Run job using the modified YAML file.
    
    :param yaml_file: Path to the job YAML file.
    :param region: Region where the job is deployed.
    """
    print(f"Updating Cloud Run job using YAML: {yaml_file}")
    subprocess.run(['gcloud', 'run', 'jobs', 'replace', yaml_file, '--region', region], check=True, shell=True)

def execute_cloud_run_job(job_name, region='us-central1'):
    """
    Execute the Cloud Run job and return the execution name.
    
    :param job_name: Name of the Cloud Run job to execute.
    :param region: Region where the Cloud Run job is deployed.
    :return: The execution name of the job.
    """
    try:
        # Execute the Cloud Run job
        print(f"Executing Cloud Run job: {job_name}")
        result = subprocess.run([
            'gcloud', 'run', 'jobs', 'execute', job_name,
            '--region', region,
            '--format=json'  # Capture execution details in JSON
        ], check=True, capture_output=True, text=True, shell=True)

        # Parse the JSON output to get the execution name
        job_execution = json.loads(result.stdout)
        execution_name = job_execution.get('metadata', {}).get('name')  # Ensure correct parsing

        if execution_name:
            print(f"Job execution started: {execution_name}")
            return execution_name
        else:
            print("Execution name not found. Job execution might have failed.")
            return None

    except subprocess.CalledProcessError as e:
        print(f"Error executing Cloud Run job: {e}")
        raise

import subprocess
import json
import time

def monitor_job_progress(execution_name, region='us-central1'):
    """
    Monitor the Cloud Run job execution status for a specific execution.
    
    :param execution_name: Name of the Cloud Run job execution to monitor.
    :param region: Region where the Cloud Run job is deployed.
    """
    try:
        print(f"Monitoring Cloud Run job execution {execution_name} for progress...")
        while True:
            try:
                # Fetch execution details using gcloud command
                result = subprocess.run(
                    ['gcloud', 'run', 'jobs', 'executions', 'describe', execution_name,
                     '--region', region, '--format=json'],
                    check=True, capture_output=True, text=True, shell=True
                )
                execution_info = json.loads(result.stdout)

                # Extract status and time details
                completion_status = execution_info.get('status', {}).get('state', 'Unknown')
                start_time = execution_info.get('startTime', 'N/A')
                completion_time = execution_info.get('completionTime', 'N/A')

                # Log the current status
                print(f"Execution started at {start_time}. Status: {completion_status}")

                # Check if the job has completed
                if completion_status == "SUCCEEDED":
                    print(f"Job execution completed at {completion_time}.")
                    return
                elif completion_status in ["FAILED", "CANCELLED"]:
                    print(f"Job execution failed or cancelled at {completion_time}. Status: {completion_status}")
                    return

            except subprocess.CalledProcessError as e:
                # Capture error from gcloud command execution
                print(f"Error during gcloud command execution: {e}")
                print(f"Command output: {e.output}")
                print(f"Command stderr: {e.stderr}")

            except json.JSONDecodeError as e:
                print(f"Error parsing JSON output: {e}")
                print(f"Raw output: {result.stdout}")
            
            # Wait for a while before polling again
            time.sleep(10)

    except Exception as e:
        print(f"Unexpected error monitoring Cloud Run job execution: {e}")
        raise

import subprocess
import json

def auth_and_setup(project_id, region):
    """
    Authenticate and set up the environment for Google Cloud Run operations.
    
    :param project_id: The Google Cloud project ID.
    :param region: The region where the Cloud Run jobs are executed.
    """
    try:
        # Check if the user is already authenticated
        auth_check = subprocess.run(['gcloud', 'auth', 'list', '--format=json'], capture_output=True, text=True, shell=True)

        if auth_check.returncode != 0 or not auth_check.stdout.strip():
            print("Failed to retrieve authentication status. Attempting to log in.")
            subprocess.run(['gcloud', 'auth', 'login'], check=True, shell=True)
        else:
            auth_data = json.loads(auth_check.stdout)

            # If no active accounts, trigger login
            if not auth_data or not any(acct.get('status') == 'ACTIVE' for acct in auth_data):
                print("No active authentication found. Logging in...")
                subprocess.run(['gcloud', 'auth', 'login'], check=True, shell=True)

        # Check if the correct project is already set
        project_check = subprocess.run(['gcloud', 'config', 'get-value', 'project'], capture_output=True, text=True, shell=True)
        current_project = project_check.stdout.strip()

        if project_check.returncode != 0 or not current_project:
            print("Failed to retrieve project configuration.")
            raise subprocess.CalledProcessError(project_check.returncode, project_check.args)

        if current_project != project_id:
            print(f"Setting project to {project_id}...")
            subprocess.run(['gcloud', 'config', 'set', 'project', project_id], check=True, shell=True)

        # Check if the correct region is already set
        region_check = subprocess.run(['gcloud', 'config', 'get-value', 'run/region'], capture_output=True, text=True, shell=True)
        current_region = region_check.stdout.strip()

        if region_check.returncode != 0 or not current_region:
            print("Failed to retrieve region configuration.")
            raise subprocess.CalledProcessError(region_check.returncode, region_check.args)

        if current_region != region:
            print(f"Setting region to {region}...")
            subprocess.run(['gcloud', 'config', 'set', 'run/region', region], check=True, shell=True)

        # Ensure required services are enabled for Cloud Run
        required_services = [
            'cloudresourcemanager.googleapis.com',
            'run.googleapis.com',
            'iam.googleapis.com'
        ]

        for service in required_services:
            enable_service_if_needed(service, project_id)

        print("Authentication and setup completed successfully.")

    except subprocess.CalledProcessError as e:
        print(f"Error during authentication and setup: {e}")
        raise

def enable_service_if_needed(service_name, project_id):
    """
    Check if a service is enabled and enable it if necessary.
    
    :param service_name: The name of the service (e.g., 'run.googleapis.com').
    :param project_id: The Google Cloud project ID.
    """
    try:
        # Check if the service is already enabled
        check_service = subprocess.run(
            ['gcloud', 'services', 'list', '--enabled', '--format=json', '--project', project_id],
            capture_output=True, text=True, shell=True
        )

        if check_service.returncode != 0 or not check_service.stdout.strip():
            print(f"Failed to retrieve enabled services for {project_id}.")
            raise subprocess.CalledProcessError(check_service.returncode, check_service.args)

        enabled_services = json.loads(check_service.stdout)
        
        if not any(service['config']['name'] == service_name for service in enabled_services):
            print(f"Enabling service: {service_name}")
            subprocess.run(['gcloud', 'services', 'enable', service_name, '--project', project_id], check=True, shell=True)

    except subprocess.CalledProcessError as e:
        print(f"Error enabling service {service_name}: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Manage Cloud Run job lifecycle.")
    parser.add_argument('--build', action='store_true', help="Build and push the Docker image.")
    parser.add_argument('--update', action='store_true', help="Update the Cloud Run job using YAML.")
    parser.add_argument('--execute', action='store_true', help="Execute the Cloud Run job.")
    parser.add_argument('--monitor', action='store_true', help="Monitor the Cloud Run job execution.")
    parser.add_argument('--auth', action='store_true', help="Authenticate and set up the environment.")

    args = parser.parse_args()

    # Define image and job details
    project_id = 'tezzract'
    image_name = f'gcr.io/{project_id}/generate_marketing_videos'
    tag = 'latest'
    job_name = 'generate-marketing-videos'
    region = 'us-central1'
    yaml_file = 'job.yaml'

    # Define task configurations (multiple containers)
    task_configs = [
        {'start_row': 321, 'end_row': 350},
        {'start_row': 351, 'end_row': 381}      
    ]

    # Step 0: Authenticate and set up the environment
    if args.auth:
        auth_and_setup(project_id, region)

    # Step 1: Build and push the Docker image to GCR
    if args.build:
        build_and_push_image(image_name, tag)

    # Step 2: Export the current Cloud Run job to a YAML file and update it
    if args.update:
        load_job_yaml(job_name, region, yaml_file)
        modify_job_yaml(yaml_file, task_configs, image_name)
        update_cloud_run_job_with_yaml(yaml_file, region)

    # Step 3: Execute the updated Cloud Run job
    if args.execute:
        execution_name = execute_cloud_run_job(job_name, region)

    # Step 4: Monitor the job's progress
    if args.monitor and execution_name:
        monitor_job_progress(execution_name, region)

if __name__ == "__main__":
    main()
