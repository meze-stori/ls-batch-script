# Steps to execute script

1. Replace the DB_CONFIG, AWS_CREDENTIALS and LAMBDA_CONFIG variables
2. Create a virtual env (venv) with 
    `
    python3 -m venv myenv
    `
3. Active venv
    `
    source myenv/bin/activate
    `
4. Install dependencies 
    `
    pip install psycopg2-binary boto3
    `
5. Connect to VPN
6. Execute script 

    `
    python luna_statements_executor.py
    `

7. When  execution is finished desactivate venv with command
    `
    deactivate
    `# batch-ls
# ls-batch-script
# ls-batch-script
