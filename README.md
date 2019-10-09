# sparc-tools
Scripts and tools for the SPARC DAT-Core pipeline

## Setting up the script

### Setting up a Sparc admin account
The first thing to set up is an access to the aws-sparc AWS account as an admin. Please contact the SRE team to do so.

### Creating the virtual environment
The first time you use this repo, you will have to create a virtual environment to run the script. You can do so by typing `make venv`

## Using the script

### Assuming sparc admin role
In order to use the script locally to update the metadata in production, one needs to assume the role of the sparc admin. The cli tools `assume-role` will do just that.
Type `assume-role sparc admin`  and use your MFA code generator to answer the prompt

### Virtual Environment
 Go to the root directory of this repo and source the virtual environment you created by typing `source venv/bin/activate`

### Environment Variables
 The script relies on a certain number of environment variables:
 - ENVIRONMENT_NAME
 - DYNAMODB_ENDPOINT
 - SPARC_METADATA_DYNAMODB_TABLE_ARN
 - SPARC_METADATA_DYNAMODB_TABLE_ID
 - DRY_RUN.

DRY_RUN needs to be set to `False` in order to actually perform the update
ENVIRONMENT_NAME needs to be set to `prod`.
The values for the other variables can be found in ssm under `/prod/sparc_tools/[ENV_VAR_NAME]`
### Running the script
Type `python3 sparc_tools/main.py `
