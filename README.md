# sparc-tools
Scripts and tools for the SPARC DAT-Core pipeline

## Setting up Locally

### Setup virtualenv
Setup your local python virtual environment

`python3 -m venv venv`
`source ./venv/bin/activate`

### Install and run Docker
You need to have Docker installed on you computer as localstack runs witin a docker container

### Setup localstack
With your virtual environment activated install localstack.  The script is written to leverage DynamoDB to store a mapping between the TTL ids and the Blackfynn Ids for providing the ability to synchronize the metadata. Currently it assumes the user mocks DynamoDB using localstack. 

`pip3 install localstack/localstack`

Run localstack and make super proper ports are open:

`docker run -p 4569:4569 localstack/localstack`

### Setup Environment Variables
The script relies on a certain number of environment variables:
 - BLACKFYNN_API_TOKEN
 - BLACKFYNN_API_SECRET
 - BLACKFYNN_API_HOST

### install locally
There is a CLI that can be used to interact with the functionality using (Click)[https://click.palletsprojects.com/en/7.x/] and is bundled with (Setuptools)[https://click.palletsprojects.com/en/7.x/setuptools/#setuptools-integration]. To install the package, run:

`pip install --editable .`


## Running against Development Env


## Running against Production






## Setting up the script

### Setting up a Sparc admin account
The first thing to set up is an access to the aws-sparc AWS account as an admin. Please contact the SRE team to do so.

### Creating the virtual environment
The first time you use this repo, you will have to create a virtual environment to run the script. You can do so by typing `make venv`







## Using the script

### Assuming sparc admin role
In order to use the script locally to update the metadata in production, one needs to assume the role of the sparc admin. The cli tools `assume-role` will do just that.
Type `assume-role sparc admin`  and use your MFA code generator to answer the prompt
