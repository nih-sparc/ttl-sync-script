# sparc-tools
Method to synchronize metadata from TTL File into Blackfynn Platform. This repository contains a command line application that can get the latest SPARC TTL file and update the metadata on the Blackfynn platform to match that in the TTL file. 

The tool contains three commands:
`ttl_updata get-ttl`
`ttl_update to-json`
`ttl_update update`

for more information about these commands, look at the help for each of the commands (e.g. `ttl_update get-ttl --help`)

These reflect the three stages in which the update is carried out. Step 1 is to get the latest TTL file. Step 2 is to align the contents of the TTL file to the schema that is implemented in the Blackfynn platform using a standardized JSON schema. Step 3 is to parse the JSON schema and update the platform. A hash of each component (model) for each dataset is created based on the JSON representation and stored on the platform such that the script can skip updating specific records if the JSON representation of those records are unchanged between TTL updates.

## Setting up Locally

### Setup virtualenv
Setup your local python virtual environment

`python3 -m venv venv`
`source ./venv/bin/activate`

### Setup Environment Variables
The script relies on a certain number of environment variables:
 - BLACKFYNN_API_TOKEN
 - BLACKFYNN_API_SECRET
 - BLACKFYNN_API_HOST

### install locally
There is a CLI that can be used to interact with the functionality using (Click)[https://click.palletsprojects.com/en/7.x/] and is bundled with (Setuptools)[https://click.palletsprojects.com/en/7.x/setuptools/#setuptools-integration]. To install the package, run:

`pip install --editable .`
