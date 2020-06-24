# sparc-tools
Method to synchronize metadata from TTL File into Blackfynn Platform. This repository contains a command line application that can get the latest SPARC TTL file and update the metadata on the Blackfynn platform to match that in the TTL file. 

The tool contains two commands:
`ttl_updata ttl-to-json`
`ttl_update update`

for more information about these commands, look at the help for each of the commands (e.g. `ttl_update get-ttl --help`)

These reflect the three stages in which the update is carried out. Step 1 is to get the latest TTL file. Step 2 is to align the contents of the TTL file to the schema that is implemented in the Blackfynn platform using a standardized JSON schema. Step 3 is to parse the JSON schema and update the platform. A hash of each component (model) for each dataset is created based on the JSON representation and stored on the platform such that the script can skip updating specific records if the JSON representation of those records are unchanged between TTL updates.

## Setting up Locally

### Setup Environment Variables
The script relies on a certain number of environment variables:
 - BLACKFYNN_API_TOKEN
 - BLACKFYNN_API_SECRET
 - BLACKFYNN_API_HOST

### Install executable
To install the `ttl_update` executable in a virtual environment, run:
`make install`

This will create a virtual environment, install the required dependencies and create the `ttl_upate` executable. After installation, you can activate the virtual environment and run the scripts.

## Running against different environments:
You can run the scripts against production or development environments. When running against development, the script will create a number of datasets that match the SPARC datasets on the production environment. The names of the datasets on the development environment will match the dataset IDs on the production environment.

## Synchronizing data
The script utilizes a special dataset on the platform to store synchronization information. This information is based on the JSON file that is being synchronized. For each set of records per model in each dataset, we compute a hash based on the sub-section in the JSON file. If any chnages were made in any of the records for a particular model, we remove all records and re-import all records for that model. 