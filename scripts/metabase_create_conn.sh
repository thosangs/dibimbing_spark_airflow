#!/bin/bash
echo "$2"

if ! echo "$2" | grep -q "Postgres DW"; then
    echo 'Setting up data warehouse source'
    echo "
    {
        'engine': 'postgres',
        'name': 'Postgres DW',
        'details': {
            'host':'$DW_POSTGRES_CONTAINER_NAME',
            'port':'$DW_POSTGRES_PORT',
            'user':'$DW_POSTGRES_USER',
            'password':'$DW_POSTGRES_PASSWORD',
            'db':'$DW_POSTGRES_DB'
        }
    }
    " > file.json
    sed 's/'\''/\"/g' file.json > file2.json
    cat file2.json
    curl -s -X POST \
    -H "Content-type: application/json" \
    -H "X-Metabase-Session: $3" \
    http://$1/api/database \
    --data-binary @file2.json
else
    echo "'Postgres DW' found in the database list, exiting..."
fi