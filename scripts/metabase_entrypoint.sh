#!/bin/sh

echo "seting up $1"
# get deps
apk add curl jq
# run the script and everything else
sh /tmp/metabase_entrypoint_wait.sh "echo 'Checking if Metabase is ready' && curl -s http://$1/api/health | grep -ioE 'ok'" 60 && \
if curl -s http://$1/api/session/properties | jq -r '."setup-token"' | grep -ioE "null"; then echo 'Instance already configured, exiting (or <v43)'; else \
    echo 'Setting up the instance' && \
    token=$(curl -s http://$1/api/session/properties | jq -r '."setup-token"') && \
    echo 'Setup token fetched, now configuring with:' && \
    echo "
    {
    'token':'$token',
    'user':{
        'first_name':'$MB_USER_FIRST_NAME',
        'last_name':'$MB_USER_LAST_NAME',
        'email':'$MB_USER_EMAIL',
        'site_name':'site_admin',
        'password':'$MB_USER_PASSWORD',
        'password_confirm':'$MB_USER_PASSWORD'
        },
    'database':null,
    'invite':null,
    'prefs':{
        'site_name':'site_admin',
        'site_locale':'en',
        'allow_tracking':'false'
        }
    }
    " > file.json && \
    sed 's/'\''/\"/g' file.json > file2.json && \
    cat file2.json && \
    curl -s http://$1/api/setup -H 'Content-Type: application/json' --data-binary @file2.json && \
    #
    echo 'Getting the admin token' && \
    echo "
    {
        'username':'$MB_USER_EMAIL',
        'password': '$MB_USER_PASSWORD'
    }
    " > file.json && \
    sed 's/'\''/\"/g' file.json > file2.json && \
    cat file2.json && \
    sessionToken=$(curl -s POST \
        -H "Content-Type: application/json" \
        --data-binary @file2.json \
    http://$1/api/session | jq -r '."id"') && \
    #
    dbList=$(curl -s GET \
        -H "Content-Type: application/json" \
        -H "X-Metabase-Session: ${sessionToken}" \
    http://$1/api/database | jq -r '.data[].name') && \
    sh /tmp/metabase_create_conn.sh "$1" "${dbList}" "${sessionToken}" && \
echo ' < Admin session token, exiting';fi
