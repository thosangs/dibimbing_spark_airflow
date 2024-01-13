#!/bin/bash

command="${1}"

until eval "${command}"
do
    sleep 1
done