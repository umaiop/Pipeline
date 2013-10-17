#!/bin/bash

# if node_modules missing then install
if [ ! -d "node_modules" ]; then
	npm install
fi

# pass args to node
node export_to_csv.js "$@"
