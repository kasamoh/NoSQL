#!/bin/bash

export FLASK_APP=mflix/mflix.py
export FLASK_DEBUG=true
export MFLIX_DB_URI="mongodb://analytics:analytics-password@mflix-shard-00-00-zmtem.mongodb.net:27017,mflix-shard-00-01-zmtem.mongodb.net:27017,mflix-shard-00-02-zmtem.mongodb.net:27017/test?ssl=true&replicaSet=mflix-shard-0&authSource=admin&retryWrites=true"
