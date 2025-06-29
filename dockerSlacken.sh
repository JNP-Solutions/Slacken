#!/bin/bash

#Docker run script for Slacken.

#This directory will be bound to /data in the container.
#SLACKEN_DATA=/data
if [ -z "$SLACKEN_DATA" ]
then
  echo Please set SLACKEN_DATA to a writable directory on fast storage.
  exit 1
fi

#For standalone mode (one process), it is helpful to provide as much memory as possible.
#This sets the default value to 16g if the variable is unassigned.
SLACKEN_MEMORY=${SLACKEN_MEMORY:-16g}
MEMORY="spark.driver.memory=$SLACKEN_MEMORY"

#By default the temporary storage (scratch) location for spark is /data/slacken_scratch (inside the container).
#To override that location, change this variable. This directory should be a fast drive (ideally physical SSD).
SLACKEN_TMP=/data/slacken_scratch

#In the command below, additional -v flags may be added to bind more volumes, e.g.
# -v /data/on/host:/data2:rw
# to expose the /data/on/host directory as /data2 inside Slacken

#We run docker as root:current user to make newly created files writable by the user's group

exec docker run -e SLACKEN_MEMORY=$SLACKEN_MEMORY -p 4040:4040 \
  -u $(id -u root):$(id -g ${USER}) \
  -e SLACKEN_TMP=$SLACKEN_TMP \
  -v $SLACKEN_DATA:/data:rw \
  jnpsolutions/slacken:2.0.0 $*
