#!/bin/bash

if [ -z "${PATH}" ]; then
    export PATH=$PWD/bin
else
    export PATH=$PATH:$PWD/bin
fi

