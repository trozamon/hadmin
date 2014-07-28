#!/bin/bash

HADMIN_DIR=`pwd`/`dirname ${0}`

export PYTHONPATH="${HADMIN_DIR}:${PYTHONPATH}"
export PATH="${HADMIN_DIR}/bin:${PATH}"
