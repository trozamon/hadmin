#!/bin/bash

# Try and get HAdmin's root directory... so dang hard
HADMIN_DIR=`dirname $0 2> /dev/null`
if [ ! $? -eq 0 ]
then
	HADMIN_DIR=`dirname ${BASH_ARGV[0]} 2> /dev/null`
fi

HADMIN_DIR="`pwd`/${HADMIN_DIR}"

export PYTHONPATH="${HADMIN_DIR}:${PYTHONPATH}"
export PATH="${HADMIN_DIR}/bin:${PATH}"
