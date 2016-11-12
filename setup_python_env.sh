#!/bin/bash
#
# Copyright 2011 TiVo Inc. All Rights Reserved.
#
# A script to set up a Python environment that can run tvset.
#
# PREREQUISITES:
#   - virtualenv (see http://pypi.python.org/pypi/virtualenv)
#
# If the VIRTUALENV environment variable is set, it will be used for the
# virtualenv command invocation.  Otherwise, the default is 'virtualenv'.
#
# If the ENVDIR environment variable is set, it will be used as the directory
# in which the virtual environment will be created.  Otherwise, the default is
# 'python_env'.  The default is recommended.

set -e

if [ -z "$VIRTUALENV" ]; then
    VIRTUALENV=virtualenv
fi

if [ -z "$ENVDIR" ]; then
    ENVDIR=python_env
fi

if [ $# -ne 0 ]; then
    echo "usage: setup_python_env.sh"
    exit 0
fi

# Xcode4 doesn't include the ppc assembler,
# but some packages still assume you want ppc versions.
# I tried using the 'arch' command to get the preferred achitecture, 
# and just building that, but on my Macbook Pro, arch returns 'i386',
# but it runs python in x86_64.
if [ `uname` == "Darwin" ]
then
    export ARCHFLAGS="-arch i386 -arch x86_64"
fi

$VIRTUALENV $ENVDIR
. $ENVDIR/bin/activate

# Bootstrap the python package installation.  We need PyYAML to read the
# configuration files.
easy_install pip==0.8.2

echo
echo "Current python packages:"
pip freeze
echo

pip install kafka-python==0.9.3
pip install pylint==0.23.0
pip install MySQL-python==1.2.5

echo ""
echo "##################################################"
echo ""
echo "  $ENVDIR now holds the Python environment for"
echo "  running tvset.  To activate this environment,"
echo "  run:"
echo ""
echo "      $ . $ENVDIR/bin/activate"
echo ""
echo "##################################################"
echo ""
