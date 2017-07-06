#!/bin/bash
if [[ $(uname -r) == 2.* ]]; then
        export LD_LIBRARY_PATH=/opt/rh/python27/root/usr/lib64/
        exec ./VIRTUALENV/virtualenv-libs/bin/python "$@"
else
        exec ./VIRTUALENV/virtualenv-libs/bin/python "$@"
fi
