#! /bin/bash

pythonpath=../../python-packages/lib.linux-x86_64-2.6:../external/deconv:../external/python-wcslib
# Add path to unittest2; unittest2 lives in the source tree
pythonpath=${pythonpath}:../../../../tkp/trunk/tkp/tests
if [ -z "$PYTHONPATH" ]
then
    export PYTHONPATH=${pythonpath}
else
    export PYTHONPATH=${PYTHONPATH}:${pythonpath}
fi

ld_library_path=../../../tkp/trunk/external/libwcstools:/opt/cep/LofIm/daily/casacore/lib:/opt/cep/LofIm/daily/pyrap/lib
if [ -z "$LD_LIBRARY_PATH" ]
then
    export LD_LIBRARY_PATH=${ld_library_path}
else
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${ld_library_path}
fi
# Needs check whether we're on OS X, but setting an extra
# environment variable should not be a problem
if [ -z "$DYLD_LIBRARY_PATH" ]
then
    export DYLD_LIBRARY_PATH=${ld_library_path}
else
    export DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}:${ld_library_path}
fi

# Extra paths needed on CEP1
export PYTHONPATH=../../python-packages/lib:${PYTHONPATH}:/opt/pythonlibs/lib/python2.5/site-packages/:/opt/MonetDB/
# Extra paths needed on CEP2
export PYTHONPATH=${PYTHONPATH}:/opt/cep/MonetDB/lib/python/site-packages:/opt/cep/LofIm/daily/pyrap/lib:/home/rol/sw/lib/python2.6/site-packages
dir=`dirname $0`

result=`python ${dir}/test.py "$@"`
exit $result