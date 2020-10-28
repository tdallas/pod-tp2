#!/bin/bash
cd ${0%/*}
java  -cp 'lib/jars/*' "itba.pod.server.Server" $*

