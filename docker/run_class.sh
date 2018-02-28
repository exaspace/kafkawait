#!/bin/bash -eux
java -cp `find . -name '*.jar'|tr '\n' ':'` "$@"
