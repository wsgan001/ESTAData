#!/bin/bash
# This is an example of how to run the jar file
# it is recommended to assign as much as DirectMemory as possible to the JVM.
java -d64 -Xmx4G -XX:MaxDirectMemorySize=13g -XX:+UseParallelGC -jar mining.jar "$@"

