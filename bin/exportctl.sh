#!/usr/bin/env bash

java -cp target/scala-2.11/druid-ftp-assembly-0.9.0-SNAPSHOT.jar io.alphash.RawDataExporter $@
