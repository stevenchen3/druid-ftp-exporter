#!/usr/bin/env bash

java -cp target/scala-2.11/druid-ftp-exporter-assembly-0.9.0-SNAPSHOT.jar io.alphash.RawDataExporter $@
