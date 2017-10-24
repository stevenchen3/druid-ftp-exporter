#!/usr/bin/env bash


java -cp target/scala-2.11/druid-ftp-assembly-0.9.jar io.alphash.RawDataExporter $@
