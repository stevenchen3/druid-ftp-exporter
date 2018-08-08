# Transfer data from Druid to FTP via streams

This repository implements a simple data exporter that converts [Druid](http://druid.io/) segments
(raw data) to CSV format and sends to FTP server via streams. Codes in this repository are not yet
covered by unit tests which is for prototyping purpose only and **NOT** for any production use.

As of this writing, Druid has not yet supported back-pressure aware streaming queries via broker.
See discussions [#4865](https://github.com/apache/incubator-druid/issues/4865) and [#4949](https://github.com/apache/incubator-druid/pull/4949) for details.
