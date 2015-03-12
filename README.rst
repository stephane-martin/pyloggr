Overview
--------

pyloggr is a set of tools to
- centralize logs
- parse logs and apply some filters
- store logs in a convenient database
- search logs

* Free software: GPLv3 (or later) license
* Documentation: https://pyloggr.readthedocs.org.
* Github: https://github.com/stephane-martin/pyloggr

Features
--------

- Syslog server: implements RFC 5424 and RFC 3164 formatting, can receive logs over TCP, TCP/TLS or RELP
- Apply some filters to logs. For instance pyloggs supports the grok filter, similar to logstash
- Database storage: currently in PostgreSQL, using JSONB support
- Web frontend: pyloggr monitoring, log exploration

Todo
----

See `Github issues <https://github.com/stephane-martin/pyloggr/issues>`_

