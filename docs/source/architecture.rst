============
Architecture
============

pyloggr architecture is not yet fully stabilized. Nevertheless here are the main characteristics :

- pyloggr has several components. The components are supposed to be ran as independant process
- Components are based on the tornado asynchronous framework
- Communication of syslog messages between components uses RabbitMQ, so that we have good resilience properties

Components can be started/stopped using the `pyloggr_ctl` script.


.. graphviz::

   digraph foo {
      "syslog_server" [shape=box];
      "RabbitMQ" [style=filled];
      "PostgreSQL" [style=filled];
      "Redis" [style=filled];
      "parser" [shape=box];
      "shipper2pgsql" [shape=box];
      "harvest" [shape=box];
      "collector" [shape=box];
      "syslog clients" -> "syslog_server" [style=bold,color=blue];
      "syslog_server" -> "RabbitMQ" [style=bold,color=blue];
      "syslog_server" -> "Redis" [style=dotted,color=red];
      "Redis" -> "collector"  [style=dotted,color=red];
      "collector" -> "RabbitMQ"  [style=dotted,color=red];
      "parser" -> "RabbitMQ" [style=bold,color=blue];
      "RabbitMQ" -> "parser" [style=bold,color=blue];
      "RabbitMQ" -> "shipper2pgsql" [style=bold,color=blue];
      "harvest" -> "RabbitMQ";
      "shipper2pgsql" -> "PostgreSQL" [style=bold,color=blue];
   }


Components
==========

Components source is located in the `main` directory.

syslog_server
-------------

syslog_server is a... syslog server. It can receive syslog messages with TCP or RELP (RELP is a reliable
syslog protocol used by Rsyslog).

Received messages can be formatted in traditional syslog format (RFC 3164), modern syslog format (RFC 5424), or as
JSON messages.

When using TCP transport, both traditional LF framing, or octet framing can be used.

syslog_server can also pack several messages into a single one, using configurable `Packers`.

harvest
-------

Sometimes you just can't use syslog... For example the paranoid production team could refuse to install
rsyslog on some servers (don't laugh, that's real). pyloggr can also receive full log files using FTP, SSH, ...
harvest is responsible to monitor the upload directory. When a file has been fully uploaded, it reads it and injects
the log lines as syslog messages.

parser
------

The parser process takes messages from RabbitMQ, parses them, and applies configurable filters to each message.
For example, a 'grok' filter similar to logstash is provided.

Filters application is multi-threaded.

shipper2pgsql
-------------

The shipper takes messages from RabbitMQ and stores them in the PostgreSQL database.

web_frontend
------------

The frontend is the web interface to pyloggr. It can be used to monitor pyloggr activity, or to query the log
database.

collector
---------

When RabbitMQ is accidently not available, syslog_server will try to save the current messages in Redis (they it will
shutdown itself so that no more syslog messages can come in).

collector processes the messages in Redis and transfers them back to RabbitMQ when RabbitMQ is back online.


Never lose a syslog message !
=============================

pyloggr project was initially started as a prototype after looking at logstash queues. Logstash is a fine and
useful piece of software (love it). But currently in the 1.4 branch, messages are stored in internal memory queues.
This means that when logstash stops, or crashes, you can actually lose some lines.

Moreover, even if logstash implements the RELP protocol as a possible source, the incoming messages are
ACKed very quickly (when they move to the "filter" queue actually). So any problem with filters or with outputs can
generate a message loss.

That's why pyloggr:
- implements RELP for incoming messages
- uses RabbitMQ for messages transitions
- only ACKs a message from step A when step A+1 has taken responsibility

This way we can ensure that messages won't be lost.





