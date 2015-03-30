define(
    ['require', 'jquery', 'stringformat', 'react', 'pyloggr/js/on_off_status_comp',
    'pyloggr/js/syslog_servers_comp', 'pyloggr/js/rabbitmq_status_comp', 'pyloggr/js/pgsql_stats_comp'],

    function (require, $, stringformat, React, OnOffStatus, SyslogServers, RabbitMQ, PGSQL) {

    stringformat.format.extend(String.prototype);



    function html_for_syslog_server(id) {
        return "<div id='syslogserver" + id + "'>" +
            "<h2>Syslog process " + id + "</h2>" +
            "<table id='listofclients" + id + "'>" +
            "<thead>" +
            "<tr><th>IP source</th><th>port source</th><th>Port destination</th></tr>" +
            "</thead>" +
            "<tbody>" +
            "</tbody>" +
            "</table>" +
            "</div>";
    }


    var ws = new WebSocket("ws://127.0.0.1:8888/syslog/websocket/");
    ws.onopen = function() {
        ws.send("getStatus");
    };

    ws.onmessage = function (evt) {
        var r = JSON.parse(evt.data);
        // alert(JSON.stringify(r, null, 4));

        if (r.action == 'queues.stats') {
            React.render(React.createElement(RabbitMQ, React.__spread({},  r)), document.getElementById('list_of_rabbitmq_queues'));
        } else if (r.action == 'status') {
            React.render(React.createElement(OnOffStatus, React.__spread({},  r)), document.getElementById('list_of_status'));
        } else if (r.action == 'syslogs') {
            React.render(React.createElement(SyslogServers, React.__spread({},  r)), document.getElementById('list_of_syslog_servers'));
        } else if (r.action == 'pgsql.stats') {
            React.render(React.createElement(PGSQL, React.__spread({},  r)), document.getElementById('list_of_pgsql_stats'));
        } else {
            alert(JSON.stringify(r, null, 4));
        }
    };
});


