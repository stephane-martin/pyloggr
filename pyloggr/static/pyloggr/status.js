define(function (require) {
    var $ = require('jquery');
    var format = require('stringformat').format;
    $(function () {
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
        format.extend(String.prototype);

        var ws = new WebSocket("ws://127.0.0.1:8888/syslog/websocket/");
        ws.onopen = function() {
            ws.send("Hello, world");
        };
        ws.onmessage = function (evt) {
            var r = JSON.parse(evt.data);
            // alert(JSON.stringify(r, null, 4));

            if (r.action == 'add_client') {
                var where = '#listofclients' + r.client.task_id + ' > tbody:last';
                $(where).append("<tr id='{0}'><td>{1}</td><td>{2}</td><td>{3}</td>".format(
                    r.client.id, r.client.host, r.client.client_port, r.client.server_port
                ));
            } else if (r.action == 'remove_client') {
                $('#'+r.client.id).remove();
            } else if (r.action == 'remove_server') {
                var where = '#syslogserver'+ r.id;
                $('#len').text(parseInt($('#len').text()) - 1);
                $(where).remove();
            } else if (r.action == 'add_server') {
                $('#len').text(parseInt($('#len').text()) + 1);
                var where = "#syslogservers";
                $(where).append(html_for_syslog_server(r.id));
            } else if (r.action == 'queues.stats') {
                alert('yes!');
            } else {
                alert(JSON.stringify(r, null, 4));
            }
        };

    });
});


