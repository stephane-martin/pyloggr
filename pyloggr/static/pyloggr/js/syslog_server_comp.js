define(['require', 'react', 'pyloggr/js/syslog_client_comp'], function(require, React, SyslogClient) {

// one syslog server
// list of syslog clients

var SyslogServer = React.createClass({displayName: "SyslogServer",
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    var rows = [];
    var clients = this.props.clients;
    Object.keys(clients).forEach(function(client_id) {
        var client = clients[client_id];
        rows.push(React.createElement(SyslogClient, React.__spread({key: client_id},  client)));
    });
    return (
        React.createElement("div", {id: "syslogserver{this.props.id}"}, 
          React.createElement("h2", null, "Syslog process ", this.props.id, " listening on [", this.props.ports.join(','), "]"), 
          React.createElement("table", {id: "listofclients{this.props.id}"}, 
            React.createElement("thead", null, 
              React.createElement("tr", null, React.createElement("th", null, "IP source"), React.createElement("th", null, "port source"), React.createElement("th", null, "Port destination"))
            ), 
            React.createElement("tbody", null, rows)
          )


        )


    )
  }
});

return SyslogServer;

});