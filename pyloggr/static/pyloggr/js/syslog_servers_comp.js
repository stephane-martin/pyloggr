define(['require', 'react', 'pyloggr/js/syslog_server_comp'], function(require, React, SyslogServer) {

// list of syslog servers

var SyslogServers = React.createClass({displayName: "SyslogServers",
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    var rows = [];
    var servers = this.props.servers;
    Object.keys(servers).forEach(function(server_id) {
        var server = servers[server_id];
        rows.push(React.createElement(SyslogServer, React.__spread({key: server_id},  server)));
    });
    return (
        React.createElement("div", {id: "syslogservers"}, 
          rows
        )


    )
  }
});

return SyslogServers;

});