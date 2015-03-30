define(['require', 'react', 'pyloggr/js/syslog_server_comp'], function(require, React, SyslogServer) {

// list of syslog servers

var SyslogServers = React.createClass({
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    var rows = [];
    var servers = this.props.servers;
    Object.keys(servers).forEach(function(server_id) {
        var server = servers[server_id];
        rows.push(<SyslogServer key={server_id} {...server} />);
    });
    return (
        <div id="syslogservers">
          {rows}
        </div>


    )
  }
});

return SyslogServers;

});