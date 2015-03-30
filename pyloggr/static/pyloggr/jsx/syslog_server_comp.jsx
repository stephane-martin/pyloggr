define(['require', 'react', 'pyloggr/js/syslog_client_comp'], function(require, React, SyslogClient) {

// one syslog server
// list of syslog clients

var SyslogServer = React.createClass({
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    var rows = [];
    var clients = this.props.clients;
    Object.keys(clients).forEach(function(client_id) {
        var client = clients[client_id];
        rows.push(<SyslogClient key={client_id} {...client} />);
    });
    return (
        <div id="syslogserver{this.props.id}">
          <h2>Syslog process {this.props.id} listening on [{this.props.ports.join(',')}]</h2>
          <table id="listofclients{this.props.id}">
            <thead>
              <tr><th>IP source</th><th>port source</th><th>Port destination</th></tr>
            </thead>
            <tbody>{rows}</tbody>
          </table>


        </div>


    )
  }
});

return SyslogServer;

});