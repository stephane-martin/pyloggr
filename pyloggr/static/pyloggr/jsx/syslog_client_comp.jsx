define(['require', 'react'], function(require, React) {

// one syslog client

var SyslogClient = React.createClass({
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    return (
        <tr>
          <td>{this.props.host}</td>
          <td>{this.props.client_port}</td>
          <td>{this.props.server_port}</td>
        </tr>


    )
  }
});

return SyslogClient;

});