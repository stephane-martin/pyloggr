define(['require', 'react'], function(require, React) {

// one syslog client

var SyslogClient = React.createClass({displayName: "SyslogClient",
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    return (
        React.createElement("tr", null, 
          React.createElement("td", null, this.props.host), 
          React.createElement("td", null, this.props.client_port), 
          React.createElement("td", null, this.props.server_port)
        )


    )
  }
});

return SyslogClient;

});