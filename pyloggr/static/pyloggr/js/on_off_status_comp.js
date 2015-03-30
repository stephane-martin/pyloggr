define(['require', 'react'], function(require, React) {

var OnOffStatus = React.createClass({displayName: "OnOffStatus",
  getDefaultProps: function() {
      return {
          "redis": 'Off',
          "rabbitmq": 'Off',
          "postgresql": 'Off',
          "syslog": 'Off',
          "parser": 'Off',
          "shipper": 'Off'
      };
  },
  render: function(){
    return (
        React.createElement("table", {id: "on_off_status"}, 
            React.createElement("thead", null, 
                React.createElement("tr", null, 
                    React.createElement("th", null, "RabbitMQ"), 
                    React.createElement("th", null, "Redis"), 
                    React.createElement("th", null, "PostgreSQL"), 
                    React.createElement("th", null, "Syslog"), 
                    React.createElement("th", null, "Parser"), 
                    React.createElement("th", null, "Shipper")
                )
            ), 
            React.createElement("tbody", null, 
                React.createElement("tr", null, 
                    React.createElement("td", null, this.props.rabbitmq), 
                    React.createElement("td", null, this.props.redis), 
                    React.createElement("td", null, this.props.postgresql), 
                    React.createElement("td", null, this.props.syslog), 
                    React.createElement("td", null, this.props.parser), 
                    React.createElement("td", null, this.props.shipper)
                )
            )
        )
    )
  }
});

return OnOffStatus;

});