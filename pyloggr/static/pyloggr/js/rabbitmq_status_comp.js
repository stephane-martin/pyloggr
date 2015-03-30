define(['require', 'react'], function(require, React) {

// details about rabbitmq queues

var RabbitMQ = React.createClass({displayName: "RabbitMQ",
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    var rows = [];
    var status = this.props.status;
    var queues = this.props.queues;
    Object.keys(queues).forEach(function(queue_name) {
        var queue = queues[queue_name];
        rows.push(React.createElement("tr", {id: "queue_{queue_name}"}, React.createElement("td", null, queue_name), React.createElement("td", null, queue['messages'])));
    });
    return (
        React.createElement("div", {id: "rabbitmq_queues"}, 
            React.createElement("h2", null, "RabbitMQ queues"), 
            React.createElement("table", {id: "queues"}, 
                rows
            )
        )
    )
  }
});

return RabbitMQ;

});