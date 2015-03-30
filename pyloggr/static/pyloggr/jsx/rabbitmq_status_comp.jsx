define(['require', 'react'], function(require, React) {

// details about rabbitmq queues

var RabbitMQ = React.createClass({
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
        rows.push(<tr id="queue_{queue_name}"><td>{queue_name}</td><td>{queue['messages']}</td></tr>);
    });
    return (
        <div id="rabbitmq_queues">
            <h2>RabbitMQ queues</h2>
            <table id="queues">
                {rows}
            </table>
        </div>
    )
  }
});

return RabbitMQ;

});