define(['require', 'react'], function(require, React) {

var OnOffStatus = React.createClass({
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
        <table id="on_off_status">
            <thead>
                <tr>
                    <th>RabbitMQ</th>
                    <th>Redis</th>
                    <th>PostgreSQL</th>
                    <th>Syslog</th>
                    <th>Parser</th>
                    <th>Shipper</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{this.props.rabbitmq}</td>
                    <td>{this.props.redis}</td>
                    <td>{this.props.postgresql}</td>
                    <td>{this.props.syslog}</td>
                    <td>{this.props.parser}</td>
                    <td>{this.props.shipper}</td>
                </tr>
            </tbody>
        </table>
    )
  }
});

return OnOffStatus;

});