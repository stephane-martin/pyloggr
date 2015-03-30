define(['require', 'react'], function(require, React) {

// details about rabbitmq queues

var PGSQL = React.createClass({
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    var rows = [];
    var status = this.props.status;
    var stats = this.props.stats;
    return (
        <div id="postgresql_stats">
            <h2>PostgreSQL stats: {stats} events</h2>
        </div>
    )
  }
});

return PGSQL;

});