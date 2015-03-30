define(['require', 'react'], function(require, React) {

// details about rabbitmq queues

var PGSQL = React.createClass({displayName: "PGSQL",
  getDefaultProps: function() {
      return {

      };
  },
  render: function(){
    var rows = [];
    var status = this.props.status;
    var stats = this.props.stats;
    return (
        React.createElement("div", {id: "postgresql_stats"}, 
            React.createElement("h2", null, "PostgreSQL stats: ", stats, " events")
        )
    )
  }
});

return PGSQL;

});