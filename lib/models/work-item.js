// Copyright (c) 2015, EMC Corporation

'use strict';

module.exports = WorkItemModelFactory;

WorkItemModelFactory.$provide = 'Models.WorkItem';
WorkItemModelFactory.$inject = [
    'Model',
    '_',
    'Promise',
    'Assert',
    'Constants',
    'Services.Configuration',
    'TaskGraph.Store'
];

function WorkItemModelFactory (
    Model, 
    _, 
    Promise,
    assert, 
    Constants, 
    configuration,
    taskGraphStore
) {
    var connection = configuration.get('taskgraph-store', 'mongo');

    function getAdjustedInterval(workitem) {
        return Math.min(
                workitem.pollInterval * Math.pow(2, workitem.failureCount + 1),
                (60 * 60 * 1000)
            );
    }

    return Model.extend({
        connection: connection,
        identity: 'workitems',
        attributes: {
            name: {
                type: 'string',
                required: true
            },
            node: {
                model: 'nodes',
                defaultsTo: null
            },
            config: {
                type: 'json',
                defaultsTo: {}
            },
            pollInterval: {
                type: 'integer',
                required: true
            },
            nextScheduled: {
                type: 'datetime',
                defaultsTo: null
            },
            lastStarted: {
                type: 'datetime',
                defaultsTo: null
            },
            lastFinished: {
                type: 'datetime',
                defaultsTo: null
            },
            leaseToken: {
                type: 'string',
                uuidv4: true,
                defaultsTo: null
            },
            leaseExpires: {
                type: 'datetime',
                defaultsTo: null
            },
            failureCount: {
                type: 'integer',
                defaultsTo: 0,
                required: true
            },
            paused:{
                type: 'boolean',
                defaultsTo: false
            },
            toJSON: function() {
                var obj = this.toObject();
                obj.config = _.omit(obj.config, function(value, key) {
                    return _.some(Constants.Logging.Redactions, function(pattern) {
                        return key.match(pattern);
                    });
                });
                return obj;
            }
        },

        startNextScheduled: function startNextScheduled(leaseToken, criteria, leaseDuration) {
            var self = this;

            return taskGraphStore.checkoutTimer(leaseToken, criteria, leaseDuration)
            .then(function(workitem) {
                if(!timer) {
                    return self.startNextScheduled(leaseToken, criteria, leaseDuration);
                }
                return self.deserialize(workitem);
            });
        },

        findExpired: function findExpired(leaseExpiry) {
            return this.find({
                leaseExpires: { lessThan: leaseExpiry }
            });
        },

        setFailed: function setFailed(leaseToken, workItems) {
            var self = this;
            if (!Array.isArray(workItems)) {
                workItems = Array.prototype.slice.call(arguments, 1);
            }
            var now = new Date();
            return Promise.all(_.map(workItems, function (workItem) {
                var nextScheduled = new Date(now.valueOf() + getAdjustedInterval(workItem));
                return taskGraphStore.updateTimerStatus(workItem.id, Constants.Task.States.Failed, nextScheduled, now)
            })).then(function (workItems) {
                return _.flattenDeep(workItems);
            });
        },

        setSucceeded: function setSucceeded(leaseToken, workItems) {
            var self = this;
            if (!Array.isArray(workItems)) {
                workItems = Array.prototype.slice.call(arguments, 1);
            }
            var now = new Date();
            return Promise.all(_.map(workItems, function (workItem) {
                var nextScheduled = new Date(now.valueOf() + workItem.pollInterval);
                return taskGraphStore.updateTimerStatus(workItem.id, Constants.Task.States.Succeeded, nextScheduled, now)
            })).then(function (workItems) {
                return _.flattenDeep(workItems);
            });
        },

        beforeValidate: function(obj, next) {
            if (obj.type && _(Constants.WorkItems.Pollers).has(obj.type.toUpperCase())) {
                obj.name = Constants.WorkItems.Pollers[obj.type.toUpperCase()];
                delete obj.type;
            }
            next();
        },

        beforeCreate: serialize,

        beforeUpdate: serialize,

        findPollers: function findPollers(criteria) {
            var self = this;
            return this.find(criteria)
            .then(function(pollers) {
                return _.map(pollers, self.deserialize, self);
            });
        },

        deserialize: function(obj) {
            return sanitize(obj, /_/ig, '.');
        }
    });

    function sanitize(obj, search, replace) {
        if(!_.has(obj.config, 'oids')) {
            return obj;
        }
        obj.config.oids = _.map(obj.config.oids, function(oid) {
            return oid.replace(search, replace);
        });

        if(!obj.config.alerts) {
            return obj;
        }
        obj.config.alerts = _.map(obj.config.alerts, function(alertItem) {
            return _.transform(alertItem, function(result, alertVal, alertKey) {
                result[alertKey.replace(search, replace)] = alertVal;
            });
        });
        return obj;
    }

    function serialize(obj, next) {
        sanitize(obj, /\./ig, '_');
        return next();
    }
}
