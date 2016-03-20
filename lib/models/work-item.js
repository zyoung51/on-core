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
    'Services.Configuration'
];

function WorkItemModelFactory (Model, _, Promise, assert, Constants, configuration) {
    var dbType = configuration.get('taskgraph-store', 'mongo');

    function byPollers() {
        return {
            or: _.map(Constants.WorkItems.Pollers, function (pollerName) {
                return { name: pollerName };
            })
        };
    }

    function getAdjustedInterval(workitem) {
        return Math.min(
                workitem.pollInterval * Math.pow(2, workitem.failureCount + 1),
                (60 * 60 * 1000)
            );
    }

    return Model.extend({
        connection: dbType,
        identity: 'workitems',
        autoPK: true,
        attributes: {
            name: {
                type: 'string',
                required: true,
                index: true
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
                defaultsTo: null,
                index: true
            },
            leaseExpires: {
                type: 'datetime',
                defaultsTo: null,
                index: true
            },
            failureCount: {
                type: 'integer',
                defaultsTo: 0,
                required: true
            },
            paused:{
                type: 'boolean',
                defaultsTo: false,
                index: true
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

        startNextScheduledCassandra : function startNextScheduledCassandra(leaseToken, criteria, leaseDuration) {
            var self = this;
            var now = new Date();
            return this.find(_.merge({}, criteria, { paused: false }, {leaseToken: ''}))
            .then(function(rows) {
                //console.log('startNextScheduledCassandra1', rows);
                if(rows) {
                    return rows[0];
                }
            })
            .then(function(workItems) {
                if(!workItems) {
                    return;
                }
                //TODO: Sort workItems by nextScheduled
                var workitem = self.convertCassandraToWaterline(workItems[0], self.definition);
                var update = 'UPDATE workitems SET lastStarted = ?, leaseToken = ?, leaseExpires = ? ' +
                'WHERE id = ? IF leaseToken = ?';
                return self.runCassandraQuery(update, [noew, leaseToken, new Date(now.valueOf() + leaseDuration), workItem.id, ''])
                .then(function(rows) {
                    console.log('startNextScheduledCassandra2', rows);
                    /* if not applied then grab the next one */
                    return self.deserialize(workitem)
                });
            })
        },

        startNextScheduledMongo: function startNextScheduledMongo(leaseToken, criteria, leaseDuration) {
            var self = this;
            var now = new Date();
            return this.findOne({
                where: {
                    $and: [
                        criteria,
                        { paused: false },
                        {
                            leaseToken: null,
                            $or: [
                                { nextScheduled: { lessThan: now } },
                                { nextScheduled: null }
                            ]
                        }
                    ]
                },
                sort: 'nextScheduled ASC'
            })
            .then(function (workItem) {
                if (!workItem) {
                    return;
                }
                return self.update({
                    id: workItem.id,
                    leaseToken: null,
                    $or: [
                        { nextScheduled: { lessThan: now } },
                        { nextScheduled: null }
                    ]
                },
                {
                    lastStarted: now,
                    leaseToken: leaseToken,
                    leaseExpires: new Date(now.valueOf() + leaseDuration)
                }).then(function (workItems) {
                    /* some other worker acquired the lease, so we request another
                     * work item to process. */
                    if (!workItems.length || workItems[0].leaseToken !== leaseToken) {
                        return self.startNextScheduled(leaseToken, criteria, leaseDuration);
                    }
                    return self.deserialize(workItems[0]);
                });
            });
        },

        startNextScheduled: function startNextScheduled(leaseToken, criteria, leaseDuration) {
            var self = this;
            if(dbType === 'mongo') {
                return self.startNextScheduledMongo(leaseToken, criteria, leaseDuration);
            } else if(dbType === 'cassandra') {
                return self.startNextScheduledCassandra(leaseToken, criteria, leaseDuration);
            }
        },

        findExpired: function findExpired(leaseExpiry) {
            return Promise.resolve([]);
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
                return self.update({
                    id: workItem.id,
                    leaseToken: leaseToken || workItem.leaseToken
                }, {
                    nextScheduled: new Date(now.valueOf() + getAdjustedInterval(workItem)),
                    failureCount: workItem.failureCount + 1,
                    lastFinished: now,
                    leaseToken: null,
                    leaseExpires: null
                });
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
                return self.update({
                    id: workItem.id,
                    leaseToken: leaseToken || workItem.leaseToken
                }, {
                    nextScheduled: new Date(now.valueOf() + workItem.pollInterval),
                    failureCount: 0,
                    lastFinished: now,
                    leaseToken: null,
                    leaseExpires: null
                });
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
            return this.find(_.merge({}, criteria, byPollers()))
            .then(function(pollers) {
                if (!Array.isArray(pollers)) {
                    pollers = Array.prototype.slice.call(arguments, 1);
                }
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
