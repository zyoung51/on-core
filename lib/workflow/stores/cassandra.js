// Copyright 2016, EMC, Inc.
'use strict';

module.exports = cassandraStoreFactory;
cassandraStoreFactory.$provide = 'TaskGraph.Stores.Cassandra';
cassandraStoreFactory.$inject = [
    'Services.Waterline',
    'Promise',
    'Constants',
    'Errors',
    'Assert',
    '_',
    'uuid'
];

function cassandraStoreFactory(waterline, Promise, Constants, Errors, assert, _, uuid) {
    var exports = {};

    // NOTE: This is meant to be idempotent, and just drop the update silently
    // if the graph has already been marked as done elsewhere and the query returns
    // empty.
    /**
     * Atomically sets the graph document in the graphobjects collection given by data.graphId
     * to the given state
     * @param {String} state - the finished state to set the graph to
     * @param {Object} data
     * @param {String} data.graphId - the graphId of the graph to be set to done
     * @memberOf store
     * @returns {Promise} - a promise for the graph after its state has been set
     */
    exports.setGraphDone = function(state, data) {
        assert.string(state, 'state');
        assert.object(data, 'data');
        assert.uuid(data.graphId, 'data.graphId');

        var query = 'SELECT * FROM graphobjects WHERE instanceId = ? AND "_status" = ? LIMIT 1';
        return waterline.graphobjects.runCassandraQuery(query, [data.graphId, Constants.Task.States.Pending])
        .then(function(rows) {
            if(rows && rows.length) {
                var row = waterline.graphobjects.convertCassandraToWaterline(rows[0], waterline.graphobjects.definition);
                console.log('setGraphDone', row.logContext.graphName, state);
                var update = 'UPDATE graphobjects SET "_status" = ?, updatedat = ? WHERE id = ? IF "_status" = ?';
                return waterline.graphobjects.runCassandraQuery(update, [
                    state, new Date(), row.id, Constants.Task.States.Pending])
                .then(function(disposition) {
                    if(disposition[0]['[applied]'] !== true) {
                        return null;
                    }
                    row._status = state;
                    return row;
                });
            }
            return null;
        });
    };

    /**
     * @param {Object} indexObject an object with keys that correspond to the mogo
     * collections on which to place indexes and values that are arrays of indexes
     * @example {
     *          taskdependencies: [
     *              {taskId: 1},
     *              {graphId: 1},
     *              {taskId: 1, graphId: 1}
     *          ],
     *          graphobjects: [
     *              {instanceId: 1}
     *          ]
     *  }
     * @memberOf store
     * @returns {Promise}
     */
    exports.setIndexes = function(indexObject) {
        return Promise.resolve();  //TODO: Because of the way CQL works, need to be careful
                                   //      just allowing these indexes to be setup the same 
                                   //      as other databases do it.
        /*
        return Promise.all(_.flatten(_.map(indexObject, function(indexObj, key) {
                return _.map(indexObj, function(index) {
                    return waterline[key].createCassandraIndexes(index);
                });
            }))
        );
        */
    };

    /**
     * Sets the state of a reachable, matching task in the taskdependencies collection
     * and updates the task's context.
     * @param {Object} task - a task object
     * @param {String} task.graphId - the unique ID of the graph to which the task belongs
     * @param {Object} task.context - the task context object
     * @param {String} task.state - the state with which to update the task document
     * in the database
     * @memberOf store
     * @returns {Promise}
     */
    exports.setTaskState = function(task) {
        assert.uuid(task.taskId, 'taskId');
        assert.uuid(task.graphId, 'task.graphId');
        assert.string(task.state, 'task.state');
        assert.optionalObject(task.context, 'task.context');
        // TODO: including graphId with the intent that we'll create an
        // index against it in the database

        if(task.state !== Constants.Task.States.Succeeded){
            task.context = null;
        }

        var query = 'SELECT * FROM taskdependencies ' +
        'WHERE graphId = ? AND taskId = ? AND reachable = true';
        return waterline.taskdependencies.runCassandraQuery(query, [task.graphId, task.taskId])
        .then(function(rows) {
            if(rows && rows.length) {
                var updates = [];
                var params = [];
                var results = [];
                _.forEach(rows, function(row) {
                    var resrow = waterline.taskdependencies.convertCassandraToWaterline(row, waterline.taskdependencies.definition);
                    updates.push('UPDATE taskdependencies SET state = ?, context = ?,updatedat = ? WHERE id = ?');
                    params.push([task.state, task.context ? JSON.stringify(task.context) : '', new Date(), row.id]);
                    resrow.state = task.state;
                    resrow.context = task.context;
                    results.push(resrow);
                });
                return waterline.taskdependencies.runCassandraQuery(updates, params)
                .then(function() {
                    return results;
                });
            }
            return [];
        });
    };

    /**
     * Atomically sets the state of a task in the graphobjects collection
     * @param {Object} data
     * @param {String} data.taskId - the unique ID of the task
     * @param {String} data.graphId - the unique ID of the graph to which the task belongs
     * @param {String} task.state - the state with which to update the task subdocument
     * @memberOf store
     * @returns {Promise} - a promise for the graph document containing the task
     */
    exports.setTaskStateInGraph = function(data) {
        assert.uuid(data.taskId, 'data.taskId');
        assert.uuid(data.graphId, 'data.graphId');
        assert.string(data.state, 'data.state');

        // TODO: including graphId with the intent that we'll create an
        // index against it in the database

        var stateKey = ['tasks', data.taskId, 'state'].join('.');
        var errorKey = ['tasks', data.taskId, 'error'].join('.');
        var query = 'SELECT * FROM graphobjects WHERE instanceId = ? LIMIT 1';

        return waterline.graphobjects.findAndModifyCassandra(
            query, [data.graphId],
            function(row) {
                _.set(row, stateKey, data.state);
                if(data.error) {
                    _.set(row, errorKey, data.error);
                }
                _.forEach(data.context, function(val, key) {
                    _.set(row, ['context', key].join('.'), val);
                });
                var update = 'UPDATE graphobjects SET tasks = ?, context = ?, updatedat = ?, rowrev = ? WHERE id = ? IF rowrev = ?';
                return waterline.graphobjects.runCassandraQuery(update, [ 
                    JSON.stringify(row.tasks), JSON.stringify(row.context), new Date(), uuid('v4'), row.id, row.rowrev]);
            });
    };

    /**
     * Get the definition of a task from the taskdefinitions collection
     * @param {String} injectableName - the injectable name for the desired task
     * @returns {Promise} - a promise for the definition for the desired task
     * @memberOf store
     */
    exports.getTaskDefinition = function(injectableName) {
        return waterline.taskdefinitions.findOne({ injectableName: injectableName })
        .then(function(taskDefinition) {
            if (_.isEmpty(taskDefinition)) {
                throw new Errors.NotFoundError(
                    'Could not find task definition with injectableName %s'
                    .format(injectableName));
            }

            return taskDefinition.toJSON();
        });
    };

    /**
     * Persists a graph definition ot the graphdefinitions collection
     * @param {Object} definition - the graph definition to persist
     * @returns {Promise} a promise for the persisted graph definition object
     * @memberOf store
     */
    exports.persistGraphDefinition = function(definition) {
        assert.object(definition, 'definition');
        assert.string(definition.injectableName, 'definition.injectableName');

        var query = {
            injectableName: definition.injectableName
        };

        // create is an insert and inserts will update if exist
        return waterline.graphdefinitions.create(definition);
    };

    /**
     * Persists a task definition ot the taskdefinitions collection
     * @param {Object} definition - the task definition to persist
     * @returns {Promise} a promise for the persisted task definition object
     * @memberOf store
     */
    exports.persistTaskDefinition = function(definition) {
        assert.object(definition, 'definition');
        assert.string(definition.injectableName, 'definition.injectableName');

        // create is an insert and inserts will update if exist
        return waterline.taskdefinitions.create(definition);
    };

    /**
     * Gets one or all graph definitions from the graphdefinitions collection
     * @param {String=} injectableName - an optional injectableName for the desired
     * graph definition
     * @returns {Promise} a promise for the matching graph definition or
     * all graph definitions if no injectableName was given
     * @memberOf store
     */
    exports.getGraphDefinitions = function(injectableName) {
        var query = {};
        if (injectableName) {
            query.injectableName = injectableName;
        }
        return waterline.graphdefinitions.find(query)
        .then(function(graphs) {
            return _.map(graphs, function(graph) {
                return graph.toJSON();
            });
        });
    };

    /**
     * Gets one or all task definitions from the taskdefinitions collection
     * @param {String=} injectableName - an optional injectableName for the desired
     * task definition
     * @returns {Promise} a promise for the matching task definition or
     * all task definitions if no injectableName was given
     * @memberOf store
     */
    exports.getTaskDefinitions = function(injectableName) {
        var query = {};
        if (injectableName) {
            query.injectableName = injectableName;
        }
        return waterline.taskdefinitions.find(query);
    };

    /**
     * Persists a graph to the graphobjects collection
     * @param {Object} graph - the graph object to persist
     * @param {String} graph.instanceId - the unique ID for the graph instance
     * @returns {Promise} a promise for the persisted graph object
     * @memberOf store
     */
    exports.persistGraphObject = function(graph) {
        assert.object(graph, 'graph');
        assert.uuid(graph.instanceId, 'graph.instanceId');

        var query = {
            instanceId: graph.instanceId
        };

        graph.contextTarget = graph.context.target || '';
        return waterline.graphobjects.create(graph)
        .then(function() {
            return waterline.graphobjects.find(query);
        })
        .then(function(result) {
            if( result.length ) {
                return _.pick(result[0], ['id', 'instanceId']);
            }
        });
    };

    /**
     * Persists a task object and its dependencies to the taskdependencies collection
     * @param {Object} taskDependencyItem - the task object
     * @param {String} taskDependencyItem.taskId - the unique ID for the task
     * @param {Object} taskDependencyItem.dependencies - the list of dependencies for the task
     * @param {String[]} taskDependencyItem.terminalOnStates - the list of states for which this task
     * can be the last task in its graph
     * @param {String} graphId - the unique ID of the graph to which the task belongs
     * @returns {Promise} a promise for the created taskdependency object
     * @memberOf store
     */
    exports.persistTaskDependencies = function(taskDependencyItem, graphId) {
        assert.object(taskDependencyItem, 'taskDependencyItem');
        assert.uuid(taskDependencyItem.taskId, 'taskDependencyItem.taskId');
        assert.uuid(graphId, 'graphId');
        assert.object(taskDependencyItem.dependencies, 'taskDependencyItem.dependencies');
        assert.arrayOfString(
                taskDependencyItem.terminalOnStates, 'taskDependencyItem.terminalOnStates');

        var obj = {
            taskId: taskDependencyItem.taskId,
            graphId: graphId,
            state: Constants.Task.States.Pending,
            dependencies: taskDependencyItem.dependencies,
            terminalOnStates: taskDependencyItem.terminalOnStates,
            taskRunnerLease: '',
            taskRunnerHeartbeat: new Date()
        };
        return waterline.taskdependencies.create(obj);
    };

    /**
     * Gets a task subdocument from the graphobjects collection
     * @param {Object} data
     * @param {String} data.graphId - the unique ID of the graph to which the task belongs
     * @param {String} data.taskId - the unique ID of the desired task subdocument
     * @returns {Promise} a promise for an object containing the graphId, requested task
     * and the associated graph context
     * @memberOf store
     */
    exports.getTaskById = function(data) {
        assert.object(data, 'data');
        assert.uuid(data.graphId, 'data.graphId');
        assert.uuid(data.taskId, 'data.taskId');

        var query = {
            instanceId: data.graphId
        };

        return waterline.graphobjects.findOne(query)
        .then(function(graph) {
            return {
                graphId: graph.instanceId,
                context: graph.context,
                task: graph.tasks[data.taskId]
            };
        });
    };

    /**
     * Gets graphs with the attribute 'serviceGraph' marked true from the
     * graphobjects collection
     * @returns {Promise} a promise for the marked serivice graphs
     * @memberOf store
     */
    exports.getServiceGraphs = function() {
        var query = {
            serviceGraph: true
        };

        return waterline.graphobjects.find(query)
        .then(function(graphs) {
            return _.map(graphs, function(graph) {
                return graph.toJSON();
            });
        });
    };

    /**
     * Updates the lease/heartbeat for all tasks in the taskdependencies collection
     *  with the given lease
     * @param {String} leaseId - the taskRunner ID to match against the
     *  taskRunnerLease document field when updating heartbeats
     * @returns {Promise} a promise containing the number of updated leases
     * @memberOf store
     */
    exports.heartbeatTasksForRunner = function(leaseId) {
        assert.uuid(leaseId, 'leaseId');

        var query = 'SELECT * FROM taskdependencies ' +
        'WHERE taskrunnerlease = ?';
        return waterline.taskdependencies.runCassandraQuery(query, [leaseId])
        .then(function(rows) {
            if(rows && rows.length) {
                var results = [];
                return Promise.map(rows, function(row) {
                    results.push(waterline.taskdependencies.convertCassandraToWaterline(row, waterline.taskdependencies.definition));
                    var update = 'UPDATE taskdependencies SET taskRunnerHeartbeat = ?,updatedat = ? WHERE id = ? IF taskrunnerlease = ?';
                    var param = [new Date(), new Date(), row.id, leaseId];
                    return waterline.taskdependencies.runCassandraQuery(update, param);
                }).then(function() {
                    return results;
                });
            }
            return [];
        });
    };

    /**
     * Gets all tasks that match the given leaseId
     * @param {String} leaseId - the leaseId to match against the taskRunnerLease document field
     * @returns {Promise} a promise for the matching tasks from the taskdependencies collection
     * @memberOf store
     */
    exports.getOwnTasks = function(leaseId) {
        assert.uuid(leaseId, 'leaseId');

        var query = {
            taskRunnerLease: leaseId,
            reachable: true,
            state: Constants.Task.States.Pending
        };

        return waterline.taskdependencies.find(query);
    };

    /**
     * Gets the active graph associated with a nodeId
     * @param {String} target - the node Id for which to return active graphs
     * @returns {Promise} a promise for a graph object
     * @memberOf store
     */
    exports.findActiveGraphForTarget = function(target) {
        if (!target) {
            return Promise.resolve(null);
        }
        assert.string(target, 'target');

        var query = {
            contextTarget: target,
            _status: Constants.Task.States.Pending
        };

        return waterline.graphobjects.findOne(query);
    };
    /**
     * Gets all the active graphs within a given domain
     * @param {String} domain - the domain to get all active graphs for
     * @returns {Promise} a promise for all active graphs in the given domain
     * @memberOf store
     */
    exports.findActiveGraphs = function(domain) {
        assert.string(domain, 'domain');

        var query = {
            domain: domain,
            _status: Constants.Task.States.Pending
        };

        return waterline.graphobjects.find(query);
    };

    /**
     * Gets all tasks for a given domain that are finished but unevaluated from
     * the taskdependencies collection
     * @param {String} domain - the domain to get tasks from
     * @param {Number=} limit - an option limit on the number of tasks to return
     * @returns {Promise} a promise for the matching task objects
     * @memberOf store
     */
    exports.findUnevaluatedTasks = function(domain, limit) {
        assert.string(domain, 'domain');
        if (limit) {
            assert.number(limit, 'limit');
        }

        var query =  '' +
        'SELECT * FROM taskdependencies ' +
        'WHERE domain = ? AND evaluated = false AND reachable = true ';
        //if(limit) {
        //    query = query + ' LIMIT ' + limit
        //}
        return waterline.taskdependencies.runCassandraQuery(query, [domain])
        .then(function(rows) {
            var retval = _(rows).filter(function(row) {
                return (-1 !== _.indexOf(Constants.Task.FinishedStates, row.state));
            }).map(function(row) {
                return waterline.taskdependencies.convertCassandraToWaterline(row, waterline.taskdependencies.definition);
            }).value();
            return retval;
        });
    };

    /**
    * Gets all tasks for a given domain and graph that are ready to run from the
    * taskdependencies collection
    * @param {String} domain - the domain to get tasks from
    * @param {String=} graphId - the unique ID for the graph to fetch ready tasks from
    * @returns {Promise} a promise for the ready tasks
    * @memberOf store
    */
    exports.findReadyTasks = function(domain, graphId) {
        assert.string(domain, 'domain');

        if (graphId) {
            assert.uuid(graphId, 'graphId');
        }

        var query = {
            taskRunnerLease: '',
            reachable: true,
            state: Constants.Task.States.Pending
        };
        if (graphId) {
            query.graphId = graphId;
        }

        return waterline.taskdependencies.find(query)
        .then(function(tasks) {
            var retval = _(tasks).filter(function(task) {
                return _.isEmpty(task.dependencies);
            }).filter(function(task) {
                return task.domain === domain;
            }).value();
            if(retval.length)  {
                console.log('findReadyTasks', retval.length, graphId);
            }
            return  {
                tasks: _.map(retval, function(task) { return task.toJSON(); }),
                graphId: graphId || null
            };
        });
    };

    /**
     * Atomically check out a taskdependencies task document by marking it's lease with
     * the given taskRunner ID and setting the taskRunnerHeartbeat field to 'now'
     * @param {String} taskRunnerId - the unique ID of the taskRunner for which the
     * task is being checked out
     * @param {Object} data
     * @param {String} data.graphId - the unique ID of the graph to which the task belongs
     * @param {String} data.taskId - the unique ID of the task to be checked out
     * @returns {Promise} a promise for the checked out task
     * @memberOf store
     */
    exports.checkoutTask = function(taskRunnerId, data) {
        assert.object(data, 'data');
        assert.uuid(data.graphId, 'data.graphId');
        assert.uuid(data.taskId, 'data.taskId');

        var query = 'SELECT * FROM taskdependencies ' +
        'WHERE graphId = ? AND taskId = ? AND reachable = true LIMIT 1';
        return waterline.taskdependencies.findAndModifyCassandra(
            query, [data.graphId, data.taskId],
            function(row) {
                var update = 'UPDATE taskdependencies SET taskRunnerLease = ?, taskRunnerHeartbeat = ?, updatedat = ? ' +
                             'WHERE id = ? IF taskRunnerLease = ?';
                row.taskRunnerLease = taskRunnerId;
                return waterline.taskdependencies.runCassandraQuery(update, [ taskRunnerId, new Date(), new Date(), row.id, ''])
                .then(function(disposition) {
                    if(disposition[0]['[applied]'] !== true) {
                        if(disposition[0].taskrunnerlease !== taskRunnerId) {
                            return disposition;  // and retry
                        }
                    }
                    return [{'[applied]': true }];  // and don't retry
                });
            });
    };

    /**
     * Checks whether there are any pending, reachable, tasks corresponding to
     * the given graph ID.
     * @param {Object} data
     * @param {String} data.graphId - The uniqe ID of the graph to be checked
     * @retuns {Promise} a promise for an object with a boolean 'done' field
     * @memberOf store
     */
    exports.checkGraphFinished = function(data) {
        assert.object(data, 'data');
        assert.uuid(data.graphId, 'data.graphId');

        var query = {
            graphId: data.graphId,
            state: Constants.Task.States.Pending,
            reachable: true
        };

        return waterline.taskdependencies.findOne(query)
        .then(function(result) {
            if (_.isEmpty(result)) {
                console.log('checkGraphFinished is finished');
                data.done = true;
            } else {
                console.log('checkGraphFinished waiting on ' + result.length + ' tasks');
                data.done = false;
            }
            return data;
        });
    };

    /**
     * Updates the tasks dependant on the given task ID to reflect its new, finished
     * state in the taskdependencies collection
     * @param {Object} data - the task data object
     * @param {String} data.taskId - the unique ID of the task whose dependencies
     * should be updated
     * @param {String} data.graphId - the unique ID of the graph to which the task
     * belongs
     * @returns {Promise} a promise for the number of updated task documents
     * @memberOf store
     */
    exports.updateDependentTasks = function(data) {
        assert.object(data, 'data');
        assert.uuid(data.graphId, 'data.graphId');
        assert.uuid(data.taskId, 'data.taskId');
        assert.string(data.state, 'data.state');

        var checkState = [ data.state ].concat(Constants.Task.States.Finished);
        var query = 'SELECT * FROM taskdependencies WHERE graphId = ? AND reachable = true';
        return waterline.taskdependencies.runCassandraQuery(query, [ data.graphId ])
        .then(function(rows) {
            if(rows && rows.length) {
                return Promise.map(rows, function(row) {
                    return waterline.taskdependencies.findAndModifyCassandra(
                        'SELECT * FROM taskdependencies WHERE id = ?', [row.id],
                        function(row) {
                            if( -1 != _.indexOf(checkState, _.get(row, 'dependencies.' + data.taskId))) {
                                delete row.dependencies[data.taskId];
                                var update = 'UPDATE taskdependencies SET dependencies = ?, updatedat = ?, rowrev = ? WHERE id = ? IF rowrev = ?';
                                return waterline.taskdependencies.runCassandraQuery(update, [
                                    JSON.stringify(row.dependencies), new Date(), uuid('v4'), row.id, row.rowrev]);
                            }
                            return Promise.resolve([{'[applied]': true }]);
                        });
                });
            }
            return [];
        });
    };

    /**
     * Updates tasks which will no longer be reachable as a result of the given
     * task's state so that they are marked as unreachable in the taskdependencies
     * collection
     * @param {Object} data - the task data object
     * @param {String} data.taskId - the unique ID of the task whose dependencies
     * should be updated
     * @param {String} data.graphId - the unique ID of the graph to which the task
     * belongs
     * @returns {Promise} a promise for the number of updated task documents
     * @memberOf store
     */
    exports.updateUnreachableTasks = function(data) {
        assert.object(data, 'data');
        assert.uuid(data.graphId, 'data.graphId');
        assert.uuid(data.taskId, 'data.taskId');
        assert.string(data.state, 'data.state');

        var checkState = _.difference(Constants.Task.FinishedStates, [data.state]);
        var query = 'SELECT * FROM taskdependencies WHERE graphId = ? AND reachable = true';
        return waterline.taskdependencies.runCassandraQuery(query, [ data.graphId ])
        .then(function(rows) {
            if(rows && rows.length) {
                var updates = [];
                var params = [];
                var results = [];
                _.forEach(rows, function(row) {
                    var resrow = waterline.taskdependencies.convertCassandraToWaterline(row, waterline.taskdependencies.definition);
                    if( -1 != _.indexOf(checkState, _.get(resrow, 'dependencies.' + data.taskId))) {
                        updates.push('UPDATE taskdependencies SET reachable = false, updatedat = ? WHERE id = ?');
                        params.push([ new Date(), row.id]);
                        results.push(resrow);
                    }
                });
                if(updates.length) {
                    return waterline.taskdependencies.runCassandraQuery(updates, params)
                    .then(function() {
                        return results;
                    });
                }
            }
            return [];
        });
    };

    /**
     * Marks the given task document's evaluated field to true in the taskdependencies
     * collection
     * @param {Object} data
     * @param {String} data.taskId - the unique ID for the task which should be marked
     * evaluated
     * @param {String} data.graphId - the unique ID of the graph to which the task
     * belongs
     * @returns {Promise} a promise for the new, updated, task document
     * @memberOf store
     */
    exports.markTaskEvaluated = function(data) {
        assert.object(data, 'data');
        assert.uuid(data.graphId, 'data.graphId');
        assert.uuid(data.taskId, 'data.taskId');

        var query = 'SELECT * FROM taskdependencies ' +
        'WHERE graphId = ? and taskId = ? AND reachable = true';
        return waterline.taskdependencies.runCassandraQuery(query, [data.graphId, data.taskId])
        .then(function(rows) {
            if(rows && rows.length) {
                var row = waterline.taskdependencies.convertCassandraToWaterline(rows[0], waterline.taskdependencies.definition);
                var update = 'UPDATE taskdependencies SET evaluated = true, updatedat = ? WHERE id = ? ';
                return waterline.taskdependencies.runCassandraQuery(update, [ new Date(), row.id ])
                .then(function() {
                    row.evaluated = true;
                    return row;
                });
            }
            return null;
        });
   };

    /**
     * Finds all reachable taskdependencies documents whose leases are more than the given
     * leaseAdjust milliseconds old
     * @param {String} domain - the domain to restrict the search to
     * @param {Number} leaseAdjust - the time after which to consier a lease expired in milliseconds
     * @returns {Promise} a promise for all taskdependencies documents whose leases are
     * expired according to the given leaseAdjst
     * @memberOf store
     */
    exports.findExpiredLeases = function(domain, leaseAdjust) {
        assert.string(domain, 'domain');
        assert.number(leaseAdjust, 'leaseAdjust');

        // TODO: something better has to be done on Cassandra, this is likely going to return
        //       to much data in the query
        var query = '' +
        'SELECT * FROM taskdependencies ' +
        'WHERE domain = ? AND reachable = true AND state = ?';
        return waterline.taskdependencies.runCassandraQuery(query, [domain, Constants.Task.States.Pending])
        .then(function(result) {
            var checkDate = new Date(Date.now() - leaseAdjust);
            var model = waterline.taskdependencies;
            var resrows = _.filter(result, function(it) {
                return it.taskrunnerlease && it.taskrunnerheartbeat < checkDate
            }).map(function(it) {
                return model.convertCassandraToWaterline(it, model.definition);
            });
            if( resrows.length ) {
                console.log('findExpiredLeases', resrows);
            }
            return resrows;
        });
    };

    /**
     * Expires the lease on a taskdependencies object by setting taskRunnerLease and
     * taskRunnerHeartbeat fields to null
     * @param {String} objId - the ID for a taskdependencies document
     * @returns {Promise} a promise for the taskdependencies document with expried lease
     * @memberOf store
     */
    exports.expireLease = function(objId) {
        //assert.string(objId, 'objId');
        var update = 'UPDATE taskdependencies SET taskRunnerLease = ?,updatedat = ? WHERE id = ?';
        return waterline.taskdependencies.runCassandraQuery(update, ['', new Date(), objId]);
    };

    /**
     * Find all taskdependencies documents that are unreachable or in a finished state
     * @param {Number=} limit - an optional limit to the number of documents returned
     * @returns {Promise} a promise for all complete or unreachable tasks
     * @memberOf store
     */
    exports.findCompletedTasks = function(limit) {
        var query = [];
        query.push({
            query: 'SELECT * FROM taskdependencies WHERE reachable = false',
            params: []
        });
        _.forEach(Constants.Task.FinishedStates, function(state) {
            query.push({
                query: 'SELECT * FROM taskdependencies WHERE evaluated = true AND state = ?',
                params: [state]
            });
        });

        return Promise.map(query, function(it) {
            if (limit != null) {  /* jshint ignore:line */
                it.query = it.query + ' LIMIT ' + limit;
            }
            return waterline.taskdependencies.runCassandraQuery(it.query, it.params);
        }).then(function(result) {
            return _([].concat.apply([], result))
                .uniq(function(o) { 
                    return o.id;
                }).map(function(item) {
                    var it = waterline.taskdependencies.convertCassandraToWaterline(item, waterline.taskdependencies.definition);
                    it._id = it.id;  // To make completed-task-poller.js happy
                    return it;
                }).value();
        });
    };

    /**
     * Deletes the taskdependencies documents by given IDs
     * @param {String[]} an array of objct IDs for the documents to be deleted
     * @returns {Promise}
     * @memberOf store
     */
    exports.deleteTasks = function(objectIds) {
        var query = [];
        var param = [];
        _.forEach(objectIds, function(id) {
            query.push('DELETE FROM taskdependencies WHERE id = ?');
            param.push([id]);
        });
        return waterline.taskdependencies.runCassandraQuery(query, param);
    };

    /**
     * Deletes a given graph by graphId from the graphobjects collection
     * @param {String} graphId - the unique ID for the graph to be deleted
     * @returns {Promise}
     * @memberOf store
     */
    exports.deleteGraph = function(graphId) {
        var query = {
            instanceId: graphId
        };

        console.log('deleteGraph', graphId);
        return waterline.graphobjects.destroy(query);
    };

    /**
     * Finds one graph document with state 'Pending' by graphId
     * @param {String} graphId - the unique ID of the desired active graph
     * @returns {Promise} a promise for the graph with the given graphId
     * @memberOf store
     */
    exports.getActiveGraphById = function(graphId) {
        assert.uuid(graphId);

        var query = {
            instanceId: graphId,
            _status: Constants.Task.States.Pending
        };

        return waterline.graphobjects.findOne(query);
    };

    return exports;
}
