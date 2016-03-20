// Copyright 2016, EMC, Inc.

'use strict';

module.exports = TaskDependencyFactory;

TaskDependencyFactory.$provide = 'Models.TaskDependency';
TaskDependencyFactory.$inject = [
    'Model',
    'Constants',
    'Services.Configuration'
];

function TaskDependencyFactory (Model, Constants, configuration) {
    var dbType = configuration.get('taskgraph-store', 'mongo');
    return Model.extend({
        connection: dbType,
        identity: 'taskdependencies',
        autoPK: true,
        attributes: {
            domain: {
                type: 'string',
                defaultsTo: Constants.Task.DefaultDomain,
                index: true
            },
            taskId: {
                type: 'string',
                required: true,
                unique: true,
                uuidv4: true,
                index: true
            },
            graphId: {
                type: 'string',
                required: true,
                index: true
            },
            state: {
                type: 'string',
                required: true,
                index: true
            },
            evaluated: {
                type: 'boolean',
                defaultsTo: false,
                index: true
            },
            reachable: {
                type: 'boolean',
                defaultsTo: true,
                index: true
            },
            taskRunnerLease: {
                type: 'string',
                defaultsTo: null,
                index: true
            },
            taskRunnerHeartbeat: {
                type: 'date',
                defaultsTo: null
            },
            dependencies: {
                type: 'json',
                required: true
            },
            terminalOnStates: {
                type: 'array',
                defaultsTo: []
            },
            context: {
                type: 'json',
                defaultsTo: null
            },
            rowrev: {
                type: 'string',
                defaultsTo: ''
            },
            toJSON: function() {
                // Remove waterline keys that we don't want in our dependency object
                var obj = this.toObject();
                delete obj.createdAt;
                delete obj.updatedAt;
                delete obj.id;
                return obj;
            }
        }
    });
}
