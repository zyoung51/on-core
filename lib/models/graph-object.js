// Copyright 2015, EMC, Inc.

'use strict';

module.exports = GraphModelFactory;

GraphModelFactory.$provide = 'Models.GraphObject';
GraphModelFactory.$inject = [
    'Model',
    'Constants',
    'Services.Configuration'
];

function GraphModelFactory (Model, Constants, configuration) {
    var dbType = configuration.get('taskgraph-store', 'mongo');
    return Model.extend({
        connection: dbType,
        identity: 'graphobjects',
        autoPK: true,
        attributes: {
            instanceId: {
                type: 'string',
                required: true,
                unique: true,
                uuidv4: true,
                index: true
            },
            context: {
                type: 'json',
                required: true,
                json: true
            },
            definition: {
                type: 'json',
                required: true,
                json: true
            },
            tasks: {
                type: 'json',
                required: true,
                json: true
            },
            node: {
                model: 'nodes'
            },
            serviceGraph : {
                type: 'boolean',
                defaultsTo: false,
                index: true
            },
            contextTarget: {
                type: 'string',
                defaultsTo: '',
                index: true
            },
            domain: {
                type: 'string',
                defaultsTo: 'default',
            },
            '_status': {
                type: 'string',
                defaultsTo: ''
            },
            rowrev: {
                type: 'string',
                defaultsTo: ''
            },
            logContext: {
                type: 'json',
                json: true
            },
            // This is duplciated in definition, but service-graph.js relies on its presence
            // in the model at this location.
            injectableName: {
                type: 'string'
            },
            active: function() {
                var obj = this.toObject();
                return Constants.Task.ActiveStates.indexOf(obj._status) > -1;
            }
        }
    });
}
