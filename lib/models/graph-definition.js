// Copyright 2015, EMC, Inc.

'use strict';

module.exports = GraphModelFactory;

GraphModelFactory.$provide = 'Models.GraphDefinition';
GraphModelFactory.$inject = [
    'Model',
    'Services.Configuration'
];

function GraphModelFactory (Model, configuration) {
    var dbType = configuration.get('taskgraph-store', 'mongo');
    return Model.extend({
        connection: dbType,
        identity: 'graphdefinitions',
        attributes: {
            friendlyName: {
                type: 'string',
                required: true
            },
            injectableName: {
                type: 'string',
                required: true,
                unique: true,
                primaryKey: true
            },
            tasks: {
                type: 'array',
                required: true,
                json: true
            },
            serviceGraph: {
                type: 'boolean',
                defaultsTo: false
            },
            toJSON: function() {
                // Remove waterline keys that we don't want in our graph objects
                var obj = this.toObject();
                delete obj.createdAt;
                delete obj.updatedAt;
                delete obj.id;
                return obj;
            }
        }
    });
}
