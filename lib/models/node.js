// Copyright 2015, EMC, Inc.

'use strict';

module.exports = NodeModelFactory;

NodeModelFactory.$provide = 'Models.Node';
NodeModelFactory.$inject = [
    'Model',
    'Services.Waterline',
    '_',
    'Promise',
    'Constants',
    'Services.Configuration'
];

var bson = require('bson');
function NodeModelFactory (Model, waterline, _, Promise, Constants, configuration) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.profiles || defaultDbType;
    return Model.extend({
        connection: dbType,
        identity: 'nodes',
        attributes: {
            id: {
                type: 'string',
                primaryKey: true
            },
            identifiers: {
                type: 'array',
                required: false,
                index: true
            },
            name: {
                type: 'string',
                required: true
            },
            obmSettings: {
                type: 'json',
                required: false,
                json: true
            },
            type: {
                type: 'string',
                enum: _.values(Constants.NodeTypes),
                defaultsTo: 'compute',
                index: true
            },
            workflows: {
                collection: 'graphobjects',
                via: 'node'
            },
            catalogs: {
                collection: 'catalogs',
                via: 'node'
            },
            sku: {
                model: 'skus'
            },
            snmpSettings: {
                type: 'json',
                json: true,
                required: false
            },
            bootSettings: {
                type: 'json',
                json: true,
                required: false
            },
            sshSettings: {
                type: 'json',
                json: true,
                required: false
            },
            autoDiscover: {
                type: 'boolean',
                defaultsTo: false
            },
            relations: {
                type: 'array',
                defaultsTo: []
            },
            tags: {
                type: 'array',
                defaultsTo: []
            },
            // We only count a node as having been discovered if
            // a node document exists AND it has any catalogs
            // associated with it
            discovered: function() {
                var self = this;
                return waterline.catalogs.findOne({"node": self.id})
                .then(function(catalog) {
                    return !_.isEmpty(catalog);
                });
            }
        },
        addTags: function(id, tags) {
            if(dbType === 'mongo') {
                return this.runNativeMongo('update', [
                    { _id: waterline.nodes.mongo.objectId(id) },
                    { 
                        $addToSet: { tags: { $each: tags } },
                        $set: { updatedAt: new Date() }
                    }
                ]);
            } else if(dbType === 'cassandra') {
                // TODO: FIXME
                var query = 'INSERT INTO nodes (tags) VALUES({?})';
                var params = [ tags.join(',') ];
                return this.runCassandraQuery( query, params );
            }
        },
        remTags: function(id, tag) {
            if(dbType === 'mongo') {
                return this.runNativeMongo('update', [
                    { _id: waterline.nodes.mongo.objectId(id) },
                    { 
                        $pull: { tags: tag },
                        $set: { updatedAt: new Date() }
                    }
                ]);
            } else if(dbType === 'cassandra') {
                // TODO: FIXME
            }
        },
        findByTag: function(tag) {
            return waterline.nodes.find({tags: tag});
        },
        beforeCreate: dbCompatibility
    });

    function dbCompatibility(obj, next) {
        if(dbType === 'cassandra') {
            var objId = new bson.ObjectID();
            obj.id = obj.id || objId.toString();
            obj.obmSettings = obj.obmSettings || [];
            obj.bootSettings = obj.bootSettings || {};
            obj.snmpSettings = obj.snmpSettings || {};
            return next();
        }
        return next();
    }
}
