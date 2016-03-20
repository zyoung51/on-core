// Copyright 2016, EMC, Inc.

'use strict';

module.exports = EnvModelFactory;

EnvModelFactory.$provide = 'Models.Environment';
EnvModelFactory.$inject = [
    'Model',
    'Services.Configuration'
];

function EnvModelFactory (Model, configuration) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.environment || defaultDbType;
    return Model.extend({
        connection: dbType,
        identity: 'environment',
        attributes: {
            identifier: {
                type: 'string',
                required: true,
                primaryKey: true
            },
            data: {
                type: 'json',
                required: true,
                json: true
            }
        }
    });
}
