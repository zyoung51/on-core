// Copyright 2016, EMC, Inc.

'use strict';

module.exports = RolesModelFactory;

RolesModelFactory.$provide = 'Models.Roles';
RolesModelFactory.$inject = [
    'Model',
    'Services.Configuration'
];

function RolesModelFactory (Model, configuration) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.profiles || defaultDbType;
    return Model.extend({
        connection: dbType,
        identity: 'roles',
        attributes: {
            role: {
                type: 'string',
                required: true,
                primaryKey: true
            },
            privileges: {
                type: 'array',
                defaultsTo: []
            }
        }
    });
}
