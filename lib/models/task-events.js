// Copyright 2016, EMC, Inc.
'use strict';

module.exports = EventModelFactory;

EventModelFactory.$provide = 'Models.TaskEvents';
EventModelFactory.$inject = [
    'Model',
    'Services.Configuration'
];

function EventModelFactory(Model, configuration) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.taskevents || defaultDbType;
    return Model.extend({
        connection: dbType,
        identity: 'taskevents'
    });
}
