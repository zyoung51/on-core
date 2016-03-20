// Copyright 2015, EMC, Inc.
'use strict';

module.exports = storeFactory;
storeFactory.$provide = 'TaskGraph.Store';
storeFactory.$inject = [
    'Services.Configuration',
    '$injector'
];

function storeFactory(configuration, injector) {
    var mode = configuration.get('taskgraph-store', 'mongo');
    switch(mode) {
        case 'mongo':
            return injector.get('TaskGraph.Stores.Mongo');
        case 'cassandra':
            return injector.get('TaskGraph.Stores.Cassandra');
        default:
            throw new Error('Unknown taskgraph store: ' + mode);
    }
}
