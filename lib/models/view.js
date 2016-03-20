// Copyright 2015, EMC, Inc.

'use strict';

module.exports = ViewModelFactory;

ViewModelFactory.$provide = 'Models.View';
ViewModelFactory.$inject = [
    'Model',
    'Renderable',
    '_',
    'Services.Configuration'
];

function ViewModelFactory (Model, Renderable, _, configuration) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.profiles || defaultDbType;
    var viewModel = _.merge(
        {},
        Renderable,
        {identity: 'views', connection: dbType }
    );
    return Model.extend(viewModel);
}
