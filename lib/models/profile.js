// Copyright 2015, EMC, Inc.

'use strict';

module.exports = ProfileModelFactory;

ProfileModelFactory.$provide = 'Models.Profile';
ProfileModelFactory.$inject = [
    'Model',
    'Renderable',
    '_',
    'Services.Configuration'
];

function ProfileModelFactory (Model, Renderable, _, configuration) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.profiles || defaultDbType;
    var profileModel = _.merge(
        {},
        Renderable,
        {identity: 'profiles', connection: dbType }
    );
    return Model.extend(profileModel);
}
