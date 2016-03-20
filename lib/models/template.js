// Copyright 2015, EMC, Inc.

'use strict';

module.exports = TemplateModelFactory;

TemplateModelFactory.$provide = 'Models.Template';
TemplateModelFactory.$inject = [
    'Model',
    'Renderable',
    '_',
    'Services.Configuration'
];

function TemplateModelFactory (Model, Renderable, _, configuration) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.templates || defaultDbType;
    var templateModel = _.merge(
        {},
        Renderable,
        { identity: 'templates', connection: dbType }
    );
    return Model.extend(templateModel);
}
