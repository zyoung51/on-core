// Copyright 2015, EMC, Inc.

'use strict';

module.exports = SkuModelFactory;

SkuModelFactory.$provide = 'Models.Sku';
SkuModelFactory.$inject = [
    'Model',
    '_',
    'Assert',
    'Validatable',
    'anchor',
    'Services.Configuration'
];

function SkuModelFactory (Model, _, assert, Validatable, Anchor, configuration) {
    var allRules = _.keys(new Anchor().rules);
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.skus || defaultDbType;

    return Model.extend({
        types: {
            skuRules: function(rules) {
                assert.arrayOfObject(rules, 'rules');
                _.forEach(rules, function (rule) {
                    assert.string(rule.path, 'rule.path');
                    _(rule).omit('path').keys().forEach(function (key) {
                        assert.isIn(key, allRules, 'rule.' + key);
                    }).value();
                });
                return true;
            },
        },
        connection: dbType,
        identity: 'skus',
        autoPK: true,
        attributes: {
            name: {
                type: 'string',
                required: true,
                index: true
            },
            rules: {
                type: 'json',
                skuRules: true,
                required: true
            },
            nodes: {
                collection: 'nodes',
                via: 'sku'
            },
            discoveryGraphName: {
                type: 'string'
            },
            discoveryGraphOptions: {
                type: 'json'
            },
            httpStaticRoot : {
                type: 'string'
            },
            httpTemplateRoot : {
                type: 'string'
            },
            httpProfileRoot : {
                type: 'string'
            },
            workflowRoot : {
                type: 'string'
            },
            taskRoot : {
                type: 'string'
            },
            skuConfig: {
                type: 'json'
            },
            version : {
                type: 'string'
            },
            description : {
                type: 'string'
            }
        }
    });
}
