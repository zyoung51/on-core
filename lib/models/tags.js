// Copyright 2016, EMC, Inc.

'use strict';

module.exports = TagModelFactory;

TagModelFactory.$provide = 'Models.Tag';
TagModelFactory.$inject = [
    'Model',
    '_',
    'Assert',
    'Validatable',
    'anchor',
    'Services.Configuration'
];

function TagModelFactory (Model, _, assert, Validatable, Anchor, configuration) {
    var allRules = _.keys(new Anchor().rules);
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.tags || defaultDbType;

    return Model.extend({
        types: {
            tagRules: function(rules) {
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
        identity: 'tags',
        autoPK: true,
        attributes: {
            name: {
                type: 'string',
                required: true,
                index: true
            },
            rules: {
                type: 'json',
                tagRules: true,
                required: true
            }
        }
    });
}
