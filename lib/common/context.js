// Copyright (c) 2015, EMC Corporation

'use strict';

module.exports = ContextFactory;

ContextFactory.$provide = 'Context';
ContextFactory.$inject = [
    'uuid',
    '_'
];

function ContextFactory (uuid, _) {
    function Context (id) {
        this.id = id || uuid.v4();
        this._domains = [];
    }

    Context.prototype.get = function (key, value) {
        return this[key] || value;
    };

    Context.prototype.set = function (key, value) {
        this[key] = value;
        return this;
    };

    Context.prototype.push = function (key, value) {
        if (!_.isArray(this[key])) {
            this[key] = [];
        }

        this[key].push(value);

        return this;
    };

    Context.prototype.pop = function (key) {
        if (_.isArray(this[key])) {
            return this[key].pop();
        } else {
            return undefined;
        }
    };

    Context.prototype.clone = function () {
        return _.omit(_.cloneDeep(this), 'id', '_domains');
    };

    Context.prototype.add = function (domain) {
        this._domains.push(domain);
    };

    Context.prototype.dispose = function () {
        _.each(this._domains, function (domain) {
            domain.dispose();
        });
    };

    return Context;
}
