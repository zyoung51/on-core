// Copyright 2015, EMC, Inc.

'use strict';

module.exports = SerializableFactory;

SerializableFactory.$provide = 'Serializable';
SerializableFactory.$inject = [
    'Promise',
    'Util',
    'Validatable',
    '_'
];

function SerializableFactory (Promise, util, Validatable, _) {
    function Serializable(schema, defaults) {
        Validatable.call(this, schema);

        this.defaults(defaults);
    }

    util.inherits(Serializable, Validatable);

    Serializable.register = function (factory, constructor) {
        // Provide inheritance in registration to cut down on
        // boilerplate registrations and includes of util.
        util.inherits(constructor, Serializable);

        // Determine the constructor provides annotation to
        // make sure the constructor is usable later in
        // dynamic object creation.
        constructor.provides = util.provides(factory);

        // Register the constructor schema with the Validatable
        // object so they can be used as references to one
        // another.
        Validatable.register(constructor);
    };

    Serializable.prototype.defaults = function (target) {
        _.defaults(this, target);
    };

    Serializable.prototype.serialize = function (target) {
        target = target || this;
        return Promise.resolve(target);
    };

    Serializable.prototype.deserialize = function (target) {
        this.defaults(target);
        return Promise.resolve(this);
    };

    return Serializable;
}

