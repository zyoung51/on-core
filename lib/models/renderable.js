// Copyright 2015, EMC, Inc.

'use strict';

module.exports = RenderableModelFactory;

RenderableModelFactory.$provide = 'Renderable';
RenderableModelFactory.$inject = [];

function RenderableModelFactory () {
    return {
        connection: 'mongo',
        autoPK: true,
        attributes: {
            name: {
                type: 'string',
                required: true,
                index: true
            },
            hash: {
                type: 'string',
                required: true
            },
            path: {
                type: 'string',
                required: true
            },
            scope: {
                type: 'string',
                defaultsTo: 'global'
            }
        }
    };
}
