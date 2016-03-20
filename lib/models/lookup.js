// Copyright 2015, EMC, Inc.

'use strict';

module.exports = LookupModelFactory;

LookupModelFactory.$provide = 'Models.Lookup';
LookupModelFactory.$inject = [
    'Services.Waterline',
    'Model',
    'Assert',
    'Errors',
    'Promise',
    'Constants',
    'Services.Configuration',
    'validator',
    '_'
];

function LookupModelFactory (waterline, Model, assert, Errors, Promise, Constants, configuration, validator, _) {
    var defaultDbType = configuration.get('databaseType', 'mongo');
    var dbTypes = configuration.get('databaseOverrideTypes', {});
    var dbType = dbTypes.lookups || defaultDbType;
    return Model.extend({
        connection: dbType,
        identity: 'lookups',
        attributes: {
            node: {
                model: 'nodes',
                index: true
            },
            ipAddress: {
                type: 'string',
                unique: true,
                regex: Constants.Regex.IpAddress
            },
            macAddress: {
                type: 'string',
                unique: true,
                required: true,
                regex: Constants.Regex.MacAddress,
                primaryKey: true
            }
        },

        findByTerm: function (term) {
            var query = {};
            if(validator.isIP(term)) {
                query.ipAddress = term;
            } else if( validator.isMongoId(term)) {
                query.node = term;
            } else if(term) {
                query.macAddress = _.map(_.isArray(term) ? term : [ term ], 
                    function(term) {
                        return term.toLowerCase();
                    });
            }
            return waterline.lookups.find(query)
            .then(function(rows) {
                return rows;
            });
        },

        findOneByTerm: function (term) {
            return this.findByTerm(term).then(function (records) {
                if (records && records.length > 0) {
                    return records[0];
                } else {
                    throw new Errors.NotFoundError('Lookup Record Not Found (findOneByTerm)');
                }
            });
        },

        upsertNodeToMacAddress: function (node, macAddress) {
            var self = this;
            assert.string(node, 'node');
            assert.string(macAddress, 'macAddress');

            var query = { macAddress: macAddress };
            var options = {
                new: true,
                upsert: true
            };

            if(dbType === 'mongo') {
                return self.findAndModifyMongo(query, {}, { $set: { node: node }}, options);
            } else if (dbType === 'cassandra' ) {
                return self.create({macAddress: macAddress, node: node});
            }
        },

        setIp: function(ipAddress, macAddress) {
            if( dbType === 'mongo' ) {
                return this.setIpMongo(ipAddress, macAddress);
            } else if( dbType === 'cassandra' ) {
                return this.setIpCassandra(ipAddress, macAddress);
            }
        },

        setIpMongo: function(ipAddress, macAddress) {
            var query = {
                ipAddress: ipAddress,
                macAddress: { $ne: macAddress } // old mac
            };

            var update = {
                $unset: {
                    ipAddress: ""
                }
            };

            var options = { new: true };

            //Queries for the ipAddress that are not matched with the macAddress
            //changes ip to null if macAddress doesn't match
            return waterline.lookups.findAndModifyMongo(query, {}, update, options)
                .then(function () {
                    // update new document for new IP assignment, do this second
                    query = {
                        macAddress: macAddress // new mac
                    };

                    update = {
                        $set: {
                            ipAddress: ipAddress
                        },
                        $setOnInsert: {
                            macAddress: macAddress
                        }
                    };

                    options = {
                        upsert: true,
                        new: true
                    };

                    return waterline.lookups.findAndModifyMongo(query, {}, update, options);
                });
        },

        setIpCassandra: function(ipAddress, macAddress) {
            var self = this;
            return this.findByTerm(ipAddress)
            .map(function(item) {
                // Clear the ipAddress but only if it is still the ipAddress we want to clear
                // when we go to clear it.  We cannot batch it because the IF crosses partitions
                return self.runCassandraQuery(
                    'UPDATE lookups SET ipaddress = ? WHERE macAddress = ? IF ipaddress = ?',
                    ['', item.macAddress, item.ipAddress]);
            })
            .then(function() {
                // insert the new values
                return self.create({macAddress: macAddress, ipAddress: ipAddress});
            });
        },

        setIndexes: function() {
            var indexes = [
                {
                    macAddress: 1
                },
                {
                    macAddress: 1, ipAddress: 1
                }
            ];
            if( dbType === 'mongo' ) {
                return waterline.lookups.createUniqueMongoIndexes(indexes);
            }
            return Promise.resolve();
        }
    });
}


