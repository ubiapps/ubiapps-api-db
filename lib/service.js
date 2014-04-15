/*******************************************************************************
 *  Code contributed to the webinos project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2013 EPU-National Technical University of Athens
 * Author: Christos Botsikas, NTUA
 ******************************************************************************/

var RPCWebinosService = require("webinos-jsonrpc2").RPCWebinosService;
var logger = require("webinos-utilities").webinosLogging(__filename);

var db = require('./db').Database;

var _rpcHandler = null;

var Service = function (rpcHandler, db) {
    this.base = RPCWebinosService;
    this.base({
        api: 'http://webinos.org/api/db',
        displayName: 'DB API (' + db._name + ')',
        description: 'webinos Database api running on ' + db.config.server.engine
    });
    this.db = db;
    _rpcHandler = rpcHandler;
    this.getName = function () {
        return db._name;
    }
};

Service.prototype = new RPCWebinosService;

Service.prototype.dbOpen = function (params, successCB, errorCB, objectRef) {
    var self = this;
    self.db.open(function (err, db) {
        if (err == null) {
            successCB(self.getName());
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.dbCollectionNames = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    // Since this is the service call, we always need just the collection names.
    // The client side will decide to return names or objects based on the requested options.
    var options = {
        namesOnly: false
    };
    var self = this;
    self.db.collectionNames(params.collectionName, options, function (err, collections) {
        if (err == null) {
            successCB(collections);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.dbCollection = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    var self = this;
    self.db.collection(params.collectionName, function (err, collection) {
        if (err == null) {
            successCB(collection._name);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.dbCreateCollection = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    var self = this;
    self.db.createCollection(params.collectionName, function (err, collection) {
        if (err == null) {
            successCB(collection._name);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.dbDropCollection = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    var self = this;
    self.db.dropCollection(params.collectionName, function (err, result) {
        if (err == null) {
            successCB(result); // in this case, result should be always true. I'm just passing it forward, just in case :)
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.dbRenameCollection = function (params, successCB, errorCB, objectRef) {
    if (typeof params.fromCollection === "undefined") {
        errorCB("No fromCollection provided");
        return;
    }
    if (typeof params.toCollection === "undefined") {
        errorCB("No toCollection provided");
        return;
    }
    if (typeof params.options === "undefined") {
        params.options = {};
    }
    var self = this;
    self.db.dropCollection(params.fromCollection, params.toCollection, params.options, function (err, collection) {
        if (err == null) {
            successCB(collection._name);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.collectionInsert = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    if (typeof params.docs === "undefined") {
        errorCB("No docs provided");
        return;
    }
    var self = this;
    var collection = self.db.collection(params.collectionName);
    collection.insert(params.docs, {w: 1}, function (err, docs) {
        if (err == null) {
            successCB(docs);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.collectionRemove = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    if (typeof params.selector === "undefined") {
        params.selector = null;
    }
    var self = this;
    var collection = self.db.collection(params.collectionName);
    collection.remove(params.selector, {w: 1}, function (err, numberOfRemovedDocs) {
        if (err == null) {
            successCB(numberOfRemovedDocs);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.collectionUpdate = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    if (typeof params.selector === "undefined") {
        errorCB("No selector provided");
        return;
    }
    if (typeof params.document === "undefined") {
        errorCB("No document provided");
        return;
    }
    if (typeof params.options === "undefined") {
        params.options = {};
    }
    // always use write concern because we want the callback!
    params.options.w = 1;
    var self = this;
    var collection = self.db.collection(params.collectionName);
    collection.update(params.selector, params.document, params.options, function (err, numberOfUpdatedDocs) {
        if (err == null) {
            successCB(numberOfUpdatedDocs);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.collectionDistinct = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    if (typeof params.key === "undefined") {
        errorCB("No key provided");
        return;
    }
    if (typeof params.query === "undefined") {
        params.query = {};
    }
    var self = this;
    var collection = self.db.collection(params.collectionName);
    collection.distinct(params.key, params.query, function (err, docs) {
        if (err == null) {
            successCB(docs);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.collectionCount = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    if (typeof params.query === "undefined") {
        params.query = {};
    }
    var self = this;
    var collection = self.db.collection(params.collectionName);
    collection.count(params.query, function (err, numberOfDocs) {
        if (err == null) {
            successCB(numberOfDocs);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.collectionFind = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    if (typeof params.selector === "undefined") {
        errorCB("No selector provided");
        return;
    }
    if (typeof params.options === "undefined") {
        params.options = {};
    }
    if (typeof params.fields !== "undefined") {
        params.options.fields = params.fields;
    }
    if (typeof params.skip !== "undefined") {
        params.options.skip = params.skip;
    }
    if (typeof params.limit !== "undefined") {
        params.options.limit = params.limit;
    }
    if (typeof params.timeout !== "undefined") {
        params.options.timeout = params.timeout;
    }

    var self = this;
    var collection = self.db.collection(params.collectionName);
    collection.find(params.selector, params.options).toArray(function (err, docs) {
        if (err == null) {
            successCB(docs);
        } else {
            errorCB(err.message);
        }
    });
};
Service.prototype.collectionFindOne = function (params, successCB, errorCB, objectRef) {
    if (typeof params.collectionName === "undefined") {
        errorCB("No collectionName provided");
        return;
    }
    if (typeof params.selector === "undefined") {
        errorCB("No selector provided");
        return;
    }
    if (typeof params.options === "undefined") {
        params.options = {};
    }
    if (typeof params.fields !== "undefined") {
        params.options.fields = params.fields;
    }
    if (typeof params.skip !== "undefined") {
        params.options.skip = params.skip;
    }
    if (typeof params.limit !== "undefined") {
        params.options.limit = params.limit;
    }
    if (typeof params.timeout !== "undefined") {
        params.options.timeout = params.timeout;
    }

    var self = this;
    var collection = self.db.collection(params.collectionName);
    collection.findOne(params.selector, params.options, function (err, doc) {
        if (err == null) {
            successCB(doc);
        } else {
            errorCB(err.message);
        }
    });
};

module.exports = Service;
