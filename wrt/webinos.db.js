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

(function () {
    var Service = function (obj, rpcHandler) {
        WebinosService.call(this, obj);
        this.rpcHandler = rpcHandler;
    };
	// Inherit all functions from WebinosService
	Service.prototype = Object.create(WebinosService.prototype);	
	// The following allows the 'instanceof' to work properly
	Service.prototype.constructor = Service;
	// Register to the service discovery
    _webinos.registerServiceConstructor("http://webinos.org/api/db", Service);

    Service.prototype.open = function (successCallback, errorCallback) {
        var self = this;
        var rpc = self.rpcHandler.createRPC(self, "dbOpen");
        self.rpcHandler.executeRPC(rpc
            , function (dbName) {
                successCallback(new Database(self, dbName));
            }
            , errorCallback
        );
    };

    var Database = function (service, name) {
        this.service = service;
        this.name = name;

        this.rpcHandler = service.rpcHandler;
    };

    /**
     * collectionNames([collectionName], [options], [successCallback, [errorCallback]])
     * @param collectionName
     * @param options
     * @param successCallback
     * @param errorCallback
     */
    Database.prototype.collectionNames = function (collectionName, options, successCallback, errorCallback) {
        if (typeof collectionName !== "string" && collectionName != null) {
            errorCallback = successCallback;
            successCallback = options;
            options = collectionName;
            collectionName = null;
        }
        if (typeof options !== "object") {
            errorCallback = successCallback;
            successCallback = options;
            options = {};
        }
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "dbCollectionNames", {collectionName: collectionName, options: options});
        self.rpcHandler.executeRPC(rpc
            , function (collections) {
                if (options.namesOnly) {
                    successCallback(collections);
                } else {
                    successCallback(collections.map(function (collectionName) {
                        return new Collection(self, collectionName);
                    }));
                }
            }
            , errorCallback);
    };
    Database.prototype.collection = function (collectionName, successCallback, errorCallback) {
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "dbCollection", {collectionName: collectionName});
        self.rpcHandler.executeRPC(rpc
            , function (collectionName) {
                successCallback(new Collection(self, collectionName));
            }
            , errorCallback
        );
    };
    Database.prototype.createCollection = function (collectionName, successCallback, errorCallback) {
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "dbCreateCollection", {collectionName: collectionName});
        self.rpcHandler.executeRPC(rpc
            , function (collectionName) {
                successCallback(new Collection(self, collectionName));
            }
            , errorCallback
        );
    };

    Database.prototype.dropCollection = function (collectionName, successCallback, errorCallback) {
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "dbDropCollection", {collectionName: collectionName});
        self.rpcHandler.executeRPC(rpc
            , successCallback
            , errorCallback
        );
    };

    Database.prototype.renameCollection = function (fromCollection, toCollection, options, successCallback, errorCallback) {
        if (typeof options == 'function') {
            errorCallback = successCallback;
            successCallback = options;
            options = {};
        }
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "dbRenameCollection", {fromCollection: fromCollection, toCollection: toCollection, options: options});
        self.rpcHandler.executeRPC(rpc
            , function (collectionName) {
                successCallback(new Collection(self, collectionName));
            }
            , errorCallback
        );
    };

    var Collection = function (database, name) {
        this.database = database;
        this.name = name;

        this.service = database.service;
        this.rpcHandler = database.rpcHandler;
    };

    Collection.prototype.insert = function (docs, successCallback, errorCallback) {
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "collectionInsert", {collectionName: self.name, docs: docs});
        self.rpcHandler.executeRPC(rpc, successCallback, errorCallback);
    };

    Collection.prototype.remove = function (selector, successCallback, errorCallback) {
        if (typeof selector === "function"){
            errorCallback = successCallback;
            successCallback  = selector;
            selector = null;
        }
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "collectionRemove", {collectionName: self.name, selector: selector});
        self.rpcHandler.executeRPC(rpc, successCallback, errorCallback);
    };

    Collection.prototype.update = function (selector, document, options, successCallback, errorCallback) {
        if (typeof options === "function"){
            errorCallback = successCallback;
            successCallback  = options;
            options = {};
        }
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "collectionUpdate", {collectionName: self.name, selector: selector, document: document, options: options});
        self.rpcHandler.executeRPC(rpc, successCallback, errorCallback);
    };

    Collection.prototype.distinct = function (key, query, successCallback, errorCallback) {
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "collectionDistinct", {collectionName: self.name, key: key, query: query});
        self.rpcHandler.executeRPC(rpc, successCallback, errorCallback);
    };

    Collection.prototype.count = function (query, successCallback, errorCallback) {
        if (typeof query === "function"){
            errorCallback = successCallback;
            successCallback  = query;
            query = {};
        }
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "collectionCount", {collectionName: self.name, query: query});
        self.rpcHandler.executeRPC(rpc, successCallback, errorCallback);
    };

    Collection.prototype.find = function (selector, options, successCallback, errorCallback) {
        if (typeof options === "function"){
            errorCallback = successCallback;
            successCallback  = options;
            options = {};
        }
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "collectionFind", {collectionName: self.name, selector: selector, options: options});
        self.rpcHandler.executeRPC(rpc, successCallback, errorCallback);
    };

    Collection.prototype.findOne = function (selector, options, successCallback, errorCallback) {
        if (typeof options === "function"){
            errorCallback = successCallback;
            successCallback  = options;
            options = {};
        }
        var self = this;
        var rpc = self.rpcHandler.createRPC(self.service, "collectionFindOne", {collectionName: self.name, selector: selector, options: options});
        self.rpcHandler.executeRPC(rpc, successCallback, errorCallback);
    };

})();
