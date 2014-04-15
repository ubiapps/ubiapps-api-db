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
var webinos = {},
    apiParams,
    Service = null,
    path = null,
    fs = null,
    existsSync = null,
    pathSeperator = null,
    wPath = null;

var init = function (rpcHandler, params, webinosConfig, serviceRegister, serviceUnregister) {
    //TODO: validations
    webinos.rpcHandler = rpcHandler;
    webinos.config = webinosConfig;
    webinos.service = {
        register: serviceRegister,
        unregister: serviceUnregister
    };
    apiParams = params;
};

var normalizeConfig = function (config) {
    if (!config.server){
        config.server = {
            engine: "tingodb"
        }
    }
    return config;
};
var getDbEngine = function (config) {
    if (config.server.engine == "tingodb"){
        if (fs == null) {
            path = require("path");
            fs = require("fs");
            existsSync = fs.existsSync || path.existsSync;
            pathSeperator = process.platform !== 'win32' ? '/' : '\\';
        }
        if (!config.server.path){
            if (wPath == null) wPath = require("webinos-utilities").webinosPath.webinosPath();
            var dbStorePath = path.join(wPath, "userData", "webinos-api-db", "dbs");
        } else {
            var dbStorePath = path.resolve(config.server.path);
        }
        var dbPath = path.join(dbStorePath, config.db);
        if (!existsSync(dbPath)) mkdirSyncRecursive(dbPath, "0744");
        //TODO: check paths for exploits!!!!
        return require('./db.engine.js')({
            engine:'tingodb',
            path: dbPath
        });
    }
};

/*
 * based on: https://github.com/bpedro/node-fs/
 */
function mkdirSyncRecursive(folderPath, permitions,  position) {
    if (fs == null) {
        path = require("path");
        fs = require("fs");
        existsSync = fs.existsSync || path.existsSync;
        pathSeperator = process.platform !== 'win32' ? '/' : '\\';
    }
    var parts = path.resolve(folderPath).split(pathSeperator);

    position = position || 0;

    if (position >= parts.length) {
        return true;
    }

    var directory = parts.slice(0, position + 1).join(pathSeperator) || pathSeperator;
    if (!existsSync(directory)) {
        try {
            fs.mkdirSync(directory, permitions);
        } catch (e) {
            return false; // could be permission error
        }
    }
    mkdirSyncRecursive(folderPath, permitions, position + 1);
}

var Database = function(config, expose) {
    var self = this;
    this.config = normalizeConfig(config);
    this.engine = getDbEngine(this.config);
    this._state = 'disconnected';
    this._name = this.config.db;
    this.service = null;
    this.serviceId = null;
    this.isExposed = !!expose;
    var isOpen = false;
    if (this.isExposed){
        if (Service == null) Service = require("./service.js");
        this.service = new Service(webinos.rpcHandler, self);
        this.serviceId = webinos.service.register(this.service);
    }
    this.changeConfig = function (config) {
        //TODO: check if the config has changed, update the internal one,
        // if the serviceID has changed, unregister the old service and register new one.
    };

    this.open = function(callback) {
        self._state = 'connecting';
        self.engine.db.open(function(err, db) {
            if (err == null) {
                self._state = 'connected';
                isOpen = true;
            }
            callback(err, db);
        });
    };
    this.collectionNames = function (collectionName, options, callback) {
        var args = Array.prototype.slice.call(arguments, 0);
        callback = args.pop();
        collectionName = args.length ? args.shift() : null;
        options = args.length ? args.shift() || {} : {};
        if(collectionName != null && typeof collectionName == 'object') {
            options = collectionName;
            collectionName = null;
        }
        if (self.config.server.engine == "tingodb"){
            self.engine.db.collectionNames(options, callback);
        } else {
            self.engine.db.collectionNames(collectionName, options, callback);
        }
    };
    var collections = {};
    this.collection = function (name, callback) {
        if (!isOpen) return null;
        if (!collections[name]) collections[name] = {collection:self.engine.db.collection(name)};
        var timestamp = (new Date()).getTime();
        collections[name].lastAccessed = timestamp;
        setTimeout(self.cleanupCollections, 1); //do not block execution for cleanup
        return callback == null ? collections[name].collection : callback(null, collections[name].collection);
    };
    this.cleanupCollections = function () {
        var timestamp = (new Date()).getTime();
        for (var collection in collections){
            //if the collection was last used 5 minutes ago, remove it to free memory.
            if (collections[collection].lastAccessed < (timestamp - 5*60*1000)){
                delete collections[collection];
            }
        }
    };

    this.createCollection = function (collectionName, callback) {
        if (!isOpen) return null;
        return self.engine.db.createCollection(collectionName, callback);
    };

    this.dropCollection = function (collectionName, callback) {
        if (!isOpen) return null;
        return self.engine.db.dropCollection(collectionName, callback);
    };

    this.renameCollection = function (fromCollection, toCollection, options, callback) {
        if (!isOpen) return null;
        if(typeof options == 'function') {
            callback = options;
            options = {};
        }
        return self.engine.db.renameCollection(fromCollection, toCollection, options, callback);
    };
};

module.exports = {
    init: init,
    Database: Database
};