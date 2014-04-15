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

var db = require('./db');

var Module = function (rpcHandler, params, webinosConfig) {
    this.rpcHandler = rpcHandler;
    this.params = params;
    this.webinosConfig = webinosConfig;
    this.internalRegistry = {};
    this.serviceUnregister = null;
};

Module.prototype.init = function (serviceRegister, serviceUnregister) {
    this.serviceUnregister = serviceUnregister;
    db.init(this.rpcHandler, this.params, this.webinosConfig, serviceRegister, serviceUnregister);
    // Do not invoke service foreach params.instances, updateServiceParams will be executed..
};

Module.prototype.updateServiceParams = function (serviceId, params){
    console.log(serviceId, params);
    // if serviceId == null, it's new
    // else the service params changed
    // invoke service
    //
    // this.params.instances[serviceId] = params; //not possible!! It's a feature, not a bug :)
    // Instead do something like this:
    //  foreach instance in this.params.instances
    //      if the instance.id == the serviceID update the params!

    //TODO: update this when the service configuration can specify it a service was added, changed or removed.
//    var self = this;
//    if (serviceId && self.internalRegistry[serviceId]) {
//        var oldDb = self.internalRegistry[serviceId];
//        oldDb.changeConfig(params);
//        if (serviceId != oldDb.serviceId){
//            delete self.internalRegistry[serviceId];
//            serviceId = oldDb.serviceId;
//            self.internalRegistry[serviceId] = oldDb;
//        }
//    }else{
//        var newDb = new db.Database(params, true);
//        serviceId = newDb.serviceId;
//        self.internalRegistry[serviceId] = newDb;
//    }
    var self = this;
    if (serviceId && self.internalRegistry[serviceId]) {
        var oldDb = self.internalRegistry[serviceId];
        self.serviceUnregister({"id": oldDb.serviceId, "api": oldDb.service.api});
        delete self.internalRegistry[serviceId];
    }
    if (params) {
        var newDb = new db.Database(params, true);
        serviceId = newDb.serviceId;
        self.internalRegistry[serviceId] = newDb;
    }

    return serviceId;
};

module.exports = Module;