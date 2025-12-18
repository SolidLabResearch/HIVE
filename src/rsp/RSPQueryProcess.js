"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
exports.__esModule = true;
exports.RSPQueryProcess = void 0;
var rsp_js_1 = require("rsp-js");
var mqtt = require('mqtt');
var DataFactory = require('n3').DataFactory;
var uuid_1 = require("uuid");
var rsp_js_2 = require("rsp-js");
var Util_1 = require("../util/Util");
var RSPQueryProcess = /** @class */ (function () {
    function RSPQueryProcess(query, rstream_topic) {
        this.query = query;
        this.rstream_topic = rstream_topic;
        this.rsp_engine = new rsp_js_1.RSPEngine(query);
        this.rstream_emitter = this.rsp_engine.register();
        this.rspql_parser = new rsp_js_2.RSPQLParser();
        this.subscribeToResultStream();
    }
    RSPQueryProcess.prototype.stream_process = function () {
        return __awaiter(this, void 0, void 0, function () {
            var parsed_query, streams, _loop_1, this_1, _i, streams_1, stream;
            var _this = this;
            return __generator(this, function (_a) {
                console.log("Processing query in RSPQueryProcess: ".concat(this.query));
                if (!this.query || this.query.trim() === "") {
                    console.error("Query is empty or undefined.");
                    return [2 /*return*/];
                }
                parsed_query = this.rspql_parser.parse(this.query);
                if (parsed_query) {
                    streams = __spreadArray([], parsed_query.s2r, true);
                    console.log("Parsed query successfully. Found ".concat(streams.length, " streams."));
                    console.log("The streams are: ".concat(JSON.stringify(streams)));
                    _loop_1 = function (stream) {
                        var stream_name = stream.stream_name;
                        var stream_url = new URL(stream_name);
                        var mqtt_broker = "".concat(stream_url.protocol, "//").concat(stream_url.hostname, ":").concat(stream_url.port, "/");
                        var rsp_client = mqtt.connect(mqtt_broker);
                        var rsp_stream_object = this_1.rsp_engine.getStream(stream_name);
                        var topic = stream_url.pathname.slice(1);
                        console.log("Connecting to MQTT broker at ".concat(mqtt_broker, " for stream ").concat(stream_name));
                        rsp_client.on("connect", function () {
                            console.log("Connected to MQTT broker");
                            rsp_client.subscribe(topic, function (err) {
                                if (err) {
                                    console.error("Failed to subscribe to stream ".concat(stream_name, ":"), err);
                                }
                                else {
                                    console.log("Subscribed to stream ".concat(stream_name));
                                }
                            });
                        });
                        rsp_client.on("message", function (topic, message) { return __awaiter(_this, void 0, void 0, function () {
                            var message_string, latest_event_store, timestamp, timestamp_epoch, error_1;
                            var _a;
                            return __generator(this, function (_b) {
                                switch (_b.label) {
                                    case 0:
                                        if (!message || message.length === 0) {
                                            console.error("Received empty message on topic ".concat(topic));
                                            return [2 /*return*/];
                                        }
                                        _b.label = 1;
                                    case 1:
                                        _b.trys.push([1, 6, , 7]);
                                        message_string = message.toString();
                                        return [4 /*yield*/, (0, Util_1.turtleStringToStore)(message_string)];
                                    case 2:
                                        latest_event_store = _b.sent();
                                        timestamp = (_a = latest_event_store.getQuads(null, DataFactory.namedNode('https://saref.etsi.org/core/hasTimestamp'), null, null)[0]) === null || _a === void 0 ? void 0 : _a.object.value;
                                        if (!timestamp) {
                                            console.error("No timestamp found in the message for stream ".concat(stream_name));
                                            return [2 /*return*/];
                                        }
                                        timestamp_epoch = Date.parse(timestamp);
                                        if (!rsp_stream_object) return [3 /*break*/, 4];
                                        return [4 /*yield*/, this.add_event_store_to_rsp_engine(latest_event_store, [rsp_stream_object], timestamp_epoch)];
                                    case 3:
                                        _b.sent();
                                        return [3 /*break*/, 5];
                                    case 4:
                                        console.error("Stream object not found for stream ".concat(stream_name));
                                        return [2 /*return*/];
                                    case 5: return [3 /*break*/, 7];
                                    case 6:
                                        error_1 = _b.sent();
                                        console.error("Error processing message for stream ".concat(stream_name, ":"), error_1);
                                        return [2 /*return*/];
                                    case 7: return [2 /*return*/];
                                }
                            });
                        }); });
                        rsp_client.on("error", function (err) {
                            console.error("Error in MQTT client for stream ".concat(stream_name, ":"), err);
                        });
                        rsp_client.on("close", function () {
                            console.log("Connection closed for stream ".concat(stream_name));
                        });
                    };
                    this_1 = this;
                    for (_i = 0, streams_1 = streams; _i < streams_1.length; _i++) {
                        stream = streams_1[_i];
                        _loop_1(stream);
                    }
                }
                else {
                    console.log("Failed to parse query: ".concat(this.query));
                }
                return [2 /*return*/];
            });
        });
    };
    RSPQueryProcess.prototype.subscribeToResultStream = function () {
        return __awaiter(this, void 0, void 0, function () {
            var mqtt_broker, rstream_publisher;
            var _this = this;
            return __generator(this, function (_a) {
                console.log("Subscribing to result stream: ".concat(this.rstream_topic));
                if (!this.rstream_topic || this.rstream_topic.trim() === "") {
                    console.error("RStream topic is empty or undefined.");
                    return [2 /*return*/];
                }
                mqtt_broker = "mqtt://localhost:1883";
                rstream_publisher = mqtt.connect(mqtt_broker);
                this.rstream_emitter.on("RStream", function (object) { return __awaiter(_this, void 0, void 0, function () {
                    var iterables, iterables_1, iterables_1_1, iterable, event_timestamp, data, aggregation_event, aggregation_object_string, e_1_1;
                    var e_1, _a;
                    return __generator(this, function (_b) {
                        switch (_b.label) {
                            case 0:
                                console.log("Received RStream object: ".concat(JSON.stringify(object)));
                                if (!object || !object.bindings) {
                                    console.log("No bindings found in the RStream object.");
                                    return [2 /*return*/];
                                }
                                iterables = object.bindings.values();
                                _b.label = 1;
                            case 1:
                                _b.trys.push([1, 6, 7, 12]);
                                iterables_1 = __asyncValues(iterables);
                                _b.label = 2;
                            case 2: return [4 /*yield*/, iterables_1.next()];
                            case 3:
                                if (!(iterables_1_1 = _b.sent(), !iterables_1_1.done)) return [3 /*break*/, 5];
                                iterable = iterables_1_1.value;
                                event_timestamp = new Date().getTime();
                                data = iterable.value;
                                console.log("Processing data: ".concat(data, " at timestamp: ").concat(event_timestamp));
                                aggregation_event = this.generate_aggregation_event(data, event_timestamp);
                                aggregation_object_string = JSON.stringify(aggregation_event);
                                rstream_publisher.publish(this.rstream_topic, aggregation_object_string, function (err) {
                                    if (err) {
                                        console.error("Error publishing aggregation event: ".concat(err));
                                    }
                                    else {
                                        console.log("Successfully published aggregation event: ".concat(aggregation_object_string));
                                    }
                                });
                                console.log("Published aggregation event: ".concat(aggregation_object_string));
                                _b.label = 4;
                            case 4: return [3 /*break*/, 2];
                            case 5: return [3 /*break*/, 12];
                            case 6:
                                e_1_1 = _b.sent();
                                e_1 = { error: e_1_1 };
                                return [3 /*break*/, 12];
                            case 7:
                                _b.trys.push([7, , 10, 11]);
                                if (!(iterables_1_1 && !iterables_1_1.done && (_a = iterables_1["return"]))) return [3 /*break*/, 9];
                                return [4 /*yield*/, _a.call(iterables_1)];
                            case 8:
                                _b.sent();
                                _b.label = 9;
                            case 9: return [3 /*break*/, 11];
                            case 10:
                                if (e_1) throw e_1.error;
                                return [7 /*endfinally*/];
                            case 11: return [7 /*endfinally*/];
                            case 12: return [2 /*return*/];
                        }
                    });
                }); });
                return [2 /*return*/];
            });
        });
    };
    RSPQueryProcess.prototype.generate_aggregation_event = function (data, timestamp) {
        var uuid_random = (0, uuid_1.v4)();
        var aggregation_event = "\n    <https://rsp.js/aggregation_event/".concat(uuid_random, "> <https://saref.etsi.org/core/hasValue> \"").concat(data, "\"^^<http://www.w3.org/2001/XMLSchema#float> .\n    ");
        return aggregation_event.trim();
    };
    RSPQueryProcess.prototype.add_event_store_to_rsp_engine = function (event_store, stream_name, timestamp) {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                stream_name.forEach(function (stream) { return __awaiter(_this, void 0, void 0, function () {
                    var quads, _i, quads_1, quad, quadWithGraph;
                    return __generator(this, function (_a) {
                        quads = event_store.getQuads(null, null, null, null);
                        for (_i = 0, quads_1 = quads; _i < quads_1.length; _i++) {
                            quad = quads_1[_i];
                            quadWithGraph = DataFactory.quad(quad.subject, quad.predicate, quad.object, DataFactory.namedNode(stream_name));
                            stream.add(quadWithGraph, timestamp);
                        }
                        return [2 /*return*/];
                    });
                }); });
                return [2 /*return*/];
            });
        });
    };
    return RSPQueryProcess;
}());
exports.RSPQueryProcess = RSPQueryProcess;
