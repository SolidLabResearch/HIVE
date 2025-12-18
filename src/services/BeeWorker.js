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
exports.__esModule = true;
exports.BeeWorker = void 0;
var hive_thought_rewriter_1 = require("hive-thought-rewriter");
var rspql_containment_checker_1 = require("rspql-containment-checker");
var Util_1 = require("../util/Util");
var StreamingQueryChunkAggregatorOperator_1 = require("./operators/StreamingQueryChunkAggregatorOperator");
var BeeWorker = /** @class */ (function () {
    function BeeWorker() {
        this.containmentChecker = new rspql_containment_checker_1.ContainmentChecker();
        this.queryCombiner = new hive_thought_rewriter_1.QueryCombiner();
        this.queryFetchLocation = "http://localhost:8080/fetchQueries";
        var query = process.env.QUERY;
        var r2s_topic = process.env.TOPIC;
        if (!query || !r2s_topic) {
            throw new Error("Missing required environment variables");
            process.exit(1);
        }
        this.query = query;
        this.r2s_topic = r2s_topic;
        this.streamingQueryChunkAggregatorOperator = new StreamingQueryChunkAggregatorOperator_1.StreamingQueryChunkAggregatorOperator(this.query);
        console.log("BeeWorker initialized with query: ".concat(this.query, " and topic: ").concat(this.r2s_topic));
        this.process();
    }
    BeeWorker.prototype.process = function () {
        return __awaiter(this, void 0, void 0, function () {
            var existingQueries, extractedQueries, containedQueries, _i, containedQueries_1, containedQuery, combinedQuery, isValid, _a, containedQueries_2, containedQuery;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        console.log("proces() method is called");
                        return [4 /*yield*/, this.fetchExistingQueries(this.queryFetchLocation)];
                    case 1:
                        existingQueries = _b.sent();
                        return [4 /*yield*/, this.extractQueriesWithTopics(JSON.parse(existingQueries))];
                    case 2:
                        extractedQueries = _b.sent();
                        return [4 /*yield*/, this.findContainedQueries(extractedQueries)];
                    case 3:
                        containedQueries = _b.sent();
                        console.log("Found ".concat(containedQueries.length, " contained queries."));
                        if (!(containedQueries.length > 0)) return [3 /*break*/, 8];
                        for (_i = 0, containedQueries_1 = containedQueries; _i < containedQueries_1.length; _i++) {
                            containedQuery = containedQueries_1[_i];
                            this.queryCombiner.addQuery(containedQuery);
                        }
                        this.queryCombiner.combine();
                        console.log("Combined query: ".concat(this.queryCombiner.ParsedToString(this.queryCombiner.combine())));
                        combinedQuery = this.queryCombiner.ParsedToString(this.queryCombiner.combine());
                        return [4 /*yield*/, this.validateQueryContainment(this.query, combinedQuery)];
                    case 4:
                        isValid = _b.sent();
                        if (!isValid) return [3 /*break*/, 6];
                        for (_a = 0, containedQueries_2 = containedQueries; _a < containedQueries_2.length; _a++) {
                            containedQuery = containedQueries_2[_a];
                            this.streamingQueryChunkAggregatorOperator.addSubQuery(containedQuery);
                        }
                        return [4 /*yield*/, this.streamingQueryChunkAggregatorOperator.init()];
                    case 5:
                        _b.sent();
                        this.streamingQueryChunkAggregatorOperator.handleAggregation();
                        return [3 /*break*/, 7];
                    case 6:
                        console.log("The subqueries are contained but however the combined query cannot be utilized to make the original registered query.");
                        _b.label = 7;
                    case 7: return [3 /*break*/, 9];
                    case 8:
                        console.log("There is no containment relationship between the query registered in the Bee and the existing queries run by the RSP Agents.");
                        _b.label = 9;
                    case 9: return [2 /*return*/];
                }
            });
        });
    };
    BeeWorker.prototype.findContainedQueries = function (extractedQueries) {
        return __awaiter(this, void 0, void 0, function () {
            var containedQueries, _i, extractedQueries_1, extractedQuery, queryY, queryZ, isContainedX, isContainedY, isContainedZ, isContained;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        containedQueries = [];
                        _i = 0, extractedQueries_1 = extractedQueries;
                        _a.label = 1;
                    case 1:
                        if (!(_i < extractedQueries_1.length)) return [3 /*break*/, 6];
                        extractedQuery = extractedQueries_1[_i];
                        this.query = this.removeAggregationFunctions(this.query);
                        extractedQuery.rspql_query = this.removeAggregationFunctions(extractedQuery.rspql_query);
                        queryY = (0, Util_1.generateQuery)(['y', 'x', 'z']);
                        queryZ = (0, Util_1.generateQuery)(['z', 'y', 'x']);
                        return [4 /*yield*/, this.containmentChecker.checkContainment(this.query, extractedQuery.rspql_query)];
                    case 2:
                        isContainedX = _a.sent();
                        return [4 /*yield*/, this.containmentChecker.checkContainment(queryY, extractedQuery.rspql_query)];
                    case 3:
                        isContainedY = _a.sent();
                        return [4 /*yield*/, this.containmentChecker.checkContainment(queryZ, extractedQuery.rspql_query)];
                    case 4:
                        isContainedZ = _a.sent();
                        isContained = isContainedX || isContainedY || isContainedZ;
                        if (isContained) {
                            console.log("Query \"".concat(extractedQuery.rspql_query, "\" is contained in the main query."));
                            containedQueries.push(extractedQuery.rspql_query);
                        }
                        else {
                            console.log("Query \"".concat(extractedQuery.rspql_query, "\" is not contained in the main query."));
                        }
                        _a.label = 5;
                    case 5:
                        _i++;
                        return [3 /*break*/, 1];
                    case 6: return [2 /*return*/, containedQueries];
                }
            });
        });
    };
    BeeWorker.prototype.fetchExistingQueries = function (location) {
        return __awaiter(this, void 0, void 0, function () {
            var response, queries;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!location) {
                            throw new Error("Location for fetching queries is not specified");
                        }
                        return [4 /*yield*/, fetch(location, {
                                'method': 'GET'
                            })];
                    case 1:
                        response = _a.sent();
                        return [4 /*yield*/, response.text()];
                    case 2:
                        queries = _a.sent();
                        if (!queries) {
                            throw new Error("No queries found at the specified location");
                        }
                        return [2 /*return*/, queries];
                }
            });
        });
    };
    BeeWorker.prototype.extractQueriesWithTopics = function (data) {
        return __awaiter(this, void 0, void 0, function () {
            var extractedQueries, key, entry;
            return __generator(this, function (_a) {
                extractedQueries = [];
                for (key in data) {
                    if (data.hasOwnProperty(key)) {
                        entry = data[key];
                        if (entry.rspql_query && entry.r2s_topic) {
                            extractedQueries.push({
                                rspql_query: entry.rspql_query,
                                r2s_topic: entry.r2s_topic
                            });
                        }
                    }
                }
                return [2 /*return*/, extractedQueries];
            });
        });
    };
    BeeWorker.prototype.validateQueryContainment = function (queryOne, queryTwo) {
        return __awaiter(this, void 0, void 0, function () {
            var isComplete, isSound;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        // Completeness Check for query containment
                        queryOne = this.removeAggregationFunctions(queryOne);
                        queryTwo = this.removeAggregationFunctions(queryTwo);
                        return [4 /*yield*/, this.containmentChecker.checkContainment(queryOne, queryTwo)];
                    case 1:
                        isComplete = _a.sent();
                        return [4 /*yield*/, this.containmentChecker.checkContainment(queryTwo, queryOne)];
                    case 2:
                        isSound = _a.sent();
                        if (isComplete && isSound) {
                            console.log("Query \"".concat(queryOne, "\" is contained in \"").concat(queryTwo, "\" and vice versa."));
                            return [2 /*return*/, true];
                        }
                        else if (isComplete) {
                            console.log("There is a completeness relationship between \"".concat(queryOne, "\" and \"").concat(queryTwo, "\""));
                            return [2 /*return*/, false];
                        }
                        else if (isSound) {
                            console.log("There is a soundness relationship between \"".concat(queryOne, "\" and \"").concat(queryTwo, "\""));
                            return [2 /*return*/, false];
                        }
                        else {
                            console.log("Query \"".concat(queryOne, "\" is not contained in \"").concat(queryTwo, "\" and vice versa."));
                            return [2 /*return*/, true];
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    BeeWorker.prototype.removeAggregationFunctions = function (query) {
        // This regex will match any aggregation function like AVG(?x) AS ?alias
        // Replace any function like AVG(?x) AS ?alias with just ?x
        return query.replace(/\(\s*\w+\s*\(\s*\?(\w+)\s*\)\s+AS\s+\?\w+\s*\)/g, '?$1');
    };
    BeeWorker.prototype.stop = function () {
    };
    return BeeWorker;
}());
exports.BeeWorker = BeeWorker;
var beeWorker = new BeeWorker();
process.on("SIGINT", function () {
    beeWorker.stop();
    process.exit(0);
});
process.on("SIGTERM", function () {
    beeWorker.stop();
    process.exit(0);
});
