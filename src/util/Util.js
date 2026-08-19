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
exports.generateQuery = exports.hash_string_md5 = exports.stringToStore = exports.storeToString = exports.turtleStringToStore = exports.lcm = void 0;
var n3_1 = require("n3");
var crypto_1 = require("crypto");
var rdfParser = require("rdf-parse")["default"];
var storeStream = require("rdf-store-stream").storeStream;
var streamifyString = require('streamify-string');
/**
 *
 * @param a
 * @param b
 */
function lcm(a, b) {
    // Calculate the least common multiple (LCM) of two numbers
    // using the formula: LCM(a, b) = |a * b| / GCD(a, b)
    // where GCD is the greatest common divisor.
    // If either a or b is 0, return 0
    return (!a || !b) ? 0 : Math.abs(a * b) / gcd(a, b);
}
exports.lcm = lcm;
/**
 *
 * @param a
 * @param b
 */
function gcd(a, b) {
    return b === 0 ? a : gcd(b, a % b);
}
/**
 *
 * @param text
 * @param baseIRI
 */
function turtleStringToStore(text, baseIRI) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, stringToStore(text, { contentType: 'text/turtle', baseIRI: baseIRI })];
                case 1: return [2 /*return*/, _a.sent()];
            }
        });
    });
}
exports.turtleStringToStore = turtleStringToStore;
/**
 *
 * @param store
 */
function storeToString(store) {
    var writer = new n3_1.Writer();
    return writer.quadsToString(store.getQuads(null, null, null, null));
}
exports.storeToString = storeToString;
/**
 *
 * @param text
 * @param options
 */
function stringToStore(text, options) {
    return __awaiter(this, void 0, void 0, function () {
        var textStream, quadStream;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    textStream = streamifyString(text);
                    quadStream = rdfParser.parse(textStream, options);
                    return [4 /*yield*/, storeStream(quadStream)];
                case 1: return [2 /*return*/, _a.sent()];
            }
        });
    });
}
exports.stringToStore = stringToStore;
/**
 *
 * @param input_string
 */
function hash_string_md5(input_string) {
    input_string = input_string.replace(/\s/g, '');
    var hash = (0, crypto_1.createHash)('md5');
    hash.update(input_string);
    return hash.digest('hex');
}
exports.hash_string_md5 = hash_string_md5;
function generateQuery(order) {
    if (order === void 0) { order = ['z', 'y', 'x']; }
    var windowMappings = {
        x: {
            window: ':w1',
            stream: 'mqtt_broker:accX',
            variable: '?o',
            property: 'dahccsensors:x',
            subject: '?s'
        },
        y: {
            window: ':w2',
            stream: 'mqtt_broker:accY',
            variable: '?o2',
            property: 'dahccsensors:y',
            subject: '?s2'
        },
        z: {
            window: ':w3',
            stream: 'mqtt_broker:accZ',
            variable: '?o3',
            property: 'dahccsensors:z',
            subject: '?s3'
        }
    };
    var windowClauses = order.map(function (axis) {
        var _a = windowMappings[axis], window = _a.window, variable = _a.variable, property = _a.property, subject = _a.subject;
        return "\n    { WINDOW ".concat(window, " {\n        ").concat(subject, " saref:hasValue ").concat(variable, " .\n        ").concat(subject, " saref:relatesToProperty ").concat(property, " .\n    }}");
    }).join(' UNION');
    var query = "\nPREFIX mqtt_broker: <mqtt://localhost:1883/>\nPREFIX saref: <https://saref.etsi.org/core/>\nPREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>\nPREFIX : <https://rsp.js> \n\nREGISTER RStream <output> AS\nSELECT ?o ?o2 ?o3\nFROM NAMED WINDOW :w1 ON STREAM mqtt_broker:accX [RANGE 120000 STEP 30000]\nFROM NAMED WINDOW :w2 ON STREAM mqtt_broker:accY [RANGE 120000 STEP 30000]\nFROM NAMED WINDOW :w3 ON STREAM mqtt_broker:accZ [RANGE 120000 STEP 30000]\nWHERE {\n    ".concat(windowClauses, "\n}\n");
    return query;
}
exports.generateQuery = generateQuery;
