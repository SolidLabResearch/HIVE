"use strict";
exports.__esModule = true;
exports.computeWindowIfAbsent = exports.CSPARQLWindow = exports.QuadContainer = exports.WindowInstance = exports.Tick = exports.ReportStrategy = void 0;
var events_1 = require("events");
var Logger_1 = require("../../util/logger/Logger");
var LoggerEnum_1 = require("../../util/logger/LoggerEnum");
var LOG_CONFIG = require("../../config/log_config.json");
var ReportStrategy;
(function (ReportStrategy) {
    ReportStrategy[ReportStrategy["NonEmptyContent"] = 0] = "NonEmptyContent";
    ReportStrategy[ReportStrategy["OnContentChange"] = 1] = "OnContentChange";
    ReportStrategy[ReportStrategy["OnWindowClose"] = 2] = "OnWindowClose";
    ReportStrategy[ReportStrategy["Periodic"] = 3] = "Periodic";
})(ReportStrategy = exports.ReportStrategy || (exports.ReportStrategy = {}));
var Tick;
(function (Tick) {
    Tick[Tick["TimeDriven"] = 0] = "TimeDriven";
    Tick[Tick["TupleDriven"] = 1] = "TupleDriven";
    Tick[Tick["BatchDriven"] = 2] = "BatchDriven";
})(Tick = exports.Tick || (exports.Tick = {}));
/* eslint-enable no-unused-vars */
/**
 * WindowInstance class to represent the window instance of the CSPARQL Window.
 */
var WindowInstance = /** @class */ (function () {
    /**
     * Constructor for the WindowInstance class.
     * @param {number} open - The open time of the window instance of the form {open, close, has_triggered}.
     * @param {number} close - The close time of the window instance of the form {open, close, has_triggered}.
     */
    function WindowInstance(open, close) {
        this.open = open;
        this.close = close;
        this.has_triggered = false;
    }
    /**
     * Get the definition of the window instance.
     * @returns {string} - The definition of the window instance in the form [open, close) Triggered: has_triggered.
     */
    WindowInstance.prototype.getDefinition = function () {
        return "[" + this.open + "," + this.close + ")" + " Triggered: " + this.has_triggered;
    };
    /**
     * Get the code of the window instance.
     * @returns {number} - The code of the window instance.
     */
    WindowInstance.prototype.hasCode = function () {
        return 0;
    };
    /**
     * Check if the window instance is the same as the other window instance.
     * @param {WindowInstance} other_window - The other window instance to be compared.
     * @returns {boolean} - True if the window instances are the same, else false.
     */
    WindowInstance.prototype.is_same = function (other_window) {
        return this.open == other_window.open && this.close == other_window.close;
    };
    /**
     *
     */
    WindowInstance.prototype.set_triggered = function () {
        this.has_triggered = true;
    };
    return WindowInstance;
}());
exports.WindowInstance = WindowInstance;
/**
 * QuadContainer class to represent the container for the quads in the CSPARQL Window.
 */
var QuadContainer = /** @class */ (function () {
    /**
     * Constructor for the QuadContainer class.
     * @param {Set<Quad>} elements - The set of quads in the container.
     * @param {number} ts - The timestamp of the last change in the container.
     */
    function QuadContainer(elements, ts) {
        this.elements = elements;
        this.last_time_stamp_changed = ts;
    }
    /**
     * Get the length of the container of the quads.
     * @returns {number} - The length of the container.
     */
    QuadContainer.prototype.len = function () {
        return this.elements.size;
    };
    /**
     * Add the quad to the container of the quads.
     * @param {Quad} quad - The quad to be added to the container.
     * @param {number} quad_timestamp - The timestamp of the quad.
     * @returns {void} - The function returns nothing.
     */
    QuadContainer.prototype.add = function (quad, quad_timestamp) {
        this.elements.add(quad);
        this.last_time_stamp_changed = quad_timestamp;
    };
    /**
     * Get the last time the container was changed.
     * @returns {number} - The last time the container was changed.
     */
    QuadContainer.prototype.last_time_changed = function () {
        return this.last_time_stamp_changed;
    };
    return QuadContainer;
}());
exports.QuadContainer = QuadContainer;
/**
 * CSPARQL Window class that implements the windowing mechanism for the RSP Engine.
 * The class is responsible for managing the windows, processing the events, and emitting the triggers based on the report strategy.
 * The class also handles the out-of-order processing of the events based on the maximum delay allowed for the events and the watermark.
 */
var CSPARQLWindow = /** @class */ (function () {
    /**
     * Constructor for the CSPARQLWindow class.
     * @param {string} name - The name of the CSPARQL Window.
     * @param {number} width - The width of the window.
     * @param {number} slide - The slide of the window.
     * @param {ReportStrategy} report - The report strategy for the window.
     * @param {Tick} tick - The tick of the window.
     * @param {number} start_time - The start time of the window.
     * @param {number} max_delay - The maximum delay allowed for an observation to be considered in the window used for out-of-order processing.
     */
    function CSPARQLWindow(name, width, slide, report, tick, start_time, max_delay) {
        this.name = name;
        this.width = width;
        this.slide = slide;
        this.report = report;
        this.tick = tick;
        var log_level = LoggerEnum_1.LogLevel[LOG_CONFIG.log_level];
        this.logger = new Logger_1.Logger(log_level, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination);
        this.time = start_time;
        this.current_watermark = start_time;
        this.watermarkStallWarningCount = 0;
        this.t0 = start_time;
        this.active_windows = new Map();
        this.emitter = new events_1.EventEmitter();
        this.max_delay = max_delay;
        this.pending_triggers = new Set();
    }
    /**
     * Get the content of the window at the given timestamp if it exists, else return undefined.
     * @param {number} timestamp - The timestamp for which the content of the window is to be retrieved.
     * @returns {QuadContainer | undefined} - The content of the window if it exists, else undefined.
     */
    CSPARQLWindow.prototype.getContent = function (timestamp) {
        var max_window = null;
        var max_time = Number.MAX_SAFE_INTEGER;
        this.active_windows.forEach(function (value, window) {
            if (window.open <= timestamp && timestamp <= window.close) {
                if (window.close < max_time) {
                    max_time = window.close;
                    max_window = window;
                }
            }
        });
        if (max_window) {
            return this.active_windows.get(max_window);
        }
        else {
            return undefined;
        }
    };
    /**
     * Add the event to the window at the given timestamp and checks if the event is late or not.
     * @param {Quad} event - The event to be added to the window.
     * @param {number} timestamp - The timestamp of the event.
     * @returns {void} - The function does not return anything.
     */
    /**
     *
     * @param event
     * @param timestamp
     */
    CSPARQLWindow.prototype.add = function (event, timestamp) {
        this.logger.info("adding_event_to_the_window", "CSPARQLWindow");
        console.debug("Adding [\" + ".concat(event, " + \"] at time : ").concat(timestamp, " and watermark ").concat(this.current_watermark));
        var t_e = timestamp;
        var to_evict = new Set();
        if (this.time > timestamp) {
            this.logger.info("out_of_order_event_received", "CSPARQLWindow");
            var event_latency = this.time - timestamp;
            this.logger.info("Event Latency : ".concat(event_latency), "CSPARQLWindow");
            // Out of order event handling
            console.error("The event is late and has arrived out of order at time ".concat(timestamp));
            if (t_e - this.time > this.max_delay) {
                this.logger.info("out_of_order_event_out_of_delay", "CSPARQLWindow");
                // Discard the event if it is too late to be considered in the window based on a simple static heuristic pre-decided
                // when the CSPARQL Window was initialized.
                console.error("Late element [" + event + "] with timestamp [" + timestamp + "] is out of the allowed delay [" + this.max_delay + "]");
            }
            else if (t_e - this.time <= this.max_delay) {
                this.logger.info("out_of_order_event_within_delay", "CSPARQLWindow");
                // The event is late but within the allowed delay, so we will add it to the specific window instance.
                for (var _i = 0, _a = this.active_windows.keys(); _i < _a.length; _i++) {
                    var w = _a[_i];
                    if (w.open <= t_e && t_e < w.close) {
                        var temp_window = this.active_windows.get(w);
                        if (temp_window) {
                            // TODO: log this for when the event is added to the window and for the latency calculation
                            this.logger.info("adding_out_of_order_event ".concat(event.subject.value, " to the window ").concat(this.name, " with bounds ").concat(w.getDefinition(), " at time ").concat(timestamp), "CSPARQLWindow");
                            temp_window.add(event, t_e);
                        }
                    }
                    else if (t_e >= w.close) {
                        to_evict.add(w);
                    }
                }
            }
            this.time = timestamp;
        }
        else if (timestamp >= this.time) {
            this.time = timestamp;
            this.logger.info("in_order_event_received", "CSPARQLWindow");
            // In order event handling
            this.scope(t_e);
            for (var _b = 0, _c = this.active_windows.keys(); _b < _c.length; _b++) {
                var w = _c[_b];
                console.debug("Processing Window ".concat(w.getDefinition(), " for the event ").concat(event, " at time ").concat(timestamp));
                if (w.open <= t_e && t_e < w.close) {
                    console.debug("Adding the event ".concat(event, " to the window ").concat(w.getDefinition(), " at time ").concat(timestamp));
                    var window_to_add = this.active_windows.get(w);
                    if (window_to_add) {
                        this.logger.info("adding_in_order_event ".concat(event.subject.value, " to the window ").concat(this.name, " with bounds ").concat(w.getDefinition(), " at time ").concat(timestamp), "CSPARQLWindow");
                        window_to_add.add(event, t_e);
                    }
                }
                else if (t_e >= w.close + this.max_delay && !w.has_triggered) {
                    console.debug("Scheduled to evict the window ".concat(w.getDefinition(), " at time ").concat(timestamp));
                    to_evict.add(w);
                }
            }
            this.update_watermark(t_e);
            this.trigger_window_content(this.current_watermark, timestamp);
        }
    };
    /**
     *
     * @param timestamp
     */
    CSPARQLWindow.prototype.if_event_late = function (timestamp) {
        return this.time > timestamp;
    };
    /**
     * Trigger the window content based on the current watermark.
     * @param {number} watermark - The current watermark which needs to be processed.
     * @returns {void} - The function does not return anything.
     */
    /**
     *
     * @param watermark
     * @param timestamp
     */
    CSPARQLWindow.prototype.trigger_window_content = function (watermark, timestamp) {
        var _this = this;
        var max_window = null;
        var max_time = 0;
        // Identify the window to trigger
        this.active_windows.forEach(function (value, window) {
            if (_this.compute_report(window, value, watermark)) {
                if (window.close > max_time) {
                    max_time = window.close;
                    max_window = window;
                }
            }
        });
        if (max_window) {
            if (this.tick == Tick.TimeDriven && watermark >= max_time) {
                setTimeout(function () {
                    if (max_window && max_window.has_triggered === false) {
                        if (watermark >= max_time + _this.max_delay) {
                            _this.logger.info("Watermark ".concat(watermark, " "), "CSPARQLWindow");
                            if (max_window) { }
                            var windowToDelete = _this.findWindowInstance(max_window);
                            if (windowToDelete) {
                                _this.emitter.emit('RStream', _this.active_windows.get(windowToDelete));
                                _this.logger.info("Window with bounds [".concat(windowToDelete.open, ",").concat(windowToDelete.close, ") ").concat(windowToDelete.getDefinition(), " is triggered for the window name ").concat(_this.name), "CSPARQLWindow");
                                max_window.set_triggered();
                                _this.active_windows["delete"](windowToDelete);
                            }
                            _this.time = timestamp;
                        }
                        else {
                            _this.logger.info("Window will not trigger.", "CSPARQLWindow");
                        }
                    }
                }, this.max_delay);
            }
            else {
                this.logger.info("Window ".concat(max_window, " is out of the watermark and will not trigger."), "CSPARQLWindow");
                console.error("Window is out of the watermark and will not trigger");
            }
        }
    };
    // Helper to find the matching instance in the Map
    /**
     *
     * @param target
     */
    CSPARQLWindow.prototype.findWindowInstance = function (target) {
        for (var _i = 0, _a = this.active_windows.keys(); _i < _a.length; _i++) {
            var window_1 = _a[_i];
            if (window_1.is_same(target)) {
                return window_1;
            }
        }
        return undefined;
    };
    /**
     * Updating the watermark.
     * @param {number} new_time - The new watermark to be set.
     * @returns {void} - The function does not return anything.
     */
    CSPARQLWindow.prototype.update_watermark = function (new_time) {
        if (new_time > this.current_watermark) {
            this.current_watermark = new_time;
            this.logger.info("Watermark is increasing ".concat(this.current_watermark, " and time ").concat(this.time), "CSPARQLWindow");
        }
        else {
            this.watermarkStallWarningCount += 1;
            if (this.watermarkStallWarningCount === 1 || this.watermarkStallWarningCount % 500 === 0) {
                console.warn("Watermark is not increasing (count=".concat(this.watermarkStallWarningCount, ", current=").concat(this.current_watermark, ", candidate=").concat(new_time, ")"));
            }
        }
    };
    /**
     * Get the current time of the window.
     * @returns {number} - The current time of the window.
     */
    CSPARQLWindow.prototype.get_current_watermark = function () {
        return this.current_watermark;
    };
    /**
     * Compute the report based on the window instance and the content of the window.
     * Max Delay is added to trigger the report computation only after waiting for a certain time.
     * @param {WindowInstance} w - The window instance for which the report is to be computed.
     * @param {QuadContainer} content - The content of the window (which is a QuadContainer).
     * @param {number} timestamp - The timestamp of the event to be processed.
     * @returns {boolean} - True if the report is to be computed, else false.
     */
    CSPARQLWindow.prototype.compute_report = function (w, content, timestamp) {
        if (this.report == ReportStrategy.OnWindowClose) {
            return w.close < timestamp;
        }
        else if (this.report == ReportStrategy.OnContentChange) {
            return true;
        }
        return false;
    };
    /**
     * Scope the window based on the given timestamp.
     * @param {number} t_e - The timestamp of the event to be processed.
     * @returns {void} - The function does not return anything.
     */
    CSPARQLWindow.prototype.scope = function (t_e) {
        var c_sup = Math.ceil((Math.abs(t_e - this.t0) / this.slide)) * this.slide;
        var o_i = c_sup - this.width;
        console.log("Scope the window for the event at time ".concat(t_e));
        console.log("".concat(c_sup, " - ").concat(this.width, " = ").concat(o_i));
        while (o_i <= t_e) {
            computeWindowIfAbsent(this.active_windows, new WindowInstance(o_i, o_i + this.width), function () { return new QuadContainer(new Set(), 0); });
            o_i += this.slide;
        }
    };
    /* eslint-disable no-unused-vars */
    /**
     * Subscribe to the window based on the output stream and the callback function.
     * @param {'RStream' | 'IStream' | 'DStream'} output - The output stream to which the window is to be subscribed. The output stream can be one of {'RStream', 'IStream', 'DStream'}.
     * @param {(QuadContainer) => void} call_back - The callback function to be called when the window emits the triggers.
     * @returns {void} - The function does not return anything.
     */
    CSPARQLWindow.prototype.subscribe = function (output, call_back) {
        this.emitter.on(output, call_back);
    };
    /* eslint-enable no-unused-vars */
    /**
     * Set the current time to the given value.
     * @param {number} t - The time to be set.
     * @returns {void} - The function does not return anything.
     */
    CSPARQLWindow.prototype.set_current_time = function (t) {
        this.time = t;
    };
    /**
     *
     * @param delay
     */
    CSPARQLWindow.prototype.set_max_delay = function (delay) {
        this.max_delay = delay;
    };
    /**
     * Set the watermark to the given value.
     * @param {number} t - The watermark to be set.
     * @returns {void} - The function does not return anything.
     */
    CSPARQLWindow.prototype.set_current_watermark = function (t) {
        this.current_watermark = t;
    };
    /**
     * Get a string representation of the CSPARQLWindow definition.
     * The function is used to get the definition of the CSPARQLWindow in a string format.
     * @returns {string} - The string representation of the CSPARQLWindow definition.
     */
    CSPARQLWindow.prototype.getCSPARQLWindowDefinition = function () {
        var windowDefinitions = [];
        for (var _i = 0, _a = this.active_windows.entries(); _i < _a.length; _i++) {
            var window_2 = _a[_i][0];
            windowDefinitions.push(window_2.getDefinition());
        }
        return "CSPARQLWindow {\n        name: ".concat(this.name, ",\n        width: ").concat(this.width, ",\n        slide: ").concat(this.slide, ",\n        current_time: ").concat(this.time, ",\n        current_watermark: ").concat(this.current_watermark, ",\n        report_strategy: ").concat(ReportStrategy[this.report], ",\n        tick: ").concat(Tick[this.tick], ",\n        active_windows: [").concat(windowDefinitions.join(", "), "]\n    }");
    };
    return CSPARQLWindow;
}());
exports.CSPARQLWindow = CSPARQLWindow;
/* eslint-disable no-unused-vars */
/**
 * Compute the window if absent based on the given window instance and the mapping function.
 * @param {Map<WindowInstance, QuadContainer>} map - The map of the active windows.
 * @param {WindowInstance} window - The window instance of the form {open, close, has_triggered}.
 * @param {mappingFunction} mappingFunction - The mapping function to be applied to the window instance.
 */
function computeWindowIfAbsent(map, window, mappingFunction) {
    var found = false;
    for (var _i = 0, _a = map.keys(); _i < _a.length; _i++) {
        var w = _a[_i];
        if (w.is_same(window)) {
            found = true;
            break;
        }
    }
    if (!found) {
        map.set(window, mappingFunction(window));
    }
    return found;
}
exports.computeWindowIfAbsent = computeWindowIfAbsent;
/* eslint-enable no-unused-vars */ 
