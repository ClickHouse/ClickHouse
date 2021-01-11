/*! @sentry/browser 5.15.5 (2accf045) | https://github.com/getsentry/sentry-javascript */
var Sentry = (function (exports) {
    /*! *****************************************************************************
    Copyright (c) Microsoft Corporation. All rights reserved.
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use
    this file except in compliance with the License. You may obtain a copy of the
    License at http://www.apache.org/licenses/LICENSE-2.0

    THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
    WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
    MERCHANTABLITY OR NON-INFRINGEMENT.

    See the Apache Version 2.0 License for specific language governing permissions
    and limitations under the License.
    ***************************************************************************** */
    /* global Reflect, Promise */

    var extendStatics = function(d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };

    function __extends(d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    }

    var __assign = function() {
        __assign = Object.assign || function __assign(t) {
            for (var s, i = 1, n = arguments.length; i < n; i++) {
                s = arguments[i];
                for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p)) t[p] = s[p];
            }
            return t;
        };
        return __assign.apply(this, arguments);
    };

    function __rest(s, e) {
        var t = {};
        for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
            t[p] = s[p];
        if (s != null && typeof Object.getOwnPropertySymbols === "function")
            for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) if (e.indexOf(p[i]) < 0)
                t[p[i]] = s[p[i]];
        return t;
    }

    function __decorate(decorators, target, key, desc) {
        var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
        if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
        else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
        return c > 3 && r && Object.defineProperty(target, key, r), r;
    }

    function __param(paramIndex, decorator) {
        return function (target, key) { decorator(target, key, paramIndex); }
    }

    function __metadata(metadataKey, metadataValue) {
        if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(metadataKey, metadataValue);
    }

    function __awaiter(thisArg, _arguments, P, generator) {
        return new (P || (P = Promise))(function (resolve, reject) {
            function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
            function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
            function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
            step((generator = generator.apply(thisArg, _arguments || [])).next());
        });
    }

    function __generator(thisArg, body) {
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
    }

    function __exportStar(m, exports) {
        for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
    }

    function __values(o) {
        var m = typeof Symbol === "function" && o[Symbol.iterator], i = 0;
        if (m) return m.call(o);
        return {
            next: function () {
                if (o && i >= o.length) o = void 0;
                return { value: o && o[i++], done: !o };
            }
        };
    }

    function __read(o, n) {
        var m = typeof Symbol === "function" && o[Symbol.iterator];
        if (!m) return o;
        var i = m.call(o), r, ar = [], e;
        try {
            while ((n === void 0 || n-- > 0) && !(r = i.next()).done) ar.push(r.value);
        }
        catch (error) { e = { error: error }; }
        finally {
            try {
                if (r && !r.done && (m = i["return"])) m.call(i);
            }
            finally { if (e) throw e.error; }
        }
        return ar;
    }

    function __spread() {
        for (var ar = [], i = 0; i < arguments.length; i++)
            ar = ar.concat(__read(arguments[i]));
        return ar;
    }

    function __await(v) {
        return this instanceof __await ? (this.v = v, this) : new __await(v);
    }

    function __asyncGenerator(thisArg, _arguments, generator) {
        if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
        var g = generator.apply(thisArg, _arguments || []), i, q = [];
        return i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i;
        function verb(n) { if (g[n]) i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; }
        function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
        function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
        function fulfill(value) { resume("next", value); }
        function reject(value) { resume("throw", value); }
        function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
    }

    function __asyncDelegator(o) {
        var i, p;
        return i = {}, verb("next"), verb("throw", function (e) { throw e; }), verb("return"), i[Symbol.iterator] = function () { return this; }, i;
        function verb(n, f) { i[n] = o[n] ? function (v) { return (p = !p) ? { value: __await(o[n](v)), done: n === "return" } : f ? f(v) : v; } : f; }
    }

    function __asyncValues(o) {
        if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
        var m = o[Symbol.asyncIterator], i;
        return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
        function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
        function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
    }

    function __makeTemplateObject(cooked, raw) {
        if (Object.defineProperty) { Object.defineProperty(cooked, "raw", { value: raw }); } else { cooked.raw = raw; }
        return cooked;
    }
    function __importStar(mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
        result.default = mod;
        return result;
    }

    function __importDefault(mod) {
        return (mod && mod.__esModule) ? mod : { default: mod };
    }

    var tslib_1 = /*#__PURE__*/Object.freeze({
        __extends: __extends,
        get __assign () { return __assign; },
        __rest: __rest,
        __decorate: __decorate,
        __param: __param,
        __metadata: __metadata,
        __awaiter: __awaiter,
        __generator: __generator,
        __exportStar: __exportStar,
        __values: __values,
        __read: __read,
        __spread: __spread,
        __await: __await,
        __asyncGenerator: __asyncGenerator,
        __asyncDelegator: __asyncDelegator,
        __asyncValues: __asyncValues,
        __makeTemplateObject: __makeTemplateObject,
        __importStar: __importStar,
        __importDefault: __importDefault
    });

    /** Console logging verbosity for the SDK. */
    var LogLevel;
    (function (LogLevel) {
        /** No logs will be generated. */
        LogLevel[LogLevel["None"] = 0] = "None";
        /** Only SDK internal errors will be logged. */
        LogLevel[LogLevel["Error"] = 1] = "Error";
        /** Information useful for debugging the SDK will be logged. */
        LogLevel[LogLevel["Debug"] = 2] = "Debug";
        /** All SDK actions will be logged. */
        LogLevel[LogLevel["Verbose"] = 3] = "Verbose";
    })(LogLevel || (LogLevel = {}));

    /** JSDoc */
    (function (Severity) {
        /** JSDoc */
        Severity["Fatal"] = "fatal";
        /** JSDoc */
        Severity["Error"] = "error";
        /** JSDoc */
        Severity["Warning"] = "warning";
        /** JSDoc */
        Severity["Log"] = "log";
        /** JSDoc */
        Severity["Info"] = "info";
        /** JSDoc */
        Severity["Debug"] = "debug";
        /** JSDoc */
        Severity["Critical"] = "critical";
    })(exports.Severity || (exports.Severity = {}));
    // tslint:disable:completed-docs
    // tslint:disable:no-unnecessary-qualifier no-namespace
    (function (Severity) {
        /**
         * Converts a string-based level into a {@link Severity}.
         *
         * @param level string representation of Severity
         * @returns Severity
         */
        function fromString(level) {
            switch (level) {
                case 'debug':
                    return Severity.Debug;
                case 'info':
                    return Severity.Info;
                case 'warn':
                case 'warning':
                    return Severity.Warning;
                case 'error':
                    return Severity.Error;
                case 'fatal':
                    return Severity.Fatal;
                case 'critical':
                    return Severity.Critical;
                case 'log':
                default:
                    return Severity.Log;
            }
        }
        Severity.fromString = fromString;
    })(exports.Severity || (exports.Severity = {}));

    /** The status of an Span. */
    var SpanStatus;
    (function (SpanStatus) {
        /** The operation completed successfully. */
        SpanStatus["Ok"] = "ok";
        /** Deadline expired before operation could complete. */
        SpanStatus["DeadlineExceeded"] = "deadline_exceeded";
        /** 401 Unauthorized (actually does mean unauthenticated according to RFC 7235) */
        SpanStatus["Unauthenticated"] = "unauthenticated";
        /** 403 Forbidden */
        SpanStatus["PermissionDenied"] = "permission_denied";
        /** 404 Not Found. Some requested entity (file or directory) was not found. */
        SpanStatus["NotFound"] = "not_found";
        /** 429 Too Many Requests */
        SpanStatus["ResourceExhausted"] = "resource_exhausted";
        /** Client specified an invalid argument. 4xx. */
        SpanStatus["InvalidArgument"] = "invalid_argument";
        /** 501 Not Implemented */
        SpanStatus["Unimplemented"] = "unimplemented";
        /** 503 Service Unavailable */
        SpanStatus["Unavailable"] = "unavailable";
        /** Other/generic 5xx. */
        SpanStatus["InternalError"] = "internal_error";
        /** Unknown. Any non-standard HTTP status code. */
        SpanStatus["UnknownError"] = "unknown_error";
        /** The operation was cancelled (typically by the user). */
        SpanStatus["Cancelled"] = "cancelled";
        /** Already exists (409) */
        SpanStatus["AlreadyExists"] = "already_exists";
        /** Operation was rejected because the system is not in a state required for the operation's */
        SpanStatus["FailedPrecondition"] = "failed_precondition";
        /** The operation was aborted, typically due to a concurrency issue. */
        SpanStatus["Aborted"] = "aborted";
        /** Operation was attempted past the valid range. */
        SpanStatus["OutOfRange"] = "out_of_range";
        /** Unrecoverable data loss or corruption */
        SpanStatus["DataLoss"] = "data_loss";
    })(SpanStatus || (SpanStatus = {}));
    // tslint:disable:no-unnecessary-qualifier no-namespace
    (function (SpanStatus) {
        /**
         * Converts a HTTP status code into a {@link SpanStatus}.
         *
         * @param httpStatus The HTTP response status code.
         * @returns The span status or {@link SpanStatus.UnknownError}.
         */
        // tslint:disable-next-line:completed-docs
        function fromHttpCode(httpStatus) {
            if (httpStatus < 400) {
                return SpanStatus.Ok;
            }
            if (httpStatus >= 400 && httpStatus < 500) {
                switch (httpStatus) {
                    case 401:
                        return SpanStatus.Unauthenticated;
                    case 403:
                        return SpanStatus.PermissionDenied;
                    case 404:
                        return SpanStatus.NotFound;
                    case 409:
                        return SpanStatus.AlreadyExists;
                    case 413:
                        return SpanStatus.FailedPrecondition;
                    case 429:
                        return SpanStatus.ResourceExhausted;
                    default:
                        return SpanStatus.InvalidArgument;
                }
            }
            if (httpStatus >= 500 && httpStatus < 600) {
                switch (httpStatus) {
                    case 501:
                        return SpanStatus.Unimplemented;
                    case 503:
                        return SpanStatus.Unavailable;
                    case 504:
                        return SpanStatus.DeadlineExceeded;
                    default:
                        return SpanStatus.InternalError;
                }
            }
            return SpanStatus.UnknownError;
        }
        SpanStatus.fromHttpCode = fromHttpCode;
    })(SpanStatus || (SpanStatus = {}));

    /** The status of an event. */
    (function (Status) {
        /** The status could not be determined. */
        Status["Unknown"] = "unknown";
        /** The event was skipped due to configuration or callbacks. */
        Status["Skipped"] = "skipped";
        /** The event was sent to Sentry successfully. */
        Status["Success"] = "success";
        /** The client is currently rate limited and will try again later. */
        Status["RateLimit"] = "rate_limit";
        /** The event could not be processed. */
        Status["Invalid"] = "invalid";
        /** A server-side error ocurred during submission. */
        Status["Failed"] = "failed";
    })(exports.Status || (exports.Status = {}));
    // tslint:disable:completed-docs
    // tslint:disable:no-unnecessary-qualifier no-namespace
    (function (Status) {
        /**
         * Converts a HTTP status code into a {@link Status}.
         *
         * @param code The HTTP response status code.
         * @returns The send status or {@link Status.Unknown}.
         */
        function fromHttpCode(code) {
            if (code >= 200 && code < 300) {
                return Status.Success;
            }
            if (code === 429) {
                return Status.RateLimit;
            }
            if (code >= 400 && code < 500) {
                return Status.Invalid;
            }
            if (code >= 500) {
                return Status.Failed;
            }
            return Status.Unknown;
        }
        Status.fromHttpCode = fromHttpCode;
    })(exports.Status || (exports.Status = {}));

    /**
     * Consumes the promise and logs the error when it rejects.
     * @param promise A promise to forget.
     */

    var setPrototypeOf = Object.setPrototypeOf || ({ __proto__: [] } instanceof Array ? setProtoOf : mixinProperties); // tslint:disable-line:no-unbound-method
    /**
     * setPrototypeOf polyfill using __proto__
     */
    function setProtoOf(obj, proto) {
        // @ts-ignore
        obj.__proto__ = proto;
        return obj;
    }
    /**
     * setPrototypeOf polyfill using mixin
     */
    function mixinProperties(obj, proto) {
        for (var prop in proto) {
            if (!obj.hasOwnProperty(prop)) {
                // @ts-ignore
                obj[prop] = proto[prop];
            }
        }
        return obj;
    }

    /** An error emitted by Sentry SDKs and related utilities. */
    var SentryError = /** @class */ (function (_super) {
        __extends(SentryError, _super);
        function SentryError(message) {
            var _newTarget = this.constructor;
            var _this = _super.call(this, message) || this;
            _this.message = message;
            // tslint:disable:no-unsafe-any
            _this.name = _newTarget.prototype.constructor.name;
            setPrototypeOf(_this, _newTarget.prototype);
            return _this;
        }
        return SentryError;
    }(Error));

    /**
     * Checks whether given value's type is one of a few Error or Error-like
     * {@link isError}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isError(wat) {
        switch (Object.prototype.toString.call(wat)) {
            case '[object Error]':
                return true;
            case '[object Exception]':
                return true;
            case '[object DOMException]':
                return true;
            default:
                return isInstanceOf(wat, Error);
        }
    }
    /**
     * Checks whether given value's type is ErrorEvent
     * {@link isErrorEvent}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isErrorEvent(wat) {
        return Object.prototype.toString.call(wat) === '[object ErrorEvent]';
    }
    /**
     * Checks whether given value's type is DOMError
     * {@link isDOMError}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isDOMError(wat) {
        return Object.prototype.toString.call(wat) === '[object DOMError]';
    }
    /**
     * Checks whether given value's type is DOMException
     * {@link isDOMException}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isDOMException(wat) {
        return Object.prototype.toString.call(wat) === '[object DOMException]';
    }
    /**
     * Checks whether given value's type is a string
     * {@link isString}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isString(wat) {
        return Object.prototype.toString.call(wat) === '[object String]';
    }
    /**
     * Checks whether given value's is a primitive (undefined, null, number, boolean, string)
     * {@link isPrimitive}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isPrimitive(wat) {
        return wat === null || (typeof wat !== 'object' && typeof wat !== 'function');
    }
    /**
     * Checks whether given value's type is an object literal
     * {@link isPlainObject}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isPlainObject(wat) {
        return Object.prototype.toString.call(wat) === '[object Object]';
    }
    /**
     * Checks whether given value's type is an Event instance
     * {@link isEvent}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isEvent(wat) {
        // tslint:disable-next-line:strict-type-predicates
        return typeof Event !== 'undefined' && isInstanceOf(wat, Event);
    }
    /**
     * Checks whether given value's type is an Element instance
     * {@link isElement}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isElement(wat) {
        // tslint:disable-next-line:strict-type-predicates
        return typeof Element !== 'undefined' && isInstanceOf(wat, Element);
    }
    /**
     * Checks whether given value's type is an regexp
     * {@link isRegExp}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isRegExp(wat) {
        return Object.prototype.toString.call(wat) === '[object RegExp]';
    }
    /**
     * Checks whether given value has a then function.
     * @param wat A value to be checked.
     */
    function isThenable(wat) {
        // tslint:disable:no-unsafe-any
        return Boolean(wat && wat.then && typeof wat.then === 'function');
        // tslint:enable:no-unsafe-any
    }
    /**
     * Checks whether given value's type is a SyntheticEvent
     * {@link isSyntheticEvent}.
     *
     * @param wat A value to be checked.
     * @returns A boolean representing the result.
     */
    function isSyntheticEvent(wat) {
        // tslint:disable-next-line:no-unsafe-any
        return isPlainObject(wat) && 'nativeEvent' in wat && 'preventDefault' in wat && 'stopPropagation' in wat;
    }
    /**
     * Checks whether given value's type is an instance of provided constructor.
     * {@link isInstanceOf}.
     *
     * @param wat A value to be checked.
     * @param base A constructor to be used in a check.
     * @returns A boolean representing the result.
     */
    function isInstanceOf(wat, base) {
        try {
            // tslint:disable-next-line:no-unsafe-any
            return wat instanceof base;
        }
        catch (_e) {
            return false;
        }
    }

    /**
     * Truncates given string to the maximum characters count
     *
     * @param str An object that contains serializable values
     * @param max Maximum number of characters in truncated string
     * @returns string Encoded
     */
    function truncate(str, max) {
        if (max === void 0) { max = 0; }
        // tslint:disable-next-line:strict-type-predicates
        if (typeof str !== 'string' || max === 0) {
            return str;
        }
        return str.length <= max ? str : str.substr(0, max) + "...";
    }
    /**
     * Join values in array
     * @param input array of values to be joined together
     * @param delimiter string to be placed in-between values
     * @returns Joined values
     */
    function safeJoin(input, delimiter) {
        if (!Array.isArray(input)) {
            return '';
        }
        var output = [];
        // tslint:disable-next-line:prefer-for-of
        for (var i = 0; i < input.length; i++) {
            var value = input[i];
            try {
                output.push(String(value));
            }
            catch (e) {
                output.push('[value cannot be serialized]');
            }
        }
        return output.join(delimiter);
    }
    /**
     * Checks if the value matches a regex or includes the string
     * @param value The string value to be checked against
     * @param pattern Either a regex or a string that must be contained in value
     */
    function isMatchingPattern(value, pattern) {
        if (!isString(value)) {
            return false;
        }
        if (isRegExp(pattern)) {
            return pattern.test(value);
        }
        if (typeof pattern === 'string') {
            return value.indexOf(pattern) !== -1;
        }
        return false;
    }

    /**
     * Requires a module which is protected against bundler minification.
     *
     * @param request The module path to resolve
     */
    function dynamicRequire(mod, request) {
        // tslint:disable-next-line: no-unsafe-any
        return mod.require(request);
    }
    /**
     * Checks whether we're in the Node.js or Browser environment
     *
     * @returns Answer to given question
     */
    function isNodeEnv() {
        // tslint:disable:strict-type-predicates
        return Object.prototype.toString.call(typeof process !== 'undefined' ? process : 0) === '[object process]';
    }
    var fallbackGlobalObject = {};
    /**
     * Safely get global scope object
     *
     * @returns Global scope object
     */
    function getGlobalObject() {
        return (isNodeEnv()
            ? global
            : typeof window !== 'undefined'
                ? window
                : typeof self !== 'undefined'
                    ? self
                    : fallbackGlobalObject);
    }
    /**
     * UUID4 generator
     *
     * @returns string Generated UUID4.
     */
    function uuid4() {
        var global = getGlobalObject();
        var crypto = global.crypto || global.msCrypto;
        if (!(crypto === void 0) && crypto.getRandomValues) {
            // Use window.crypto API if available
            var arr = new Uint16Array(8);
            crypto.getRandomValues(arr);
            // set 4 in byte 7
            // tslint:disable-next-line:no-bitwise
            arr[3] = (arr[3] & 0xfff) | 0x4000;
            // set 2 most significant bits of byte 9 to '10'
            // tslint:disable-next-line:no-bitwise
            arr[4] = (arr[4] & 0x3fff) | 0x8000;
            var pad = function (num) {
                var v = num.toString(16);
                while (v.length < 4) {
                    v = "0" + v;
                }
                return v;
            };
            return (pad(arr[0]) + pad(arr[1]) + pad(arr[2]) + pad(arr[3]) + pad(arr[4]) + pad(arr[5]) + pad(arr[6]) + pad(arr[7]));
        }
        // http://stackoverflow.com/questions/105034/how-to-create-a-guid-uuid-in-javascript/2117523#2117523
        return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            // tslint:disable-next-line:no-bitwise
            var r = (Math.random() * 16) | 0;
            // tslint:disable-next-line:no-bitwise
            var v = c === 'x' ? r : (r & 0x3) | 0x8;
            return v.toString(16);
        });
    }
    /**
     * Parses string form of URL into an object
     * // borrowed from https://tools.ietf.org/html/rfc3986#appendix-B
     * // intentionally using regex and not <a/> href parsing trick because React Native and other
     * // environments where DOM might not be available
     * @returns parsed URL object
     */
    function parseUrl(url) {
        if (!url) {
            return {};
        }
        var match = url.match(/^(([^:\/?#]+):)?(\/\/([^\/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$/);
        if (!match) {
            return {};
        }
        // coerce to undefined values to empty string so we don't get 'undefined'
        var query = match[6] || '';
        var fragment = match[8] || '';
        return {
            host: match[4],
            path: match[5],
            protocol: match[2],
            relative: match[5] + query + fragment,
        };
    }
    /**
     * Extracts either message or type+value from an event that can be used for user-facing logs
     * @returns event's description
     */
    function getEventDescription(event) {
        if (event.message) {
            return event.message;
        }
        if (event.exception && event.exception.values && event.exception.values[0]) {
            var exception = event.exception.values[0];
            if (exception.type && exception.value) {
                return exception.type + ": " + exception.value;
            }
            return exception.type || exception.value || event.event_id || '<unknown>';
        }
        return event.event_id || '<unknown>';
    }
    /** JSDoc */
    function consoleSandbox(callback) {
        var global = getGlobalObject();
        var levels = ['debug', 'info', 'warn', 'error', 'log', 'assert'];
        if (!('console' in global)) {
            return callback();
        }
        var originalConsole = global.console;
        var wrappedLevels = {};
        // Restore all wrapped console methods
        levels.forEach(function (level) {
            if (level in global.console && originalConsole[level].__sentry_original__) {
                wrappedLevels[level] = originalConsole[level];
                originalConsole[level] = originalConsole[level].__sentry_original__;
            }
        });
        // Perform callback manipulations
        var result = callback();
        // Revert restoration to wrapped state
        Object.keys(wrappedLevels).forEach(function (level) {
            originalConsole[level] = wrappedLevels[level];
        });
        return result;
    }
    /**
     * Adds exception values, type and value to an synthetic Exception.
     * @param event The event to modify.
     * @param value Value of the exception.
     * @param type Type of the exception.
     * @hidden
     */
    function addExceptionTypeValue(event, value, type) {
        event.exception = event.exception || {};
        event.exception.values = event.exception.values || [];
        event.exception.values[0] = event.exception.values[0] || {};
        event.exception.values[0].value = event.exception.values[0].value || value || '';
        event.exception.values[0].type = event.exception.values[0].type || type || 'Error';
    }
    /**
     * Adds exception mechanism to a given event.
     * @param event The event to modify.
     * @param mechanism Mechanism of the mechanism.
     * @hidden
     */
    function addExceptionMechanism(event, mechanism) {
        if (mechanism === void 0) { mechanism = {}; }
        // TODO: Use real type with `keyof Mechanism` thingy and maybe make it better?
        try {
            // @ts-ignore
            // tslint:disable:no-non-null-assertion
            event.exception.values[0].mechanism = event.exception.values[0].mechanism || {};
            Object.keys(mechanism).forEach(function (key) {
                // @ts-ignore
                event.exception.values[0].mechanism[key] = mechanism[key];
            });
        }
        catch (_oO) {
            // no-empty
        }
    }
    /**
     * A safe form of location.href
     */
    function getLocationHref() {
        try {
            return document.location.href;
        }
        catch (oO) {
            return '';
        }
    }
    /**
     * Given a child DOM element, returns a query-selector statement describing that
     * and its ancestors
     * e.g. [HTMLElement] => body > div > input#foo.btn[name=baz]
     * @returns generated DOM path
     */
    function htmlTreeAsString(elem) {
        // try/catch both:
        // - accessing event.target (see getsentry/raven-js#838, #768)
        // - `htmlTreeAsString` because it's complex, and just accessing the DOM incorrectly
        // - can throw an exception in some circumstances.
        try {
            var currentElem = elem;
            var MAX_TRAVERSE_HEIGHT = 5;
            var MAX_OUTPUT_LEN = 80;
            var out = [];
            var height = 0;
            var len = 0;
            var separator = ' > ';
            var sepLength = separator.length;
            var nextStr = void 0;
            while (currentElem && height++ < MAX_TRAVERSE_HEIGHT) {
                nextStr = _htmlElementAsString(currentElem);
                // bail out if
                // - nextStr is the 'html' element
                // - the length of the string that would be created exceeds MAX_OUTPUT_LEN
                //   (ignore this limit if we are on the first iteration)
                if (nextStr === 'html' || (height > 1 && len + out.length * sepLength + nextStr.length >= MAX_OUTPUT_LEN)) {
                    break;
                }
                out.push(nextStr);
                len += nextStr.length;
                currentElem = currentElem.parentNode;
            }
            return out.reverse().join(separator);
        }
        catch (_oO) {
            return '<unknown>';
        }
    }
    /**
     * Returns a simple, query-selector representation of a DOM element
     * e.g. [HTMLElement] => input#foo.btn[name=baz]
     * @returns generated DOM path
     */
    function _htmlElementAsString(el) {
        var elem = el;
        var out = [];
        var className;
        var classes;
        var key;
        var attr;
        var i;
        if (!elem || !elem.tagName) {
            return '';
        }
        out.push(elem.tagName.toLowerCase());
        if (elem.id) {
            out.push("#" + elem.id);
        }
        className = elem.className;
        if (className && isString(className)) {
            classes = className.split(/\s+/);
            for (i = 0; i < classes.length; i++) {
                out.push("." + classes[i]);
            }
        }
        var attrWhitelist = ['type', 'name', 'title', 'alt'];
        for (i = 0; i < attrWhitelist.length; i++) {
            key = attrWhitelist[i];
            attr = elem.getAttribute(key);
            if (attr) {
                out.push("[" + key + "=\"" + attr + "\"]");
            }
        }
        return out.join('');
    }
    var INITIAL_TIME = Date.now();
    var prevNow = 0;
    var performanceFallback = {
        now: function () {
            var now = Date.now() - INITIAL_TIME;
            if (now < prevNow) {
                now = prevNow;
            }
            prevNow = now;
            return now;
        },
        timeOrigin: INITIAL_TIME,
    };
    var crossPlatformPerformance = (function () {
        if (isNodeEnv()) {
            try {
                var perfHooks = dynamicRequire(module, 'perf_hooks');
                return perfHooks.performance;
            }
            catch (_) {
                return performanceFallback;
            }
        }
        if (getGlobalObject().performance) {
            // Polyfill for performance.timeOrigin.
            //
            // While performance.timing.navigationStart is deprecated in favor of performance.timeOrigin, performance.timeOrigin
            // is not as widely supported. Namely, performance.timeOrigin is undefined in Safari as of writing.
            // tslint:disable-next-line:strict-type-predicates
            if (performance.timeOrigin === undefined) {
                // As of writing, performance.timing is not available in Web Workers in mainstream browsers, so it is not always a
                // valid fallback. In the absence of a initial time provided by the browser, fallback to INITIAL_TIME.
                // @ts-ignore
                // tslint:disable-next-line:deprecation
                performance.timeOrigin = (performance.timing && performance.timing.navigationStart) || INITIAL_TIME;
            }
        }
        return getGlobalObject().performance || performanceFallback;
    })();
    /**
     * Returns a timestamp in seconds with milliseconds precision since the UNIX epoch calculated with the monotonic clock.
     */
    function timestampWithMs() {
        return (crossPlatformPerformance.timeOrigin + crossPlatformPerformance.now()) / 1000;
    }
    var defaultRetryAfter = 60 * 1000; // 60 seconds
    /**
     * Extracts Retry-After value from the request header or returns default value
     * @param now current unix timestamp
     * @param header string representation of 'Retry-After' header
     */
    function parseRetryAfterHeader(now, header) {
        if (!header) {
            return defaultRetryAfter;
        }
        var headerDelay = parseInt("" + header, 10);
        if (!isNaN(headerDelay)) {
            return headerDelay * 1000;
        }
        var headerDate = Date.parse("" + header);
        if (!isNaN(headerDate)) {
            return headerDate - now;
        }
        return defaultRetryAfter;
    }
    var defaultFunctionName = '<anonymous>';
    /**
     * Safely extract function name from itself
     */
    function getFunctionName(fn) {
        try {
            if (!fn || typeof fn !== 'function') {
                return defaultFunctionName;
            }
            return fn.name || defaultFunctionName;
        }
        catch (e) {
            // Just accessing custom props in some Selenium environments
            // can cause a "Permission denied" exception (see raven-js#495).
            return defaultFunctionName;
        }
    }

    // TODO: Implement different loggers for different environments
    var global$1 = getGlobalObject();
    /** Prefix for logging strings */
    var PREFIX = 'Sentry Logger ';
    /** JSDoc */
    var Logger = /** @class */ (function () {
        /** JSDoc */
        function Logger() {
            this._enabled = false;
        }
        /** JSDoc */
        Logger.prototype.disable = function () {
            this._enabled = false;
        };
        /** JSDoc */
        Logger.prototype.enable = function () {
            this._enabled = true;
        };
        /** JSDoc */
        Logger.prototype.log = function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            if (!this._enabled) {
                return;
            }
            consoleSandbox(function () {
                global$1.console.log(PREFIX + "[Log]: " + args.join(' ')); // tslint:disable-line:no-console
            });
        };
        /** JSDoc */
        Logger.prototype.warn = function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            if (!this._enabled) {
                return;
            }
            consoleSandbox(function () {
                global$1.console.warn(PREFIX + "[Warn]: " + args.join(' ')); // tslint:disable-line:no-console
            });
        };
        /** JSDoc */
        Logger.prototype.error = function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            if (!this._enabled) {
                return;
            }
            consoleSandbox(function () {
                global$1.console.error(PREFIX + "[Error]: " + args.join(' ')); // tslint:disable-line:no-console
            });
        };
        return Logger;
    }());
    // Ensure we only have a single logger instance, even if multiple versions of @sentry/utils are being used
    global$1.__SENTRY__ = global$1.__SENTRY__ || {};
    var logger = global$1.__SENTRY__.logger || (global$1.__SENTRY__.logger = new Logger());

    // tslint:disable:no-unsafe-any
    /**
     * Memo class used for decycle json objects. Uses WeakSet if available otherwise array.
     */
    var Memo = /** @class */ (function () {
        function Memo() {
            // tslint:disable-next-line
            this._hasWeakSet = typeof WeakSet === 'function';
            this._inner = this._hasWeakSet ? new WeakSet() : [];
        }
        /**
         * Sets obj to remember.
         * @param obj Object to remember
         */
        Memo.prototype.memoize = function (obj) {
            if (this._hasWeakSet) {
                if (this._inner.has(obj)) {
                    return true;
                }
                this._inner.add(obj);
                return false;
            }
            // tslint:disable-next-line:prefer-for-of
            for (var i = 0; i < this._inner.length; i++) {
                var value = this._inner[i];
                if (value === obj) {
                    return true;
                }
            }
            this._inner.push(obj);
            return false;
        };
        /**
         * Removes object from internal storage.
         * @param obj Object to forget
         */
        Memo.prototype.unmemoize = function (obj) {
            if (this._hasWeakSet) {
                this._inner.delete(obj);
            }
            else {
                for (var i = 0; i < this._inner.length; i++) {
                    if (this._inner[i] === obj) {
                        this._inner.splice(i, 1);
                        break;
                    }
                }
            }
        };
        return Memo;
    }());

    /**
     * Wrap a given object method with a higher-order function
     *
     * @param source An object that contains a method to be wrapped.
     * @param name A name of method to be wrapped.
     * @param replacement A function that should be used to wrap a given method.
     * @returns void
     */
    function fill(source, name, replacement) {
        if (!(name in source)) {
            return;
        }
        var original = source[name];
        var wrapped = replacement(original);
        // Make sure it's a function first, as we need to attach an empty prototype for `defineProperties` to work
        // otherwise it'll throw "TypeError: Object.defineProperties called on non-object"
        // tslint:disable-next-line:strict-type-predicates
        if (typeof wrapped === 'function') {
            try {
                wrapped.prototype = wrapped.prototype || {};
                Object.defineProperties(wrapped, {
                    __sentry_original__: {
                        enumerable: false,
                        value: original,
                    },
                });
            }
            catch (_Oo) {
                // This can throw if multiple fill happens on a global object like XMLHttpRequest
                // Fixes https://github.com/getsentry/sentry-javascript/issues/2043
            }
        }
        source[name] = wrapped;
    }
    /**
     * Encodes given object into url-friendly format
     *
     * @param object An object that contains serializable values
     * @returns string Encoded
     */
    function urlEncode(object) {
        return Object.keys(object)
            .map(
        // tslint:disable-next-line:no-unsafe-any
        function (key) { return encodeURIComponent(key) + "=" + encodeURIComponent(object[key]); })
            .join('&');
    }
    /**
     * Transforms any object into an object literal with all it's attributes
     * attached to it.
     *
     * @param value Initial source that we have to transform in order to be usable by the serializer
     */
    function getWalkSource(value) {
        if (isError(value)) {
            var error = value;
            var err = {
                message: error.message,
                name: error.name,
                stack: error.stack,
            };
            for (var i in error) {
                if (Object.prototype.hasOwnProperty.call(error, i)) {
                    err[i] = error[i];
                }
            }
            return err;
        }
        if (isEvent(value)) {
            var event_1 = value;
            var source = {};
            source.type = event_1.type;
            // Accessing event.target can throw (see getsentry/raven-js#838, #768)
            try {
                source.target = isElement(event_1.target)
                    ? htmlTreeAsString(event_1.target)
                    : Object.prototype.toString.call(event_1.target);
            }
            catch (_oO) {
                source.target = '<unknown>';
            }
            try {
                source.currentTarget = isElement(event_1.currentTarget)
                    ? htmlTreeAsString(event_1.currentTarget)
                    : Object.prototype.toString.call(event_1.currentTarget);
            }
            catch (_oO) {
                source.currentTarget = '<unknown>';
            }
            // tslint:disable-next-line:strict-type-predicates
            if (typeof CustomEvent !== 'undefined' && isInstanceOf(value, CustomEvent)) {
                source.detail = event_1.detail;
            }
            for (var i in event_1) {
                if (Object.prototype.hasOwnProperty.call(event_1, i)) {
                    source[i] = event_1;
                }
            }
            return source;
        }
        return value;
    }
    /** Calculates bytes size of input string */
    function utf8Length(value) {
        // tslint:disable-next-line:no-bitwise
        return ~-encodeURI(value).split(/%..|./).length;
    }
    /** Calculates bytes size of input object */
    function jsonSize(value) {
        return utf8Length(JSON.stringify(value));
    }
    /** JSDoc */
    function normalizeToSize(object, 
    // Default Node.js REPL depth
    depth, 
    // 100kB, as 200kB is max payload size, so half sounds reasonable
    maxSize) {
        if (depth === void 0) { depth = 3; }
        if (maxSize === void 0) { maxSize = 100 * 1024; }
        var serialized = normalize(object, depth);
        if (jsonSize(serialized) > maxSize) {
            return normalizeToSize(object, depth - 1, maxSize);
        }
        return serialized;
    }
    /** Transforms any input value into a string form, either primitive value or a type of the input */
    function serializeValue(value) {
        var type = Object.prototype.toString.call(value);
        // Node.js REPL notation
        if (typeof value === 'string') {
            return value;
        }
        if (type === '[object Object]') {
            return '[Object]';
        }
        if (type === '[object Array]') {
            return '[Array]';
        }
        var normalized = normalizeValue(value);
        return isPrimitive(normalized) ? normalized : type;
    }
    /**
     * normalizeValue()
     *
     * Takes unserializable input and make it serializable friendly
     *
     * - translates undefined/NaN values to "[undefined]"/"[NaN]" respectively,
     * - serializes Error objects
     * - filter global objects
     */
    // tslint:disable-next-line:cyclomatic-complexity
    function normalizeValue(value, key) {
        if (key === 'domain' && value && typeof value === 'object' && value._events) {
            return '[Domain]';
        }
        if (key === 'domainEmitter') {
            return '[DomainEmitter]';
        }
        if (typeof global !== 'undefined' && value === global) {
            return '[Global]';
        }
        if (typeof window !== 'undefined' && value === window) {
            return '[Window]';
        }
        if (typeof document !== 'undefined' && value === document) {
            return '[Document]';
        }
        // React's SyntheticEvent thingy
        if (isSyntheticEvent(value)) {
            return '[SyntheticEvent]';
        }
        // tslint:disable-next-line:no-tautology-expression
        if (typeof value === 'number' && value !== value) {
            return '[NaN]';
        }
        if (value === void 0) {
            return '[undefined]';
        }
        if (typeof value === 'function') {
            return "[Function: " + getFunctionName(value) + "]";
        }
        return value;
    }
    /**
     * Walks an object to perform a normalization on it
     *
     * @param key of object that's walked in current iteration
     * @param value object to be walked
     * @param depth Optional number indicating how deep should walking be performed
     * @param memo Optional Memo class handling decycling
     */
    function walk(key, value, depth, memo) {
        if (depth === void 0) { depth = +Infinity; }
        if (memo === void 0) { memo = new Memo(); }
        // If we reach the maximum depth, serialize whatever has left
        if (depth === 0) {
            return serializeValue(value);
        }
        // If value implements `toJSON` method, call it and return early
        // tslint:disable:no-unsafe-any
        if (value !== null && value !== undefined && typeof value.toJSON === 'function') {
            return value.toJSON();
        }
        // tslint:enable:no-unsafe-any
        // If normalized value is a primitive, there are no branches left to walk, so we can just bail out, as theres no point in going down that branch any further
        var normalized = normalizeValue(value, key);
        if (isPrimitive(normalized)) {
            return normalized;
        }
        // Create source that we will use for next itterations, either objectified error object (Error type with extracted keys:value pairs) or the input itself
        var source = getWalkSource(value);
        // Create an accumulator that will act as a parent for all future itterations of that branch
        var acc = Array.isArray(value) ? [] : {};
        // If we already walked that branch, bail out, as it's circular reference
        if (memo.memoize(value)) {
            return '[Circular ~]';
        }
        // Walk all keys of the source
        for (var innerKey in source) {
            // Avoid iterating over fields in the prototype if they've somehow been exposed to enumeration.
            if (!Object.prototype.hasOwnProperty.call(source, innerKey)) {
                continue;
            }
            // Recursively walk through all the child nodes
            acc[innerKey] = walk(innerKey, source[innerKey], depth - 1, memo);
        }
        // Once walked through all the branches, remove the parent from memo storage
        memo.unmemoize(value);
        // Return accumulated values
        return acc;
    }
    /**
     * normalize()
     *
     * - Creates a copy to prevent original input mutation
     * - Skip non-enumerablers
     * - Calls `toJSON` if implemented
     * - Removes circular references
     * - Translates non-serializeable values (undefined/NaN/Functions) to serializable format
     * - Translates known global objects/Classes to a string representations
     * - Takes care of Error objects serialization
     * - Optionally limit depth of final output
     */
    function normalize(input, depth) {
        try {
            // tslint:disable-next-line:no-unsafe-any
            return JSON.parse(JSON.stringify(input, function (key, value) { return walk(key, value, depth); }));
        }
        catch (_oO) {
            return '**non-serializable**';
        }
    }
    /**
     * Given any captured exception, extract its keys and create a sorted
     * and truncated list that will be used inside the event message.
     * eg. `Non-error exception captured with keys: foo, bar, baz`
     */
    function extractExceptionKeysForMessage(exception, maxLength) {
        if (maxLength === void 0) { maxLength = 40; }
        // tslint:disable:strict-type-predicates
        var keys = Object.keys(getWalkSource(exception));
        keys.sort();
        if (!keys.length) {
            return '[object has no keys]';
        }
        if (keys[0].length >= maxLength) {
            return truncate(keys[0], maxLength);
        }
        for (var includedKeys = keys.length; includedKeys > 0; includedKeys--) {
            var serialized = keys.slice(0, includedKeys).join(', ');
            if (serialized.length > maxLength) {
                continue;
            }
            if (includedKeys === keys.length) {
                return serialized;
            }
            return truncate(serialized, maxLength);
        }
        return '';
    }

    // Slightly modified (no IE8 support, ES6) and transcribed to TypeScript

    /** SyncPromise internal states */
    var States;
    (function (States) {
        /** Pending */
        States["PENDING"] = "PENDING";
        /** Resolved / OK */
        States["RESOLVED"] = "RESOLVED";
        /** Rejected / Error */
        States["REJECTED"] = "REJECTED";
    })(States || (States = {}));
    /**
     * Thenable class that behaves like a Promise and follows it's interface
     * but is not async internally
     */
    var SyncPromise = /** @class */ (function () {
        function SyncPromise(executor) {
            var _this = this;
            this._state = States.PENDING;
            this._handlers = [];
            /** JSDoc */
            this._resolve = function (value) {
                _this._setResult(States.RESOLVED, value);
            };
            /** JSDoc */
            this._reject = function (reason) {
                _this._setResult(States.REJECTED, reason);
            };
            /** JSDoc */
            this._setResult = function (state, value) {
                if (_this._state !== States.PENDING) {
                    return;
                }
                if (isThenable(value)) {
                    value.then(_this._resolve, _this._reject);
                    return;
                }
                _this._state = state;
                _this._value = value;
                _this._executeHandlers();
            };
            // TODO: FIXME
            /** JSDoc */
            this._attachHandler = function (handler) {
                _this._handlers = _this._handlers.concat(handler);
                _this._executeHandlers();
            };
            /** JSDoc */
            this._executeHandlers = function () {
                if (_this._state === States.PENDING) {
                    return;
                }
                var cachedHandlers = _this._handlers.slice();
                _this._handlers = [];
                cachedHandlers.forEach(function (handler) {
                    if (handler.done) {
                        return;
                    }
                    if (_this._state === States.RESOLVED) {
                        if (handler.onfulfilled) {
                            handler.onfulfilled(_this._value);
                        }
                    }
                    if (_this._state === States.REJECTED) {
                        if (handler.onrejected) {
                            handler.onrejected(_this._value);
                        }
                    }
                    handler.done = true;
                });
            };
            try {
                executor(this._resolve, this._reject);
            }
            catch (e) {
                this._reject(e);
            }
        }
        /** JSDoc */
        SyncPromise.prototype.toString = function () {
            return '[object SyncPromise]';
        };
        /** JSDoc */
        SyncPromise.resolve = function (value) {
            return new SyncPromise(function (resolve) {
                resolve(value);
            });
        };
        /** JSDoc */
        SyncPromise.reject = function (reason) {
            return new SyncPromise(function (_, reject) {
                reject(reason);
            });
        };
        /** JSDoc */
        SyncPromise.all = function (collection) {
            return new SyncPromise(function (resolve, reject) {
                if (!Array.isArray(collection)) {
                    reject(new TypeError("Promise.all requires an array as input."));
                    return;
                }
                if (collection.length === 0) {
                    resolve([]);
                    return;
                }
                var counter = collection.length;
                var resolvedCollection = [];
                collection.forEach(function (item, index) {
                    SyncPromise.resolve(item)
                        .then(function (value) {
                        resolvedCollection[index] = value;
                        counter -= 1;
                        if (counter !== 0) {
                            return;
                        }
                        resolve(resolvedCollection);
                    })
                        .then(null, reject);
                });
            });
        };
        /** JSDoc */
        SyncPromise.prototype.then = function (onfulfilled, onrejected) {
            var _this = this;
            return new SyncPromise(function (resolve, reject) {
                _this._attachHandler({
                    done: false,
                    onfulfilled: function (result) {
                        if (!onfulfilled) {
                            // TODO: ¯\_(ツ)_/¯
                            // TODO: FIXME
                            resolve(result);
                            return;
                        }
                        try {
                            resolve(onfulfilled(result));
                            return;
                        }
                        catch (e) {
                            reject(e);
                            return;
                        }
                    },
                    onrejected: function (reason) {
                        if (!onrejected) {
                            reject(reason);
                            return;
                        }
                        try {
                            resolve(onrejected(reason));
                            return;
                        }
                        catch (e) {
                            reject(e);
                            return;
                        }
                    },
                });
            });
        };
        /** JSDoc */
        SyncPromise.prototype.catch = function (onrejected) {
            return this.then(function (val) { return val; }, onrejected);
        };
        /** JSDoc */
        SyncPromise.prototype.finally = function (onfinally) {
            var _this = this;
            return new SyncPromise(function (resolve, reject) {
                var val;
                var isRejected;
                return _this.then(function (value) {
                    isRejected = false;
                    val = value;
                    if (onfinally) {
                        onfinally();
                    }
                }, function (reason) {
                    isRejected = true;
                    val = reason;
                    if (onfinally) {
                        onfinally();
                    }
                }).then(function () {
                    if (isRejected) {
                        reject(val);
                        return;
                    }
                    resolve(val);
                });
            });
        };
        return SyncPromise;
    }());

    /** A simple queue that holds promises. */
    var PromiseBuffer = /** @class */ (function () {
        function PromiseBuffer(_limit) {
            this._limit = _limit;
            /** Internal set of queued Promises */
            this._buffer = [];
        }
        /**
         * Says if the buffer is ready to take more requests
         */
        PromiseBuffer.prototype.isReady = function () {
            return this._limit === undefined || this.length() < this._limit;
        };
        /**
         * Add a promise to the queue.
         *
         * @param task Can be any PromiseLike<T>
         * @returns The original promise.
         */
        PromiseBuffer.prototype.add = function (task) {
            var _this = this;
            if (!this.isReady()) {
                return SyncPromise.reject(new SentryError('Not adding Promise due to buffer limit reached.'));
            }
            if (this._buffer.indexOf(task) === -1) {
                this._buffer.push(task);
            }
            task
                .then(function () { return _this.remove(task); })
                .then(null, function () {
                return _this.remove(task).then(null, function () {
                    // We have to add this catch here otherwise we have an unhandledPromiseRejection
                    // because it's a new Promise chain.
                });
            });
            return task;
        };
        /**
         * Remove a promise to the queue.
         *
         * @param task Can be any PromiseLike<T>
         * @returns Removed promise.
         */
        PromiseBuffer.prototype.remove = function (task) {
            var removedTask = this._buffer.splice(this._buffer.indexOf(task), 1)[0];
            return removedTask;
        };
        /**
         * This function returns the number of unresolved promises in the queue.
         */
        PromiseBuffer.prototype.length = function () {
            return this._buffer.length;
        };
        /**
         * This will drain the whole queue, returns true if queue is empty or drained.
         * If timeout is provided and the queue takes longer to drain, the promise still resolves but with false.
         *
         * @param timeout Number in ms to wait until it resolves with false.
         */
        PromiseBuffer.prototype.drain = function (timeout) {
            var _this = this;
            return new SyncPromise(function (resolve) {
                var capturedSetTimeout = setTimeout(function () {
                    if (timeout && timeout > 0) {
                        resolve(false);
                    }
                }, timeout);
                SyncPromise.all(_this._buffer)
                    .then(function () {
                    clearTimeout(capturedSetTimeout);
                    resolve(true);
                })
                    .then(null, function () {
                    resolve(true);
                });
            });
        };
        return PromiseBuffer;
    }());

    /**
     * Tells whether current environment supports Fetch API
     * {@link supportsFetch}.
     *
     * @returns Answer to the given question.
     */
    function supportsFetch() {
        if (!('fetch' in getGlobalObject())) {
            return false;
        }
        try {
            // tslint:disable-next-line:no-unused-expression
            new Headers();
            // tslint:disable-next-line:no-unused-expression
            new Request('');
            // tslint:disable-next-line:no-unused-expression
            new Response();
            return true;
        }
        catch (e) {
            return false;
        }
    }
    /**
     * isNativeFetch checks if the given function is a native implementation of fetch()
     */
    function isNativeFetch(func) {
        return func && /^function fetch\(\)\s+\{\s+\[native code\]\s+\}$/.test(func.toString());
    }
    /**
     * Tells whether current environment supports Fetch API natively
     * {@link supportsNativeFetch}.
     *
     * @returns true if `window.fetch` is natively implemented, false otherwise
     */
    function supportsNativeFetch() {
        if (!supportsFetch()) {
            return false;
        }
        var global = getGlobalObject();
        // Fast path to avoid DOM I/O
        // tslint:disable-next-line:no-unbound-method
        if (isNativeFetch(global.fetch)) {
            return true;
        }
        // window.fetch is implemented, but is polyfilled or already wrapped (e.g: by a chrome extension)
        // so create a "pure" iframe to see if that has native fetch
        var result = false;
        var doc = global.document;
        // tslint:disable-next-line:no-unbound-method deprecation
        if (doc && typeof doc.createElement === "function") {
            try {
                var sandbox = doc.createElement('iframe');
                sandbox.hidden = true;
                doc.head.appendChild(sandbox);
                if (sandbox.contentWindow && sandbox.contentWindow.fetch) {
                    // tslint:disable-next-line:no-unbound-method
                    result = isNativeFetch(sandbox.contentWindow.fetch);
                }
                doc.head.removeChild(sandbox);
            }
            catch (err) {
                logger.warn('Could not create sandbox iframe for pure fetch check, bailing to window.fetch: ', err);
            }
        }
        return result;
    }
    /**
     * Tells whether current environment supports Referrer Policy API
     * {@link supportsReferrerPolicy}.
     *
     * @returns Answer to the given question.
     */
    function supportsReferrerPolicy() {
        // Despite all stars in the sky saying that Edge supports old draft syntax, aka 'never', 'always', 'origin' and 'default
        // https://caniuse.com/#feat=referrer-policy
        // It doesn't. And it throw exception instead of ignoring this parameter...
        // REF: https://github.com/getsentry/raven-js/issues/1233
        if (!supportsFetch()) {
            return false;
        }
        try {
            // tslint:disable:no-unused-expression
            new Request('_', {
                referrerPolicy: 'origin',
            });
            return true;
        }
        catch (e) {
            return false;
        }
    }
    /**
     * Tells whether current environment supports History API
     * {@link supportsHistory}.
     *
     * @returns Answer to the given question.
     */
    function supportsHistory() {
        // NOTE: in Chrome App environment, touching history.pushState, *even inside
        //       a try/catch block*, will cause Chrome to output an error to console.error
        // borrowed from: https://github.com/angular/angular.js/pull/13945/files
        var global = getGlobalObject();
        var chrome = global.chrome;
        // tslint:disable-next-line:no-unsafe-any
        var isChromePackagedApp = chrome && chrome.app && chrome.app.runtime;
        var hasHistoryApi = 'history' in global && !!global.history.pushState && !!global.history.replaceState;
        return !isChromePackagedApp && hasHistoryApi;
    }

    /* tslint:disable:only-arrow-functions no-unsafe-any */
    var global$2 = getGlobalObject();
    /**
     * Instrument native APIs to call handlers that can be used to create breadcrumbs, APM spans etc.
     *  - Console API
     *  - Fetch API
     *  - XHR API
     *  - History API
     *  - DOM API (click/typing)
     *  - Error API
     *  - UnhandledRejection API
     */
    var handlers = {};
    var instrumented = {};
    /** Instruments given API */
    function instrument(type) {
        if (instrumented[type]) {
            return;
        }
        instrumented[type] = true;
        switch (type) {
            case 'console':
                instrumentConsole();
                break;
            case 'dom':
                instrumentDOM();
                break;
            case 'xhr':
                instrumentXHR();
                break;
            case 'fetch':
                instrumentFetch();
                break;
            case 'history':
                instrumentHistory();
                break;
            case 'error':
                instrumentError();
                break;
            case 'unhandledrejection':
                instrumentUnhandledRejection();
                break;
            default:
                logger.warn('unknown instrumentation type:', type);
        }
    }
    /**
     * Add handler that will be called when given type of instrumentation triggers.
     * Use at your own risk, this might break without changelog notice, only used internally.
     * @hidden
     */
    function addInstrumentationHandler(handler) {
        // tslint:disable-next-line:strict-type-predicates
        if (!handler || typeof handler.type !== 'string' || typeof handler.callback !== 'function') {
            return;
        }
        handlers[handler.type] = handlers[handler.type] || [];
        handlers[handler.type].push(handler.callback);
        instrument(handler.type);
    }
    /** JSDoc */
    function triggerHandlers(type, data) {
        var e_1, _a;
        if (!type || !handlers[type]) {
            return;
        }
        try {
            for (var _b = __values(handlers[type] || []), _c = _b.next(); !_c.done; _c = _b.next()) {
                var handler = _c.value;
                try {
                    handler(data);
                }
                catch (e) {
                    logger.error("Error while triggering instrumentation handler.\nType: " + type + "\nName: " + getFunctionName(handler) + "\nError: " + e);
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
    }
    /** JSDoc */
    function instrumentConsole() {
        if (!('console' in global$2)) {
            return;
        }
        ['debug', 'info', 'warn', 'error', 'log', 'assert'].forEach(function (level) {
            if (!(level in global$2.console)) {
                return;
            }
            fill(global$2.console, level, function (originalConsoleLevel) {
                return function () {
                    var args = [];
                    for (var _i = 0; _i < arguments.length; _i++) {
                        args[_i] = arguments[_i];
                    }
                    triggerHandlers('console', { args: args, level: level });
                    // this fails for some browsers. :(
                    if (originalConsoleLevel) {
                        Function.prototype.apply.call(originalConsoleLevel, global$2.console, args);
                    }
                };
            });
        });
    }
    /** JSDoc */
    function instrumentFetch() {
        if (!supportsNativeFetch()) {
            return;
        }
        fill(global$2, 'fetch', function (originalFetch) {
            return function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                var commonHandlerData = {
                    args: args,
                    fetchData: {
                        method: getFetchMethod(args),
                        url: getFetchUrl(args),
                    },
                    startTimestamp: Date.now(),
                };
                triggerHandlers('fetch', __assign({}, commonHandlerData));
                return originalFetch.apply(global$2, args).then(function (response) {
                    triggerHandlers('fetch', __assign({}, commonHandlerData, { endTimestamp: Date.now(), response: response }));
                    return response;
                }, function (error) {
                    triggerHandlers('fetch', __assign({}, commonHandlerData, { endTimestamp: Date.now(), error: error }));
                    throw error;
                });
            };
        });
    }
    /** Extract `method` from fetch call arguments */
    function getFetchMethod(fetchArgs) {
        if (fetchArgs === void 0) { fetchArgs = []; }
        if ('Request' in global$2 && isInstanceOf(fetchArgs[0], Request) && fetchArgs[0].method) {
            return String(fetchArgs[0].method).toUpperCase();
        }
        if (fetchArgs[1] && fetchArgs[1].method) {
            return String(fetchArgs[1].method).toUpperCase();
        }
        return 'GET';
    }
    /** Extract `url` from fetch call arguments */
    function getFetchUrl(fetchArgs) {
        if (fetchArgs === void 0) { fetchArgs = []; }
        if (typeof fetchArgs[0] === 'string') {
            return fetchArgs[0];
        }
        if ('Request' in global$2 && isInstanceOf(fetchArgs[0], Request)) {
            return fetchArgs[0].url;
        }
        return String(fetchArgs[0]);
    }
    /** JSDoc */
    function instrumentXHR() {
        if (!('XMLHttpRequest' in global$2)) {
            return;
        }
        var xhrproto = XMLHttpRequest.prototype;
        fill(xhrproto, 'open', function (originalOpen) {
            return function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                var url = args[1];
                this.__sentry_xhr__ = {
                    method: isString(args[0]) ? args[0].toUpperCase() : args[0],
                    url: args[1],
                };
                // if Sentry key appears in URL, don't capture it as a request
                if (isString(url) && this.__sentry_xhr__.method === 'POST' && url.match(/sentry_key/)) {
                    this.__sentry_own_request__ = true;
                }
                return originalOpen.apply(this, args);
            };
        });
        fill(xhrproto, 'send', function (originalSend) {
            return function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                var xhr = this; // tslint:disable-line:no-this-assignment
                var commonHandlerData = {
                    args: args,
                    startTimestamp: Date.now(),
                    xhr: xhr,
                };
                triggerHandlers('xhr', __assign({}, commonHandlerData));
                xhr.addEventListener('readystatechange', function () {
                    if (xhr.readyState === 4) {
                        try {
                            // touching statusCode in some platforms throws
                            // an exception
                            if (xhr.__sentry_xhr__) {
                                xhr.__sentry_xhr__.status_code = xhr.status;
                            }
                        }
                        catch (e) {
                            /* do nothing */
                        }
                        triggerHandlers('xhr', __assign({}, commonHandlerData, { endTimestamp: Date.now() }));
                    }
                });
                return originalSend.apply(this, args);
            };
        });
    }
    var lastHref;
    /** JSDoc */
    function instrumentHistory() {
        if (!supportsHistory()) {
            return;
        }
        var oldOnPopState = global$2.onpopstate;
        global$2.onpopstate = function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            var to = global$2.location.href;
            // keep track of the current URL state, as we always receive only the updated state
            var from = lastHref;
            lastHref = to;
            triggerHandlers('history', {
                from: from,
                to: to,
            });
            if (oldOnPopState) {
                return oldOnPopState.apply(this, args);
            }
        };
        /** @hidden */
        function historyReplacementFunction(originalHistoryFunction) {
            return function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                var url = args.length > 2 ? args[2] : undefined;
                if (url) {
                    // coerce to string (this is what pushState does)
                    var from = lastHref;
                    var to = String(url);
                    // keep track of the current URL state, as we always receive only the updated state
                    lastHref = to;
                    triggerHandlers('history', {
                        from: from,
                        to: to,
                    });
                }
                return originalHistoryFunction.apply(this, args);
            };
        }
        fill(global$2.history, 'pushState', historyReplacementFunction);
        fill(global$2.history, 'replaceState', historyReplacementFunction);
    }
    /** JSDoc */
    function instrumentDOM() {
        if (!('document' in global$2)) {
            return;
        }
        // Capture breadcrumbs from any click that is unhandled / bubbled up all the way
        // to the document. Do this before we instrument addEventListener.
        global$2.document.addEventListener('click', domEventHandler('click', triggerHandlers.bind(null, 'dom')), false);
        global$2.document.addEventListener('keypress', keypressEventHandler(triggerHandlers.bind(null, 'dom')), false);
        // After hooking into document bubbled up click and keypresses events, we also hook into user handled click & keypresses.
        ['EventTarget', 'Node'].forEach(function (target) {
            var proto = global$2[target] && global$2[target].prototype;
            if (!proto || !proto.hasOwnProperty || !proto.hasOwnProperty('addEventListener')) {
                return;
            }
            fill(proto, 'addEventListener', function (original) {
                return function (eventName, fn, options) {
                    if (fn && fn.handleEvent) {
                        if (eventName === 'click') {
                            fill(fn, 'handleEvent', function (innerOriginal) {
                                return function (event) {
                                    domEventHandler('click', triggerHandlers.bind(null, 'dom'))(event);
                                    return innerOriginal.call(this, event);
                                };
                            });
                        }
                        if (eventName === 'keypress') {
                            fill(fn, 'handleEvent', function (innerOriginal) {
                                return function (event) {
                                    keypressEventHandler(triggerHandlers.bind(null, 'dom'))(event);
                                    return innerOriginal.call(this, event);
                                };
                            });
                        }
                    }
                    else {
                        if (eventName === 'click') {
                            domEventHandler('click', triggerHandlers.bind(null, 'dom'), true)(this);
                        }
                        if (eventName === 'keypress') {
                            keypressEventHandler(triggerHandlers.bind(null, 'dom'))(this);
                        }
                    }
                    return original.call(this, eventName, fn, options);
                };
            });
            fill(proto, 'removeEventListener', function (original) {
                return function (eventName, fn, options) {
                    var callback = fn;
                    try {
                        callback = callback && (callback.__sentry_wrapped__ || callback);
                    }
                    catch (e) {
                        // ignore, accessing __sentry_wrapped__ will throw in some Selenium environments
                    }
                    return original.call(this, eventName, callback, options);
                };
            });
        });
    }
    var debounceDuration = 1000;
    var debounceTimer = 0;
    var keypressTimeout;
    var lastCapturedEvent;
    /**
     * Wraps addEventListener to capture UI breadcrumbs
     * @param name the event name (e.g. "click")
     * @param handler function that will be triggered
     * @param debounce decides whether it should wait till another event loop
     * @returns wrapped breadcrumb events handler
     * @hidden
     */
    function domEventHandler(name, handler, debounce) {
        if (debounce === void 0) { debounce = false; }
        return function (event) {
            // reset keypress timeout; e.g. triggering a 'click' after
            // a 'keypress' will reset the keypress debounce so that a new
            // set of keypresses can be recorded
            keypressTimeout = undefined;
            // It's possible this handler might trigger multiple times for the same
            // event (e.g. event propagation through node ancestors). Ignore if we've
            // already captured the event.
            if (!event || lastCapturedEvent === event) {
                return;
            }
            lastCapturedEvent = event;
            if (debounceTimer) {
                clearTimeout(debounceTimer);
            }
            if (debounce) {
                debounceTimer = setTimeout(function () {
                    handler({ event: event, name: name });
                });
            }
            else {
                handler({ event: event, name: name });
            }
        };
    }
    /**
     * Wraps addEventListener to capture keypress UI events
     * @param handler function that will be triggered
     * @returns wrapped keypress events handler
     * @hidden
     */
    function keypressEventHandler(handler) {
        // TODO: if somehow user switches keypress target before
        //       debounce timeout is triggered, we will only capture
        //       a single breadcrumb from the FIRST target (acceptable?)
        return function (event) {
            var target;
            try {
                target = event.target;
            }
            catch (e) {
                // just accessing event properties can throw an exception in some rare circumstances
                // see: https://github.com/getsentry/raven-js/issues/838
                return;
            }
            var tagName = target && target.tagName;
            // only consider keypress events on actual input elements
            // this will disregard keypresses targeting body (e.g. tabbing
            // through elements, hotkeys, etc)
            if (!tagName || (tagName !== 'INPUT' && tagName !== 'TEXTAREA' && !target.isContentEditable)) {
                return;
            }
            // record first keypress in a series, but ignore subsequent
            // keypresses until debounce clears
            if (!keypressTimeout) {
                domEventHandler('input', handler)(event);
            }
            clearTimeout(keypressTimeout);
            keypressTimeout = setTimeout(function () {
                keypressTimeout = undefined;
            }, debounceDuration);
        };
    }
    var _oldOnErrorHandler = null;
    /** JSDoc */
    function instrumentError() {
        _oldOnErrorHandler = global$2.onerror;
        global$2.onerror = function (msg, url, line, column, error) {
            triggerHandlers('error', {
                column: column,
                error: error,
                line: line,
                msg: msg,
                url: url,
            });
            if (_oldOnErrorHandler) {
                return _oldOnErrorHandler.apply(this, arguments);
            }
            return false;
        };
    }
    var _oldOnUnhandledRejectionHandler = null;
    /** JSDoc */
    function instrumentUnhandledRejection() {
        _oldOnUnhandledRejectionHandler = global$2.onunhandledrejection;
        global$2.onunhandledrejection = function (e) {
            triggerHandlers('unhandledrejection', e);
            if (_oldOnUnhandledRejectionHandler) {
                return _oldOnUnhandledRejectionHandler.apply(this, arguments);
            }
            return true;
        };
    }

    /** Regular expression used to parse a Dsn. */
    var DSN_REGEX = /^(?:(\w+):)\/\/(?:(\w+)(?::(\w+))?@)([\w\.-]+)(?::(\d+))?\/(.+)/;
    /** Error message */
    var ERROR_MESSAGE = 'Invalid Dsn';
    /** The Sentry Dsn, identifying a Sentry instance and project. */
    var Dsn = /** @class */ (function () {
        /** Creates a new Dsn component */
        function Dsn(from) {
            if (typeof from === 'string') {
                this._fromString(from);
            }
            else {
                this._fromComponents(from);
            }
            this._validate();
        }
        /**
         * Renders the string representation of this Dsn.
         *
         * By default, this will render the public representation without the password
         * component. To get the deprecated private representation, set `withPassword`
         * to true.
         *
         * @param withPassword When set to true, the password will be included.
         */
        Dsn.prototype.toString = function (withPassword) {
            if (withPassword === void 0) { withPassword = false; }
            // tslint:disable-next-line:no-this-assignment
            var _a = this, host = _a.host, path = _a.path, pass = _a.pass, port = _a.port, projectId = _a.projectId, protocol = _a.protocol, user = _a.user;
            return (protocol + "://" + user + (withPassword && pass ? ":" + pass : '') +
                ("@" + host + (port ? ":" + port : '') + "/" + (path ? path + "/" : path) + projectId));
        };
        /** Parses a string into this Dsn. */
        Dsn.prototype._fromString = function (str) {
            var match = DSN_REGEX.exec(str);
            if (!match) {
                throw new SentryError(ERROR_MESSAGE);
            }
            var _a = __read(match.slice(1), 6), protocol = _a[0], user = _a[1], _b = _a[2], pass = _b === void 0 ? '' : _b, host = _a[3], _c = _a[4], port = _c === void 0 ? '' : _c, lastPath = _a[5];
            var path = '';
            var projectId = lastPath;
            var split = projectId.split('/');
            if (split.length > 1) {
                path = split.slice(0, -1).join('/');
                projectId = split.pop();
            }
            this._fromComponents({ host: host, pass: pass, path: path, projectId: projectId, port: port, protocol: protocol, user: user });
        };
        /** Maps Dsn components into this instance. */
        Dsn.prototype._fromComponents = function (components) {
            this.protocol = components.protocol;
            this.user = components.user;
            this.pass = components.pass || '';
            this.host = components.host;
            this.port = components.port || '';
            this.path = components.path || '';
            this.projectId = components.projectId;
        };
        /** Validates this Dsn and throws on error. */
        Dsn.prototype._validate = function () {
            var _this = this;
            ['protocol', 'user', 'host', 'projectId'].forEach(function (component) {
                if (!_this[component]) {
                    throw new SentryError(ERROR_MESSAGE);
                }
            });
            if (this.protocol !== 'http' && this.protocol !== 'https') {
                throw new SentryError(ERROR_MESSAGE);
            }
            if (this.port && isNaN(parseInt(this.port, 10))) {
                throw new SentryError(ERROR_MESSAGE);
            }
        };
        return Dsn;
    }());

    /**
     * Holds additional event information. {@link Scope.applyToEvent} will be
     * called by the client before an event will be sent.
     */
    var Scope = /** @class */ (function () {
        function Scope() {
            /** Flag if notifiying is happening. */
            this._notifyingListeners = false;
            /** Callback for client to receive scope changes. */
            this._scopeListeners = [];
            /** Callback list that will be called after {@link applyToEvent}. */
            this._eventProcessors = [];
            /** Array of breadcrumbs. */
            this._breadcrumbs = [];
            /** User */
            this._user = {};
            /** Tags */
            this._tags = {};
            /** Extra */
            this._extra = {};
            /** Contexts */
            this._context = {};
        }
        /**
         * Add internal on change listener. Used for sub SDKs that need to store the scope.
         * @hidden
         */
        Scope.prototype.addScopeListener = function (callback) {
            this._scopeListeners.push(callback);
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.addEventProcessor = function (callback) {
            this._eventProcessors.push(callback);
            return this;
        };
        /**
         * This will be called on every set call.
         */
        Scope.prototype._notifyScopeListeners = function () {
            var _this = this;
            if (!this._notifyingListeners) {
                this._notifyingListeners = true;
                setTimeout(function () {
                    _this._scopeListeners.forEach(function (callback) {
                        callback(_this);
                    });
                    _this._notifyingListeners = false;
                });
            }
        };
        /**
         * This will be called after {@link applyToEvent} is finished.
         */
        Scope.prototype._notifyEventProcessors = function (processors, event, hint, index) {
            var _this = this;
            if (index === void 0) { index = 0; }
            return new SyncPromise(function (resolve, reject) {
                var processor = processors[index];
                // tslint:disable-next-line:strict-type-predicates
                if (event === null || typeof processor !== 'function') {
                    resolve(event);
                }
                else {
                    var result = processor(__assign({}, event), hint);
                    if (isThenable(result)) {
                        result
                            .then(function (final) { return _this._notifyEventProcessors(processors, final, hint, index + 1).then(resolve); })
                            .then(null, reject);
                    }
                    else {
                        _this._notifyEventProcessors(processors, result, hint, index + 1)
                            .then(resolve)
                            .then(null, reject);
                    }
                }
            });
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setUser = function (user) {
            this._user = user || {};
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setTags = function (tags) {
            this._tags = __assign({}, this._tags, tags);
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setTag = function (key, value) {
            var _a;
            this._tags = __assign({}, this._tags, (_a = {}, _a[key] = value, _a));
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setExtras = function (extras) {
            this._extra = __assign({}, this._extra, extras);
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setExtra = function (key, extra) {
            var _a;
            this._extra = __assign({}, this._extra, (_a = {}, _a[key] = extra, _a));
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setFingerprint = function (fingerprint) {
            this._fingerprint = fingerprint;
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setLevel = function (level) {
            this._level = level;
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setTransaction = function (transaction) {
            this._transaction = transaction;
            if (this._span) {
                this._span.transaction = transaction;
            }
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setContext = function (key, context) {
            var _a;
            this._context = __assign({}, this._context, (_a = {}, _a[key] = context, _a));
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.setSpan = function (span) {
            this._span = span;
            this._notifyScopeListeners();
            return this;
        };
        /**
         * Internal getter for Span, used in Hub.
         * @hidden
         */
        Scope.prototype.getSpan = function () {
            return this._span;
        };
        /**
         * Inherit values from the parent scope.
         * @param scope to clone.
         */
        Scope.clone = function (scope) {
            var newScope = new Scope();
            if (scope) {
                newScope._breadcrumbs = __spread(scope._breadcrumbs);
                newScope._tags = __assign({}, scope._tags);
                newScope._extra = __assign({}, scope._extra);
                newScope._context = __assign({}, scope._context);
                newScope._user = scope._user;
                newScope._level = scope._level;
                newScope._span = scope._span;
                newScope._transaction = scope._transaction;
                newScope._fingerprint = scope._fingerprint;
                newScope._eventProcessors = __spread(scope._eventProcessors);
            }
            return newScope;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.clear = function () {
            this._breadcrumbs = [];
            this._tags = {};
            this._extra = {};
            this._user = {};
            this._context = {};
            this._level = undefined;
            this._transaction = undefined;
            this._fingerprint = undefined;
            this._span = undefined;
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.addBreadcrumb = function (breadcrumb, maxBreadcrumbs) {
            var mergedBreadcrumb = __assign({ timestamp: timestampWithMs() }, breadcrumb);
            this._breadcrumbs =
                maxBreadcrumbs !== undefined && maxBreadcrumbs >= 0
                    ? __spread(this._breadcrumbs, [mergedBreadcrumb]).slice(-maxBreadcrumbs)
                    : __spread(this._breadcrumbs, [mergedBreadcrumb]);
            this._notifyScopeListeners();
            return this;
        };
        /**
         * @inheritDoc
         */
        Scope.prototype.clearBreadcrumbs = function () {
            this._breadcrumbs = [];
            this._notifyScopeListeners();
            return this;
        };
        /**
         * Applies fingerprint from the scope to the event if there's one,
         * uses message if there's one instead or get rid of empty fingerprint
         */
        Scope.prototype._applyFingerprint = function (event) {
            // Make sure it's an array first and we actually have something in place
            event.fingerprint = event.fingerprint
                ? Array.isArray(event.fingerprint)
                    ? event.fingerprint
                    : [event.fingerprint]
                : [];
            // If we have something on the scope, then merge it with event
            if (this._fingerprint) {
                event.fingerprint = event.fingerprint.concat(this._fingerprint);
            }
            // If we have no data at all, remove empty array default
            if (event.fingerprint && !event.fingerprint.length) {
                delete event.fingerprint;
            }
        };
        /**
         * Applies the current context and fingerprint to the event.
         * Note that breadcrumbs will be added by the client.
         * Also if the event has already breadcrumbs on it, we do not merge them.
         * @param event Event
         * @param hint May contain additional informartion about the original exception.
         * @hidden
         */
        Scope.prototype.applyToEvent = function (event, hint) {
            if (this._extra && Object.keys(this._extra).length) {
                event.extra = __assign({}, this._extra, event.extra);
            }
            if (this._tags && Object.keys(this._tags).length) {
                event.tags = __assign({}, this._tags, event.tags);
            }
            if (this._user && Object.keys(this._user).length) {
                event.user = __assign({}, this._user, event.user);
            }
            if (this._context && Object.keys(this._context).length) {
                event.contexts = __assign({}, this._context, event.contexts);
            }
            if (this._level) {
                event.level = this._level;
            }
            if (this._transaction) {
                event.transaction = this._transaction;
            }
            if (this._span) {
                event.contexts = __assign({ trace: this._span.getTraceContext() }, event.contexts);
            }
            this._applyFingerprint(event);
            event.breadcrumbs = __spread((event.breadcrumbs || []), this._breadcrumbs);
            event.breadcrumbs = event.breadcrumbs.length > 0 ? event.breadcrumbs : undefined;
            return this._notifyEventProcessors(__spread(getGlobalEventProcessors(), this._eventProcessors), event, hint);
        };
        return Scope;
    }());
    /**
     * Retruns the global event processors.
     */
    function getGlobalEventProcessors() {
        var global = getGlobalObject();
        global.__SENTRY__ = global.__SENTRY__ || {};
        global.__SENTRY__.globalEventProcessors = global.__SENTRY__.globalEventProcessors || [];
        return global.__SENTRY__.globalEventProcessors;
    }
    /**
     * Add a EventProcessor to be kept globally.
     * @param callback EventProcessor to add
     */
    function addGlobalEventProcessor(callback) {
        getGlobalEventProcessors().push(callback);
    }

    /**
     * API compatibility version of this hub.
     *
     * WARNING: This number should only be incresed when the global interface
     * changes a and new methods are introduced.
     *
     * @hidden
     */
    var API_VERSION = 3;
    /**
     * Default maximum number of breadcrumbs added to an event. Can be overwritten
     * with {@link Options.maxBreadcrumbs}.
     */
    var DEFAULT_BREADCRUMBS = 100;
    /**
     * Absolute maximum number of breadcrumbs added to an event. The
     * `maxBreadcrumbs` option cannot be higher than this value.
     */
    var MAX_BREADCRUMBS = 100;
    /**
     * @inheritDoc
     */
    var Hub = /** @class */ (function () {
        /**
         * Creates a new instance of the hub, will push one {@link Layer} into the
         * internal stack on creation.
         *
         * @param client bound to the hub.
         * @param scope bound to the hub.
         * @param version number, higher number means higher priority.
         */
        function Hub(client, scope, _version) {
            if (scope === void 0) { scope = new Scope(); }
            if (_version === void 0) { _version = API_VERSION; }
            this._version = _version;
            /** Is a {@link Layer}[] containing the client and scope */
            this._stack = [];
            this._stack.push({ client: client, scope: scope });
        }
        /**
         * Internal helper function to call a method on the top client if it exists.
         *
         * @param method The method to call on the client.
         * @param args Arguments to pass to the client function.
         */
        Hub.prototype._invokeClient = function (method) {
            var _a;
            var args = [];
            for (var _i = 1; _i < arguments.length; _i++) {
                args[_i - 1] = arguments[_i];
            }
            var top = this.getStackTop();
            if (top && top.client && top.client[method]) {
                (_a = top.client)[method].apply(_a, __spread(args, [top.scope]));
            }
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.isOlderThan = function (version) {
            return this._version < version;
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.bindClient = function (client) {
            var top = this.getStackTop();
            top.client = client;
            if (client && client.setupIntegrations) {
                client.setupIntegrations();
            }
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.pushScope = function () {
            // We want to clone the content of prev scope
            var stack = this.getStack();
            var parentScope = stack.length > 0 ? stack[stack.length - 1].scope : undefined;
            var scope = Scope.clone(parentScope);
            this.getStack().push({
                client: this.getClient(),
                scope: scope,
            });
            return scope;
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.popScope = function () {
            return this.getStack().pop() !== undefined;
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.withScope = function (callback) {
            var scope = this.pushScope();
            try {
                callback(scope);
            }
            finally {
                this.popScope();
            }
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.getClient = function () {
            return this.getStackTop().client;
        };
        /** Returns the scope of the top stack. */
        Hub.prototype.getScope = function () {
            return this.getStackTop().scope;
        };
        /** Returns the scope stack for domains or the process. */
        Hub.prototype.getStack = function () {
            return this._stack;
        };
        /** Returns the topmost scope layer in the order domain > local > process. */
        Hub.prototype.getStackTop = function () {
            return this._stack[this._stack.length - 1];
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.captureException = function (exception, hint) {
            var eventId = (this._lastEventId = uuid4());
            var finalHint = hint;
            // If there's no explicit hint provided, mimick the same thing that would happen
            // in the minimal itself to create a consistent behavior.
            // We don't do this in the client, as it's the lowest level API, and doing this,
            // would prevent user from having full control over direct calls.
            if (!hint) {
                var syntheticException = void 0;
                try {
                    throw new Error('Sentry syntheticException');
                }
                catch (exception) {
                    syntheticException = exception;
                }
                finalHint = {
                    originalException: exception,
                    syntheticException: syntheticException,
                };
            }
            this._invokeClient('captureException', exception, __assign({}, finalHint, { event_id: eventId }));
            return eventId;
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.captureMessage = function (message, level, hint) {
            var eventId = (this._lastEventId = uuid4());
            var finalHint = hint;
            // If there's no explicit hint provided, mimick the same thing that would happen
            // in the minimal itself to create a consistent behavior.
            // We don't do this in the client, as it's the lowest level API, and doing this,
            // would prevent user from having full control over direct calls.
            if (!hint) {
                var syntheticException = void 0;
                try {
                    throw new Error(message);
                }
                catch (exception) {
                    syntheticException = exception;
                }
                finalHint = {
                    originalException: message,
                    syntheticException: syntheticException,
                };
            }
            this._invokeClient('captureMessage', message, level, __assign({}, finalHint, { event_id: eventId }));
            return eventId;
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.captureEvent = function (event, hint) {
            var eventId = (this._lastEventId = uuid4());
            this._invokeClient('captureEvent', event, __assign({}, hint, { event_id: eventId }));
            return eventId;
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.lastEventId = function () {
            return this._lastEventId;
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.addBreadcrumb = function (breadcrumb, hint) {
            var top = this.getStackTop();
            if (!top.scope || !top.client) {
                return;
            }
            var _a = (top.client.getOptions && top.client.getOptions()) || {}, _b = _a.beforeBreadcrumb, beforeBreadcrumb = _b === void 0 ? null : _b, _c = _a.maxBreadcrumbs, maxBreadcrumbs = _c === void 0 ? DEFAULT_BREADCRUMBS : _c;
            if (maxBreadcrumbs <= 0) {
                return;
            }
            var timestamp = timestampWithMs();
            var mergedBreadcrumb = __assign({ timestamp: timestamp }, breadcrumb);
            var finalBreadcrumb = beforeBreadcrumb
                ? consoleSandbox(function () { return beforeBreadcrumb(mergedBreadcrumb, hint); })
                : mergedBreadcrumb;
            if (finalBreadcrumb === null) {
                return;
            }
            top.scope.addBreadcrumb(finalBreadcrumb, Math.min(maxBreadcrumbs, MAX_BREADCRUMBS));
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.setUser = function (user) {
            var top = this.getStackTop();
            if (!top.scope) {
                return;
            }
            top.scope.setUser(user);
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.setTags = function (tags) {
            var top = this.getStackTop();
            if (!top.scope) {
                return;
            }
            top.scope.setTags(tags);
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.setExtras = function (extras) {
            var top = this.getStackTop();
            if (!top.scope) {
                return;
            }
            top.scope.setExtras(extras);
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.setTag = function (key, value) {
            var top = this.getStackTop();
            if (!top.scope) {
                return;
            }
            top.scope.setTag(key, value);
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.setExtra = function (key, extra) {
            var top = this.getStackTop();
            if (!top.scope) {
                return;
            }
            top.scope.setExtra(key, extra);
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.setContext = function (name, context) {
            var top = this.getStackTop();
            if (!top.scope) {
                return;
            }
            top.scope.setContext(name, context);
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.configureScope = function (callback) {
            var top = this.getStackTop();
            if (top.scope && top.client) {
                callback(top.scope);
            }
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.run = function (callback) {
            var oldHub = makeMain(this);
            try {
                callback(this);
            }
            finally {
                makeMain(oldHub);
            }
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.getIntegration = function (integration) {
            var client = this.getClient();
            if (!client) {
                return null;
            }
            try {
                return client.getIntegration(integration);
            }
            catch (_oO) {
                logger.warn("Cannot retrieve integration " + integration.id + " from the current Hub");
                return null;
            }
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.startSpan = function (spanOrSpanContext, forceNoChild) {
            if (forceNoChild === void 0) { forceNoChild = false; }
            return this._callExtensionMethod('startSpan', spanOrSpanContext, forceNoChild);
        };
        /**
         * @inheritDoc
         */
        Hub.prototype.traceHeaders = function () {
            return this._callExtensionMethod('traceHeaders');
        };
        /**
         * Calls global extension method and binding current instance to the function call
         */
        // @ts-ignore
        Hub.prototype._callExtensionMethod = function (method) {
            var args = [];
            for (var _i = 1; _i < arguments.length; _i++) {
                args[_i - 1] = arguments[_i];
            }
            var carrier = getMainCarrier();
            var sentry = carrier.__SENTRY__;
            // tslint:disable-next-line: strict-type-predicates
            if (sentry && sentry.extensions && typeof sentry.extensions[method] === 'function') {
                return sentry.extensions[method].apply(this, args);
            }
            logger.warn("Extension method " + method + " couldn't be found, doing nothing.");
        };
        return Hub;
    }());
    /** Returns the global shim registry. */
    function getMainCarrier() {
        var carrier = getGlobalObject();
        carrier.__SENTRY__ = carrier.__SENTRY__ || {
            extensions: {},
            hub: undefined,
        };
        return carrier;
    }
    /**
     * Replaces the current main hub with the passed one on the global object
     *
     * @returns The old replaced hub
     */
    function makeMain(hub) {
        var registry = getMainCarrier();
        var oldHub = getHubFromCarrier(registry);
        setHubOnCarrier(registry, hub);
        return oldHub;
    }
    /**
     * Returns the default hub instance.
     *
     * If a hub is already registered in the global carrier but this module
     * contains a more recent version, it replaces the registered version.
     * Otherwise, the currently registered hub will be returned.
     */
    function getCurrentHub() {
        // Get main carrier (global for every environment)
        var registry = getMainCarrier();
        // If there's no hub, or its an old API, assign a new one
        if (!hasHubOnCarrier(registry) || getHubFromCarrier(registry).isOlderThan(API_VERSION)) {
            setHubOnCarrier(registry, new Hub());
        }
        // Prefer domains over global if they are there (applicable only to Node environment)
        if (isNodeEnv()) {
            return getHubFromActiveDomain(registry);
        }
        // Return hub that lives on a global object
        return getHubFromCarrier(registry);
    }
    /**
     * Try to read the hub from an active domain, fallback to the registry if one doesnt exist
     * @returns discovered hub
     */
    function getHubFromActiveDomain(registry) {
        try {
            var property = 'domain';
            var carrier = getMainCarrier();
            var sentry = carrier.__SENTRY__;
            // tslint:disable-next-line: strict-type-predicates
            if (!sentry || !sentry.extensions || !sentry.extensions[property]) {
                return getHubFromCarrier(registry);
            }
            var domain = sentry.extensions[property];
            var activeDomain = domain.active;
            // If there no active domain, just return global hub
            if (!activeDomain) {
                return getHubFromCarrier(registry);
            }
            // If there's no hub on current domain, or its an old API, assign a new one
            if (!hasHubOnCarrier(activeDomain) || getHubFromCarrier(activeDomain).isOlderThan(API_VERSION)) {
                var registryHubTopStack = getHubFromCarrier(registry).getStackTop();
                setHubOnCarrier(activeDomain, new Hub(registryHubTopStack.client, Scope.clone(registryHubTopStack.scope)));
            }
            // Return hub that lives on a domain
            return getHubFromCarrier(activeDomain);
        }
        catch (_Oo) {
            // Return hub that lives on a global object
            return getHubFromCarrier(registry);
        }
    }
    /**
     * This will tell whether a carrier has a hub on it or not
     * @param carrier object
     */
    function hasHubOnCarrier(carrier) {
        if (carrier && carrier.__SENTRY__ && carrier.__SENTRY__.hub) {
            return true;
        }
        return false;
    }
    /**
     * This will create a new {@link Hub} and add to the passed object on
     * __SENTRY__.hub.
     * @param carrier object
     * @hidden
     */
    function getHubFromCarrier(carrier) {
        if (carrier && carrier.__SENTRY__ && carrier.__SENTRY__.hub) {
            return carrier.__SENTRY__.hub;
        }
        carrier.__SENTRY__ = carrier.__SENTRY__ || {};
        carrier.__SENTRY__.hub = new Hub();
        return carrier.__SENTRY__.hub;
    }
    /**
     * This will set passed {@link Hub} on the passed object's __SENTRY__.hub attribute
     * @param carrier object
     * @param hub Hub
     */
    function setHubOnCarrier(carrier, hub) {
        if (!carrier) {
            return false;
        }
        carrier.__SENTRY__ = carrier.__SENTRY__ || {};
        carrier.__SENTRY__.hub = hub;
        return true;
    }

    /**
     * This calls a function on the current hub.
     * @param method function to call on hub.
     * @param args to pass to function.
     */
    function callOnHub(method) {
        var args = [];
        for (var _i = 1; _i < arguments.length; _i++) {
            args[_i - 1] = arguments[_i];
        }
        var hub = getCurrentHub();
        if (hub && hub[method]) {
            // tslint:disable-next-line:no-unsafe-any
            return hub[method].apply(hub, __spread(args));
        }
        throw new Error("No hub defined or " + method + " was not found on the hub, please open a bug report.");
    }
    /**
     * Captures an exception event and sends it to Sentry.
     *
     * @param exception An exception-like object.
     * @returns The generated eventId.
     */
    function captureException(exception) {
        var syntheticException;
        try {
            throw new Error('Sentry syntheticException');
        }
        catch (exception) {
            syntheticException = exception;
        }
        return callOnHub('captureException', exception, {
            originalException: exception,
            syntheticException: syntheticException,
        });
    }
    /**
     * Captures a message event and sends it to Sentry.
     *
     * @param message The message to send to Sentry.
     * @param level Define the level of the message.
     * @returns The generated eventId.
     */
    function captureMessage(message, level) {
        var syntheticException;
        try {
            throw new Error(message);
        }
        catch (exception) {
            syntheticException = exception;
        }
        return callOnHub('captureMessage', message, level, {
            originalException: message,
            syntheticException: syntheticException,
        });
    }
    /**
     * Captures a manually created event and sends it to Sentry.
     *
     * @param event The event to send to Sentry.
     * @returns The generated eventId.
     */
    function captureEvent(event) {
        return callOnHub('captureEvent', event);
    }
    /**
     * Callback to set context information onto the scope.
     * @param callback Callback function that receives Scope.
     */
    function configureScope(callback) {
        callOnHub('configureScope', callback);
    }
    /**
     * Records a new breadcrumb which will be attached to future events.
     *
     * Breadcrumbs will be added to subsequent events to provide more context on
     * user's actions prior to an error or crash.
     *
     * @param breadcrumb The breadcrumb to record.
     */
    function addBreadcrumb(breadcrumb) {
        callOnHub('addBreadcrumb', breadcrumb);
    }
    /**
     * Sets context data with the given name.
     * @param name of the context
     * @param context Any kind of data. This data will be normailzed.
     */
    function setContext(name, context) {
        callOnHub('setContext', name, context);
    }
    /**
     * Set an object that will be merged sent as extra data with the event.
     * @param extras Extras object to merge into current context.
     */
    function setExtras(extras) {
        callOnHub('setExtras', extras);
    }
    /**
     * Set an object that will be merged sent as tags data with the event.
     * @param tags Tags context object to merge into current context.
     */
    function setTags(tags) {
        callOnHub('setTags', tags);
    }
    /**
     * Set key:value that will be sent as extra data with the event.
     * @param key String of extra
     * @param extra Any kind of data. This data will be normailzed.
     */
    function setExtra(key, extra) {
        callOnHub('setExtra', key, extra);
    }
    /**
     * Set key:value that will be sent as tags data with the event.
     * @param key String key of tag
     * @param value String value of tag
     */
    function setTag(key, value) {
        callOnHub('setTag', key, value);
    }
    /**
     * Updates user context information for future events.
     *
     * @param user User context object to be set in the current context. Pass `null` to unset the user.
     */
    function setUser(user) {
        callOnHub('setUser', user);
    }
    /**
     * Creates a new scope with and executes the given operation within.
     * The scope is automatically removed once the operation
     * finishes or throws.
     *
     * This is essentially a convenience function for:
     *
     *     pushScope();
     *     callback();
     *     popScope();
     *
     * @param callback that will be enclosed into push/popScope.
     */
    function withScope(callback) {
        callOnHub('withScope', callback);
    }

    var SENTRY_API_VERSION = '7';
    /** Helper class to provide urls to different Sentry endpoints. */
    var API = /** @class */ (function () {
        /** Create a new instance of API */
        function API(dsn) {
            this.dsn = dsn;
            this._dsnObject = new Dsn(dsn);
        }
        /** Returns the Dsn object. */
        API.prototype.getDsn = function () {
            return this._dsnObject;
        };
        /** Returns a string with auth headers in the url to the store endpoint. */
        API.prototype.getStoreEndpoint = function () {
            return "" + this._getBaseUrl() + this.getStoreEndpointPath();
        };
        /** Returns the store endpoint with auth added in url encoded. */
        API.prototype.getStoreEndpointWithUrlEncodedAuth = function () {
            var dsn = this._dsnObject;
            var auth = {
                sentry_key: dsn.user,
                sentry_version: SENTRY_API_VERSION,
            };
            // Auth is intentionally sent as part of query string (NOT as custom HTTP header)
            // to avoid preflight CORS requests
            return this.getStoreEndpoint() + "?" + urlEncode(auth);
        };
        /** Returns the base path of the url including the port. */
        API.prototype._getBaseUrl = function () {
            var dsn = this._dsnObject;
            var protocol = dsn.protocol ? dsn.protocol + ":" : '';
            var port = dsn.port ? ":" + dsn.port : '';
            return protocol + "//" + dsn.host + port;
        };
        /** Returns only the path component for the store endpoint. */
        API.prototype.getStoreEndpointPath = function () {
            var dsn = this._dsnObject;
            return (dsn.path ? "/" + dsn.path : '') + "/api/" + dsn.projectId + "/store/";
        };
        /** Returns an object that can be used in request headers. */
        API.prototype.getRequestHeaders = function (clientName, clientVersion) {
            var dsn = this._dsnObject;
            var header = ["Sentry sentry_version=" + SENTRY_API_VERSION];
            header.push("sentry_client=" + clientName + "/" + clientVersion);
            header.push("sentry_key=" + dsn.user);
            if (dsn.pass) {
                header.push("sentry_secret=" + dsn.pass);
            }
            return {
                'Content-Type': 'application/json',
                'X-Sentry-Auth': header.join(', '),
            };
        };
        /** Returns the url to the report dialog endpoint. */
        API.prototype.getReportDialogEndpoint = function (dialogOptions) {
            if (dialogOptions === void 0) { dialogOptions = {}; }
            var dsn = this._dsnObject;
            var endpoint = "" + this._getBaseUrl() + (dsn.path ? "/" + dsn.path : '') + "/api/embed/error-page/";
            var encodedOptions = [];
            encodedOptions.push("dsn=" + dsn.toString());
            for (var key in dialogOptions) {
                if (key === 'user') {
                    if (!dialogOptions.user) {
                        continue;
                    }
                    if (dialogOptions.user.name) {
                        encodedOptions.push("name=" + encodeURIComponent(dialogOptions.user.name));
                    }
                    if (dialogOptions.user.email) {
                        encodedOptions.push("email=" + encodeURIComponent(dialogOptions.user.email));
                    }
                }
                else {
                    encodedOptions.push(encodeURIComponent(key) + "=" + encodeURIComponent(dialogOptions[key]));
                }
            }
            if (encodedOptions.length) {
                return endpoint + "?" + encodedOptions.join('&');
            }
            return endpoint;
        };
        return API;
    }());

    var installedIntegrations = [];
    /** Gets integration to install */
    function getIntegrationsToSetup(options) {
        var defaultIntegrations = (options.defaultIntegrations && __spread(options.defaultIntegrations)) || [];
        var userIntegrations = options.integrations;
        var integrations = [];
        if (Array.isArray(userIntegrations)) {
            var userIntegrationsNames_1 = userIntegrations.map(function (i) { return i.name; });
            var pickedIntegrationsNames_1 = [];
            // Leave only unique default integrations, that were not overridden with provided user integrations
            defaultIntegrations.forEach(function (defaultIntegration) {
                if (userIntegrationsNames_1.indexOf(defaultIntegration.name) === -1 &&
                    pickedIntegrationsNames_1.indexOf(defaultIntegration.name) === -1) {
                    integrations.push(defaultIntegration);
                    pickedIntegrationsNames_1.push(defaultIntegration.name);
                }
            });
            // Don't add same user integration twice
            userIntegrations.forEach(function (userIntegration) {
                if (pickedIntegrationsNames_1.indexOf(userIntegration.name) === -1) {
                    integrations.push(userIntegration);
                    pickedIntegrationsNames_1.push(userIntegration.name);
                }
            });
        }
        else if (typeof userIntegrations === 'function') {
            integrations = userIntegrations(defaultIntegrations);
            integrations = Array.isArray(integrations) ? integrations : [integrations];
        }
        else {
            integrations = __spread(defaultIntegrations);
        }
        // Make sure that if present, `Debug` integration will always run last
        var integrationsNames = integrations.map(function (i) { return i.name; });
        var alwaysLastToRun = 'Debug';
        if (integrationsNames.indexOf(alwaysLastToRun) !== -1) {
            integrations.push.apply(integrations, __spread(integrations.splice(integrationsNames.indexOf(alwaysLastToRun), 1)));
        }
        return integrations;
    }
    /** Setup given integration */
    function setupIntegration(integration) {
        if (installedIntegrations.indexOf(integration.name) !== -1) {
            return;
        }
        integration.setupOnce(addGlobalEventProcessor, getCurrentHub);
        installedIntegrations.push(integration.name);
        logger.log("Integration installed: " + integration.name);
    }
    /**
     * Given a list of integration instances this installs them all. When `withDefaults` is set to `true` then all default
     * integrations are added unless they were already provided before.
     * @param integrations array of integration instances
     * @param withDefault should enable default integrations
     */
    function setupIntegrations(options) {
        var integrations = {};
        getIntegrationsToSetup(options).forEach(function (integration) {
            integrations[integration.name] = integration;
            setupIntegration(integration);
        });
        return integrations;
    }

    /**
     * Base implementation for all JavaScript SDK clients.
     *
     * Call the constructor with the corresponding backend constructor and options
     * specific to the client subclass. To access these options later, use
     * {@link Client.getOptions}. Also, the Backend instance is available via
     * {@link Client.getBackend}.
     *
     * If a Dsn is specified in the options, it will be parsed and stored. Use
     * {@link Client.getDsn} to retrieve the Dsn at any moment. In case the Dsn is
     * invalid, the constructor will throw a {@link SentryException}. Note that
     * without a valid Dsn, the SDK will not send any events to Sentry.
     *
     * Before sending an event via the backend, it is passed through
     * {@link BaseClient.prepareEvent} to add SDK information and scope data
     * (breadcrumbs and context). To add more custom information, override this
     * method and extend the resulting prepared event.
     *
     * To issue automatically created events (e.g. via instrumentation), use
     * {@link Client.captureEvent}. It will prepare the event and pass it through
     * the callback lifecycle. To issue auto-breadcrumbs, use
     * {@link Client.addBreadcrumb}.
     *
     * @example
     * class NodeClient extends BaseClient<NodeBackend, NodeOptions> {
     *   public constructor(options: NodeOptions) {
     *     super(NodeBackend, options);
     *   }
     *
     *   // ...
     * }
     */
    var BaseClient = /** @class */ (function () {
        /**
         * Initializes this client instance.
         *
         * @param backendClass A constructor function to create the backend.
         * @param options Options for the client.
         */
        function BaseClient(backendClass, options) {
            /** Array of used integrations. */
            this._integrations = {};
            /** Is the client still processing a call? */
            this._processing = false;
            this._backend = new backendClass(options);
            this._options = options;
            if (options.dsn) {
                this._dsn = new Dsn(options.dsn);
            }
        }
        /**
         * @inheritDoc
         */
        BaseClient.prototype.captureException = function (exception, hint, scope) {
            var _this = this;
            var eventId = hint && hint.event_id;
            this._processing = true;
            this._getBackend()
                .eventFromException(exception, hint)
                .then(function (event) { return _this._processEvent(event, hint, scope); })
                .then(function (finalEvent) {
                // We need to check for finalEvent in case beforeSend returned null
                eventId = finalEvent && finalEvent.event_id;
                _this._processing = false;
            })
                .then(null, function (reason) {
                logger.error(reason);
                _this._processing = false;
            });
            return eventId;
        };
        /**
         * @inheritDoc
         */
        BaseClient.prototype.captureMessage = function (message, level, hint, scope) {
            var _this = this;
            var eventId = hint && hint.event_id;
            this._processing = true;
            var promisedEvent = isPrimitive(message)
                ? this._getBackend().eventFromMessage("" + message, level, hint)
                : this._getBackend().eventFromException(message, hint);
            promisedEvent
                .then(function (event) { return _this._processEvent(event, hint, scope); })
                .then(function (finalEvent) {
                // We need to check for finalEvent in case beforeSend returned null
                eventId = finalEvent && finalEvent.event_id;
                _this._processing = false;
            })
                .then(null, function (reason) {
                logger.error(reason);
                _this._processing = false;
            });
            return eventId;
        };
        /**
         * @inheritDoc
         */
        BaseClient.prototype.captureEvent = function (event, hint, scope) {
            var _this = this;
            var eventId = hint && hint.event_id;
            this._processing = true;
            this._processEvent(event, hint, scope)
                .then(function (finalEvent) {
                // We need to check for finalEvent in case beforeSend returned null
                eventId = finalEvent && finalEvent.event_id;
                _this._processing = false;
            })
                .then(null, function (reason) {
                logger.error(reason);
                _this._processing = false;
            });
            return eventId;
        };
        /**
         * @inheritDoc
         */
        BaseClient.prototype.getDsn = function () {
            return this._dsn;
        };
        /**
         * @inheritDoc
         */
        BaseClient.prototype.getOptions = function () {
            return this._options;
        };
        /**
         * @inheritDoc
         */
        BaseClient.prototype.flush = function (timeout) {
            var _this = this;
            return this._isClientProcessing(timeout).then(function (status) {
                clearInterval(status.interval);
                return _this._getBackend()
                    .getTransport()
                    .close(timeout)
                    .then(function (transportFlushed) { return status.ready && transportFlushed; });
            });
        };
        /**
         * @inheritDoc
         */
        BaseClient.prototype.close = function (timeout) {
            var _this = this;
            return this.flush(timeout).then(function (result) {
                _this.getOptions().enabled = false;
                return result;
            });
        };
        /**
         * Sets up the integrations
         */
        BaseClient.prototype.setupIntegrations = function () {
            if (this._isEnabled()) {
                this._integrations = setupIntegrations(this._options);
            }
        };
        /**
         * @inheritDoc
         */
        BaseClient.prototype.getIntegration = function (integration) {
            try {
                return this._integrations[integration.id] || null;
            }
            catch (_oO) {
                logger.warn("Cannot retrieve integration " + integration.id + " from the current Client");
                return null;
            }
        };
        /** Waits for the client to be done with processing. */
        BaseClient.prototype._isClientProcessing = function (timeout) {
            var _this = this;
            return new SyncPromise(function (resolve) {
                var ticked = 0;
                var tick = 1;
                var interval = 0;
                clearInterval(interval);
                interval = setInterval(function () {
                    if (!_this._processing) {
                        resolve({
                            interval: interval,
                            ready: true,
                        });
                    }
                    else {
                        ticked += tick;
                        if (timeout && ticked >= timeout) {
                            resolve({
                                interval: interval,
                                ready: false,
                            });
                        }
                    }
                }, tick);
            });
        };
        /** Returns the current backend. */
        BaseClient.prototype._getBackend = function () {
            return this._backend;
        };
        /** Determines whether this SDK is enabled and a valid Dsn is present. */
        BaseClient.prototype._isEnabled = function () {
            return this.getOptions().enabled !== false && this._dsn !== undefined;
        };
        /**
         * Adds common information to events.
         *
         * The information includes release and environment from `options`,
         * breadcrumbs and context (extra, tags and user) from the scope.
         *
         * Information that is already present in the event is never overwritten. For
         * nested objects, such as the context, keys are merged.
         *
         * @param event The original event.
         * @param hint May contain additional informartion about the original exception.
         * @param scope A scope containing event metadata.
         * @returns A new event with more information.
         */
        BaseClient.prototype._prepareEvent = function (event, scope, hint) {
            var _this = this;
            var _a = this.getOptions(), environment = _a.environment, release = _a.release, dist = _a.dist, _b = _a.maxValueLength, maxValueLength = _b === void 0 ? 250 : _b, _c = _a.normalizeDepth, normalizeDepth = _c === void 0 ? 3 : _c;
            var prepared = __assign({}, event);
            if (prepared.environment === undefined && environment !== undefined) {
                prepared.environment = environment;
            }
            if (prepared.release === undefined && release !== undefined) {
                prepared.release = release;
            }
            if (prepared.dist === undefined && dist !== undefined) {
                prepared.dist = dist;
            }
            if (prepared.message) {
                prepared.message = truncate(prepared.message, maxValueLength);
            }
            var exception = prepared.exception && prepared.exception.values && prepared.exception.values[0];
            if (exception && exception.value) {
                exception.value = truncate(exception.value, maxValueLength);
            }
            var request = prepared.request;
            if (request && request.url) {
                request.url = truncate(request.url, maxValueLength);
            }
            if (prepared.event_id === undefined) {
                prepared.event_id = hint && hint.event_id ? hint.event_id : uuid4();
            }
            this._addIntegrations(prepared.sdk);
            // We prepare the result here with a resolved Event.
            var result = SyncPromise.resolve(prepared);
            // This should be the last thing called, since we want that
            // {@link Hub.addEventProcessor} gets the finished prepared event.
            if (scope) {
                // In case we have a hub we reassign it.
                result = scope.applyToEvent(prepared, hint);
            }
            return result.then(function (evt) {
                // tslint:disable-next-line:strict-type-predicates
                if (typeof normalizeDepth === 'number' && normalizeDepth > 0) {
                    return _this._normalizeEvent(evt, normalizeDepth);
                }
                return evt;
            });
        };
        /**
         * Applies `normalize` function on necessary `Event` attributes to make them safe for serialization.
         * Normalized keys:
         * - `breadcrumbs.data`
         * - `user`
         * - `contexts`
         * - `extra`
         * @param event Event
         * @returns Normalized event
         */
        BaseClient.prototype._normalizeEvent = function (event, depth) {
            if (!event) {
                return null;
            }
            // tslint:disable:no-unsafe-any
            return __assign({}, event, (event.breadcrumbs && {
                breadcrumbs: event.breadcrumbs.map(function (b) { return (__assign({}, b, (b.data && {
                    data: normalize(b.data, depth),
                }))); }),
            }), (event.user && {
                user: normalize(event.user, depth),
            }), (event.contexts && {
                contexts: normalize(event.contexts, depth),
            }), (event.extra && {
                extra: normalize(event.extra, depth),
            }));
        };
        /**
         * This function adds all used integrations to the SDK info in the event.
         * @param sdkInfo The sdkInfo of the event that will be filled with all integrations.
         */
        BaseClient.prototype._addIntegrations = function (sdkInfo) {
            var integrationsArray = Object.keys(this._integrations);
            if (sdkInfo && integrationsArray.length > 0) {
                sdkInfo.integrations = integrationsArray;
            }
        };
        /**
         * Processes an event (either error or message) and sends it to Sentry.
         *
         * This also adds breadcrumbs and context information to the event. However,
         * platform specific meta data (such as the User's IP address) must be added
         * by the SDK implementor.
         *
         *
         * @param event The event to send to Sentry.
         * @param hint May contain additional informartion about the original exception.
         * @param scope A scope containing event metadata.
         * @returns A SyncPromise that resolves with the event or rejects in case event was/will not be send.
         */
        BaseClient.prototype._processEvent = function (event, hint, scope) {
            var _this = this;
            var _a = this.getOptions(), beforeSend = _a.beforeSend, sampleRate = _a.sampleRate;
            if (!this._isEnabled()) {
                return SyncPromise.reject('SDK not enabled, will not send event.');
            }
            // 1.0 === 100% events are sent
            // 0.0 === 0% events are sent
            if (typeof sampleRate === 'number' && Math.random() > sampleRate) {
                return SyncPromise.reject('This event has been sampled, will not send event.');
            }
            return new SyncPromise(function (resolve, reject) {
                _this._prepareEvent(event, scope, hint)
                    .then(function (prepared) {
                    if (prepared === null) {
                        reject('An event processor returned null, will not send event.');
                        return;
                    }
                    var finalEvent = prepared;
                    var isInternalException = hint && hint.data && hint.data.__sentry__ === true;
                    if (isInternalException || !beforeSend) {
                        _this._getBackend().sendEvent(finalEvent);
                        resolve(finalEvent);
                        return;
                    }
                    var beforeSendResult = beforeSend(prepared, hint);
                    // tslint:disable-next-line:strict-type-predicates
                    if (typeof beforeSendResult === 'undefined') {
                        logger.error('`beforeSend` method has to return `null` or a valid event.');
                    }
                    else if (isThenable(beforeSendResult)) {
                        _this._handleAsyncBeforeSend(beforeSendResult, resolve, reject);
                    }
                    else {
                        finalEvent = beforeSendResult;
                        if (finalEvent === null) {
                            logger.log('`beforeSend` returned `null`, will not send event.');
                            resolve(null);
                            return;
                        }
                        // From here on we are really async
                        _this._getBackend().sendEvent(finalEvent);
                        resolve(finalEvent);
                    }
                })
                    .then(null, function (reason) {
                    _this.captureException(reason, {
                        data: {
                            __sentry__: true,
                        },
                        originalException: reason,
                    });
                    reject("Event processing pipeline threw an error, original event will not be sent. Details have been sent as a new event.\nReason: " + reason);
                });
            });
        };
        /**
         * Resolves before send Promise and calls resolve/reject on parent SyncPromise.
         */
        BaseClient.prototype._handleAsyncBeforeSend = function (beforeSend, resolve, reject) {
            var _this = this;
            beforeSend
                .then(function (processedEvent) {
                if (processedEvent === null) {
                    reject('`beforeSend` returned `null`, will not send event.');
                    return;
                }
                // From here on we are really async
                _this._getBackend().sendEvent(processedEvent);
                resolve(processedEvent);
            })
                .then(null, function (e) {
                reject("beforeSend rejected with " + e);
            });
        };
        return BaseClient;
    }());

    /** Noop transport */
    var NoopTransport = /** @class */ (function () {
        function NoopTransport() {
        }
        /**
         * @inheritDoc
         */
        NoopTransport.prototype.sendEvent = function (_) {
            return SyncPromise.resolve({
                reason: "NoopTransport: Event has been skipped because no Dsn is configured.",
                status: exports.Status.Skipped,
            });
        };
        /**
         * @inheritDoc
         */
        NoopTransport.prototype.close = function (_) {
            return SyncPromise.resolve(true);
        };
        return NoopTransport;
    }());

    /**
     * This is the base implemention of a Backend.
     * @hidden
     */
    var BaseBackend = /** @class */ (function () {
        /** Creates a new backend instance. */
        function BaseBackend(options) {
            this._options = options;
            if (!this._options.dsn) {
                logger.warn('No DSN provided, backend will not do anything.');
            }
            this._transport = this._setupTransport();
        }
        /**
         * Sets up the transport so it can be used later to send requests.
         */
        BaseBackend.prototype._setupTransport = function () {
            return new NoopTransport();
        };
        /**
         * @inheritDoc
         */
        BaseBackend.prototype.eventFromException = function (_exception, _hint) {
            throw new SentryError('Backend has to implement `eventFromException` method');
        };
        /**
         * @inheritDoc
         */
        BaseBackend.prototype.eventFromMessage = function (_message, _level, _hint) {
            throw new SentryError('Backend has to implement `eventFromMessage` method');
        };
        /**
         * @inheritDoc
         */
        BaseBackend.prototype.sendEvent = function (event) {
            this._transport.sendEvent(event).then(null, function (reason) {
                logger.error("Error while sending event: " + reason);
            });
        };
        /**
         * @inheritDoc
         */
        BaseBackend.prototype.getTransport = function () {
            return this._transport;
        };
        return BaseBackend;
    }());

    /**
     * Internal function to create a new SDK client instance. The client is
     * installed and then bound to the current scope.
     *
     * @param clientClass The client class to instanciate.
     * @param options Options to pass to the client.
     */
    function initAndBind(clientClass, options) {
        if (options.debug === true) {
            logger.enable();
        }
        var hub = getCurrentHub();
        var client = new clientClass(options);
        hub.bindClient(client);
    }

    var originalFunctionToString;
    /** Patch toString calls to return proper name for wrapped functions */
    var FunctionToString = /** @class */ (function () {
        function FunctionToString() {
            /**
             * @inheritDoc
             */
            this.name = FunctionToString.id;
        }
        /**
         * @inheritDoc
         */
        FunctionToString.prototype.setupOnce = function () {
            originalFunctionToString = Function.prototype.toString;
            Function.prototype.toString = function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                var context = this.__sentry_original__ || this;
                // tslint:disable-next-line:no-unsafe-any
                return originalFunctionToString.apply(context, args);
            };
        };
        /**
         * @inheritDoc
         */
        FunctionToString.id = 'FunctionToString';
        return FunctionToString;
    }());

    // "Script error." is hard coded into browsers for errors that it can't read.
    // this is the result of a script being pulled in from an external domain and CORS.
    var DEFAULT_IGNORE_ERRORS = [/^Script error\.?$/, /^Javascript error: Script error\.? on line 0$/];
    /** Inbound filters configurable by the user */
    var InboundFilters = /** @class */ (function () {
        function InboundFilters(_options) {
            if (_options === void 0) { _options = {}; }
            this._options = _options;
            /**
             * @inheritDoc
             */
            this.name = InboundFilters.id;
        }
        /**
         * @inheritDoc
         */
        InboundFilters.prototype.setupOnce = function () {
            addGlobalEventProcessor(function (event) {
                var hub = getCurrentHub();
                if (!hub) {
                    return event;
                }
                var self = hub.getIntegration(InboundFilters);
                if (self) {
                    var client = hub.getClient();
                    var clientOptions = client ? client.getOptions() : {};
                    var options = self._mergeOptions(clientOptions);
                    if (self._shouldDropEvent(event, options)) {
                        return null;
                    }
                }
                return event;
            });
        };
        /** JSDoc */
        InboundFilters.prototype._shouldDropEvent = function (event, options) {
            if (this._isSentryError(event, options)) {
                logger.warn("Event dropped due to being internal Sentry Error.\nEvent: " + getEventDescription(event));
                return true;
            }
            if (this._isIgnoredError(event, options)) {
                logger.warn("Event dropped due to being matched by `ignoreErrors` option.\nEvent: " + getEventDescription(event));
                return true;
            }
            if (this._isBlacklistedUrl(event, options)) {
                logger.warn("Event dropped due to being matched by `blacklistUrls` option.\nEvent: " + getEventDescription(event) + ".\nUrl: " + this._getEventFilterUrl(event));
                return true;
            }
            if (!this._isWhitelistedUrl(event, options)) {
                logger.warn("Event dropped due to not being matched by `whitelistUrls` option.\nEvent: " + getEventDescription(event) + ".\nUrl: " + this._getEventFilterUrl(event));
                return true;
            }
            return false;
        };
        /** JSDoc */
        InboundFilters.prototype._isSentryError = function (event, options) {
            if (options === void 0) { options = {}; }
            if (!options.ignoreInternal) {
                return false;
            }
            try {
                return ((event &&
                    event.exception &&
                    event.exception.values &&
                    event.exception.values[0] &&
                    event.exception.values[0].type === 'SentryError') ||
                    false);
            }
            catch (_oO) {
                return false;
            }
        };
        /** JSDoc */
        InboundFilters.prototype._isIgnoredError = function (event, options) {
            if (options === void 0) { options = {}; }
            if (!options.ignoreErrors || !options.ignoreErrors.length) {
                return false;
            }
            return this._getPossibleEventMessages(event).some(function (message) {
                // Not sure why TypeScript complains here...
                return options.ignoreErrors.some(function (pattern) { return isMatchingPattern(message, pattern); });
            });
        };
        /** JSDoc */
        InboundFilters.prototype._isBlacklistedUrl = function (event, options) {
            if (options === void 0) { options = {}; }
            // TODO: Use Glob instead?
            if (!options.blacklistUrls || !options.blacklistUrls.length) {
                return false;
            }
            var url = this._getEventFilterUrl(event);
            return !url ? false : options.blacklistUrls.some(function (pattern) { return isMatchingPattern(url, pattern); });
        };
        /** JSDoc */
        InboundFilters.prototype._isWhitelistedUrl = function (event, options) {
            if (options === void 0) { options = {}; }
            // TODO: Use Glob instead?
            if (!options.whitelistUrls || !options.whitelistUrls.length) {
                return true;
            }
            var url = this._getEventFilterUrl(event);
            return !url ? true : options.whitelistUrls.some(function (pattern) { return isMatchingPattern(url, pattern); });
        };
        /** JSDoc */
        InboundFilters.prototype._mergeOptions = function (clientOptions) {
            if (clientOptions === void 0) { clientOptions = {}; }
            return {
                blacklistUrls: __spread((this._options.blacklistUrls || []), (clientOptions.blacklistUrls || [])),
                ignoreErrors: __spread((this._options.ignoreErrors || []), (clientOptions.ignoreErrors || []), DEFAULT_IGNORE_ERRORS),
                ignoreInternal: typeof this._options.ignoreInternal !== 'undefined' ? this._options.ignoreInternal : true,
                whitelistUrls: __spread((this._options.whitelistUrls || []), (clientOptions.whitelistUrls || [])),
            };
        };
        /** JSDoc */
        InboundFilters.prototype._getPossibleEventMessages = function (event) {
            if (event.message) {
                return [event.message];
            }
            if (event.exception) {
                try {
                    var _a = (event.exception.values && event.exception.values[0]) || {}, _b = _a.type, type = _b === void 0 ? '' : _b, _c = _a.value, value = _c === void 0 ? '' : _c;
                    return ["" + value, type + ": " + value];
                }
                catch (oO) {
                    logger.error("Cannot extract message for event " + getEventDescription(event));
                    return [];
                }
            }
            return [];
        };
        /** JSDoc */
        InboundFilters.prototype._getEventFilterUrl = function (event) {
            try {
                if (event.stacktrace) {
                    var frames_1 = event.stacktrace.frames;
                    return (frames_1 && frames_1[frames_1.length - 1].filename) || null;
                }
                if (event.exception) {
                    var frames_2 = event.exception.values && event.exception.values[0].stacktrace && event.exception.values[0].stacktrace.frames;
                    return (frames_2 && frames_2[frames_2.length - 1].filename) || null;
                }
                return null;
            }
            catch (oO) {
                logger.error("Cannot extract url for event " + getEventDescription(event));
                return null;
            }
        };
        /**
         * @inheritDoc
         */
        InboundFilters.id = 'InboundFilters';
        return InboundFilters;
    }());



    var CoreIntegrations = /*#__PURE__*/Object.freeze({
        FunctionToString: FunctionToString,
        InboundFilters: InboundFilters
    });

    // tslint:disable:object-literal-sort-keys
    // global reference to slice
    var UNKNOWN_FUNCTION = '?';
    // Chromium based browsers: Chrome, Brave, new Opera, new Edge
    var chrome = /^\s*at (?:(.*?) ?\()?((?:file|https?|blob|chrome-extension|address|native|eval|webpack|<anonymous>|[-a-z]+:|.*bundle|\/).*?)(?::(\d+))?(?::(\d+))?\)?\s*$/i;
    // gecko regex: `(?:bundle|\d+\.js)`: `bundle` is for react native, `\d+\.js` also but specifically for ram bundles because it
    // generates filenames without a prefix like `file://` the filenames in the stacktrace are just 42.js
    // We need this specific case for now because we want no other regex to match.
    var gecko = /^\s*(.*?)(?:\((.*?)\))?(?:^|@)?((?:file|https?|blob|chrome|webpack|resource|moz-extension).*?:\/.*?|\[native code\]|[^@]*(?:bundle|\d+\.js))(?::(\d+))?(?::(\d+))?\s*$/i;
    var winjs = /^\s*at (?:((?:\[object object\])?.+) )?\(?((?:file|ms-appx|https?|webpack|blob):.*?):(\d+)(?::(\d+))?\)?\s*$/i;
    var geckoEval = /(\S+) line (\d+)(?: > eval line \d+)* > eval/i;
    var chromeEval = /\((\S*)(?::(\d+))(?::(\d+))\)/;
    /** JSDoc */
    function computeStackTrace(ex) {
        // tslint:disable:no-unsafe-any
        var stack = null;
        var popSize = ex && ex.framesToPop;
        try {
            // This must be tried first because Opera 10 *destroys*
            // its stacktrace property if you try to access the stack
            // property first!!
            stack = computeStackTraceFromStacktraceProp(ex);
            if (stack) {
                return popFrames(stack, popSize);
            }
        }
        catch (e) {
            // no-empty
        }
        try {
            stack = computeStackTraceFromStackProp(ex);
            if (stack) {
                return popFrames(stack, popSize);
            }
        }
        catch (e) {
            // no-empty
        }
        return {
            message: extractMessage(ex),
            name: ex && ex.name,
            stack: [],
            failed: true,
        };
    }
    /** JSDoc */
    // tslint:disable-next-line:cyclomatic-complexity
    function computeStackTraceFromStackProp(ex) {
        // tslint:disable:no-conditional-assignment
        if (!ex || !ex.stack) {
            return null;
        }
        var stack = [];
        var lines = ex.stack.split('\n');
        var isEval;
        var submatch;
        var parts;
        var element;
        for (var i = 0; i < lines.length; ++i) {
            if ((parts = chrome.exec(lines[i]))) {
                var isNative = parts[2] && parts[2].indexOf('native') === 0; // start of line
                isEval = parts[2] && parts[2].indexOf('eval') === 0; // start of line
                if (isEval && (submatch = chromeEval.exec(parts[2]))) {
                    // throw out eval line/column and use top-most line/column number
                    parts[2] = submatch[1]; // url
                    parts[3] = submatch[2]; // line
                    parts[4] = submatch[3]; // column
                }
                element = {
                    // working with the regexp above is super painful. it is quite a hack, but just stripping the `address at `
                    // prefix here seems like the quickest solution for now.
                    url: parts[2] && parts[2].indexOf('address at ') === 0 ? parts[2].substr('address at '.length) : parts[2],
                    func: parts[1] || UNKNOWN_FUNCTION,
                    args: isNative ? [parts[2]] : [],
                    line: parts[3] ? +parts[3] : null,
                    column: parts[4] ? +parts[4] : null,
                };
            }
            else if ((parts = winjs.exec(lines[i]))) {
                element = {
                    url: parts[2],
                    func: parts[1] || UNKNOWN_FUNCTION,
                    args: [],
                    line: +parts[3],
                    column: parts[4] ? +parts[4] : null,
                };
            }
            else if ((parts = gecko.exec(lines[i]))) {
                isEval = parts[3] && parts[3].indexOf(' > eval') > -1;
                if (isEval && (submatch = geckoEval.exec(parts[3]))) {
                    // throw out eval line/column and use top-most line number
                    parts[1] = parts[1] || "eval";
                    parts[3] = submatch[1];
                    parts[4] = submatch[2];
                    parts[5] = ''; // no column when eval
                }
                else if (i === 0 && !parts[5] && ex.columnNumber !== void 0) {
                    // FireFox uses this awesome columnNumber property for its top frame
                    // Also note, Firefox's column number is 0-based and everything else expects 1-based,
                    // so adding 1
                    // NOTE: this hack doesn't work if top-most frame is eval
                    stack[0].column = ex.columnNumber + 1;
                }
                element = {
                    url: parts[3],
                    func: parts[1] || UNKNOWN_FUNCTION,
                    args: parts[2] ? parts[2].split(',') : [],
                    line: parts[4] ? +parts[4] : null,
                    column: parts[5] ? +parts[5] : null,
                };
            }
            else {
                continue;
            }
            if (!element.func && element.line) {
                element.func = UNKNOWN_FUNCTION;
            }
            stack.push(element);
        }
        if (!stack.length) {
            return null;
        }
        return {
            message: extractMessage(ex),
            name: ex.name,
            stack: stack,
        };
    }
    /** JSDoc */
    function computeStackTraceFromStacktraceProp(ex) {
        if (!ex || !ex.stacktrace) {
            return null;
        }
        // Access and store the stacktrace property before doing ANYTHING
        // else to it because Opera is not very good at providing it
        // reliably in other circumstances.
        var stacktrace = ex.stacktrace;
        var opera10Regex = / line (\d+).*script (?:in )?(\S+)(?:: in function (\S+))?$/i;
        var opera11Regex = / line (\d+), column (\d+)\s*(?:in (?:<anonymous function: ([^>]+)>|([^\)]+))\((.*)\))? in (.*):\s*$/i;
        var lines = stacktrace.split('\n');
        var stack = [];
        var parts;
        for (var line = 0; line < lines.length; line += 2) {
            // tslint:disable:no-conditional-assignment
            var element = null;
            if ((parts = opera10Regex.exec(lines[line]))) {
                element = {
                    url: parts[2],
                    func: parts[3],
                    args: [],
                    line: +parts[1],
                    column: null,
                };
            }
            else if ((parts = opera11Regex.exec(lines[line]))) {
                element = {
                    url: parts[6],
                    func: parts[3] || parts[4],
                    args: parts[5] ? parts[5].split(',') : [],
                    line: +parts[1],
                    column: +parts[2],
                };
            }
            if (element) {
                if (!element.func && element.line) {
                    element.func = UNKNOWN_FUNCTION;
                }
                stack.push(element);
            }
        }
        if (!stack.length) {
            return null;
        }
        return {
            message: extractMessage(ex),
            name: ex.name,
            stack: stack,
        };
    }
    /** Remove N number of frames from the stack */
    function popFrames(stacktrace, popSize) {
        try {
            return __assign({}, stacktrace, { stack: stacktrace.stack.slice(popSize) });
        }
        catch (e) {
            return stacktrace;
        }
    }
    /**
     * There are cases where stacktrace.message is an Event object
     * https://github.com/getsentry/sentry-javascript/issues/1949
     * In this specific case we try to extract stacktrace.message.error.message
     */
    function extractMessage(ex) {
        var message = ex && ex.message;
        if (!message) {
            return 'No error message';
        }
        if (message.error && typeof message.error.message === 'string') {
            return message.error.message;
        }
        return message;
    }

    var STACKTRACE_LIMIT = 50;
    /**
     * This function creates an exception from an TraceKitStackTrace
     * @param stacktrace TraceKitStackTrace that will be converted to an exception
     * @hidden
     */
    function exceptionFromStacktrace(stacktrace) {
        var frames = prepareFramesForEvent(stacktrace.stack);
        var exception = {
            type: stacktrace.name,
            value: stacktrace.message,
        };
        if (frames && frames.length) {
            exception.stacktrace = { frames: frames };
        }
        // tslint:disable-next-line:strict-type-predicates
        if (exception.type === undefined && exception.value === '') {
            exception.value = 'Unrecoverable error caught';
        }
        return exception;
    }
    /**
     * @hidden
     */
    function eventFromPlainObject(exception, syntheticException, rejection) {
        var event = {
            exception: {
                values: [
                    {
                        type: isEvent(exception) ? exception.constructor.name : rejection ? 'UnhandledRejection' : 'Error',
                        value: "Non-Error " + (rejection ? 'promise rejection' : 'exception') + " captured with keys: " + extractExceptionKeysForMessage(exception),
                    },
                ],
            },
            extra: {
                __serialized__: normalizeToSize(exception),
            },
        };
        if (syntheticException) {
            var stacktrace = computeStackTrace(syntheticException);
            var frames_1 = prepareFramesForEvent(stacktrace.stack);
            event.stacktrace = {
                frames: frames_1,
            };
        }
        return event;
    }
    /**
     * @hidden
     */
    function eventFromStacktrace(stacktrace) {
        var exception = exceptionFromStacktrace(stacktrace);
        return {
            exception: {
                values: [exception],
            },
        };
    }
    /**
     * @hidden
     */
    function prepareFramesForEvent(stack) {
        if (!stack || !stack.length) {
            return [];
        }
        var localStack = stack;
        var firstFrameFunction = localStack[0].func || '';
        var lastFrameFunction = localStack[localStack.length - 1].func || '';
        // If stack starts with one of our API calls, remove it (starts, meaning it's the top of the stack - aka last call)
        if (firstFrameFunction.indexOf('captureMessage') !== -1 || firstFrameFunction.indexOf('captureException') !== -1) {
            localStack = localStack.slice(1);
        }
        // If stack ends with one of our internal API calls, remove it (ends, meaning it's the bottom of the stack - aka top-most call)
        if (lastFrameFunction.indexOf('sentryWrapped') !== -1) {
            localStack = localStack.slice(0, -1);
        }
        // The frame where the crash happened, should be the last entry in the array
        return localStack
            .map(function (frame) { return ({
            colno: frame.column === null ? undefined : frame.column,
            filename: frame.url || localStack[0].url,
            function: frame.func || '?',
            in_app: true,
            lineno: frame.line === null ? undefined : frame.line,
        }); })
            .slice(0, STACKTRACE_LIMIT)
            .reverse();
    }

    /** JSDoc */
    function eventFromUnknownInput(exception, syntheticException, options) {
        if (options === void 0) { options = {}; }
        var event;
        if (isErrorEvent(exception) && exception.error) {
            // If it is an ErrorEvent with `error` property, extract it to get actual Error
            var errorEvent = exception;
            exception = errorEvent.error; // tslint:disable-line:no-parameter-reassignment
            event = eventFromStacktrace(computeStackTrace(exception));
            return event;
        }
        if (isDOMError(exception) || isDOMException(exception)) {
            // If it is a DOMError or DOMException (which are legacy APIs, but still supported in some browsers)
            // then we just extract the name and message, as they don't provide anything else
            // https://developer.mozilla.org/en-US/docs/Web/API/DOMError
            // https://developer.mozilla.org/en-US/docs/Web/API/DOMException
            var domException = exception;
            var name_1 = domException.name || (isDOMError(domException) ? 'DOMError' : 'DOMException');
            var message = domException.message ? name_1 + ": " + domException.message : name_1;
            event = eventFromString(message, syntheticException, options);
            addExceptionTypeValue(event, message);
            return event;
        }
        if (isError(exception)) {
            // we have a real Error object, do nothing
            event = eventFromStacktrace(computeStackTrace(exception));
            return event;
        }
        if (isPlainObject(exception) || isEvent(exception)) {
            // If it is plain Object or Event, serialize it manually and extract options
            // This will allow us to group events based on top-level keys
            // which is much better than creating new group when any key/value change
            var objectException = exception;
            event = eventFromPlainObject(objectException, syntheticException, options.rejection);
            addExceptionMechanism(event, {
                synthetic: true,
            });
            return event;
        }
        // If none of previous checks were valid, then it means that it's not:
        // - an instance of DOMError
        // - an instance of DOMException
        // - an instance of Event
        // - an instance of Error
        // - a valid ErrorEvent (one with an error property)
        // - a plain Object
        //
        // So bail out and capture it as a simple message:
        event = eventFromString(exception, syntheticException, options);
        addExceptionTypeValue(event, "" + exception, undefined);
        addExceptionMechanism(event, {
            synthetic: true,
        });
        return event;
    }
    // this._options.attachStacktrace
    /** JSDoc */
    function eventFromString(input, syntheticException, options) {
        if (options === void 0) { options = {}; }
        var event = {
            message: input,
        };
        if (options.attachStacktrace && syntheticException) {
            var stacktrace = computeStackTrace(syntheticException);
            var frames_1 = prepareFramesForEvent(stacktrace.stack);
            event.stacktrace = {
                frames: frames_1,
            };
        }
        return event;
    }

    /** Base Transport class implementation */
    var BaseTransport = /** @class */ (function () {
        function BaseTransport(options) {
            this.options = options;
            /** A simple buffer holding all requests. */
            this._buffer = new PromiseBuffer(30);
            this.url = new API(this.options.dsn).getStoreEndpointWithUrlEncodedAuth();
        }
        /**
         * @inheritDoc
         */
        BaseTransport.prototype.sendEvent = function (_) {
            throw new SentryError('Transport Class has to implement `sendEvent` method');
        };
        /**
         * @inheritDoc
         */
        BaseTransport.prototype.close = function (timeout) {
            return this._buffer.drain(timeout);
        };
        return BaseTransport;
    }());

    var global$3 = getGlobalObject();
    /** `fetch` based transport */
    var FetchTransport = /** @class */ (function (_super) {
        __extends(FetchTransport, _super);
        function FetchTransport() {
            var _this = _super !== null && _super.apply(this, arguments) || this;
            /** Locks transport after receiving 429 response */
            _this._disabledUntil = new Date(Date.now());
            return _this;
        }
        /**
         * @inheritDoc
         */
        FetchTransport.prototype.sendEvent = function (event) {
            var _this = this;
            if (new Date(Date.now()) < this._disabledUntil) {
                return Promise.reject({
                    event: event,
                    reason: "Transport locked till " + this._disabledUntil + " due to too many requests.",
                    status: 429,
                });
            }
            var defaultOptions = {
                body: JSON.stringify(event),
                method: 'POST',
                // Despite all stars in the sky saying that Edge supports old draft syntax, aka 'never', 'always', 'origin' and 'default
                // https://caniuse.com/#feat=referrer-policy
                // It doesn't. And it throw exception instead of ignoring this parameter...
                // REF: https://github.com/getsentry/raven-js/issues/1233
                referrerPolicy: (supportsReferrerPolicy() ? 'origin' : ''),
            };
            if (this.options.headers !== undefined) {
                defaultOptions.headers = this.options.headers;
            }
            return this._buffer.add(new SyncPromise(function (resolve, reject) {
                global$3
                    .fetch(_this.url, defaultOptions)
                    .then(function (response) {
                    var status = exports.Status.fromHttpCode(response.status);
                    if (status === exports.Status.Success) {
                        resolve({ status: status });
                        return;
                    }
                    if (status === exports.Status.RateLimit) {
                        var now = Date.now();
                        _this._disabledUntil = new Date(now + parseRetryAfterHeader(now, response.headers.get('Retry-After')));
                        logger.warn("Too many requests, backing off till: " + _this._disabledUntil);
                    }
                    reject(response);
                })
                    .catch(reject);
            }));
        };
        return FetchTransport;
    }(BaseTransport));

    /** `XHR` based transport */
    var XHRTransport = /** @class */ (function (_super) {
        __extends(XHRTransport, _super);
        function XHRTransport() {
            var _this = _super !== null && _super.apply(this, arguments) || this;
            /** Locks transport after receiving 429 response */
            _this._disabledUntil = new Date(Date.now());
            return _this;
        }
        /**
         * @inheritDoc
         */
        XHRTransport.prototype.sendEvent = function (event) {
            var _this = this;
            if (new Date(Date.now()) < this._disabledUntil) {
                return Promise.reject({
                    event: event,
                    reason: "Transport locked till " + this._disabledUntil + " due to too many requests.",
                    status: 429,
                });
            }
            return this._buffer.add(new SyncPromise(function (resolve, reject) {
                var request = new XMLHttpRequest();
                request.onreadystatechange = function () {
                    if (request.readyState !== 4) {
                        return;
                    }
                    var status = exports.Status.fromHttpCode(request.status);
                    if (status === exports.Status.Success) {
                        resolve({ status: status });
                        return;
                    }
                    if (status === exports.Status.RateLimit) {
                        var now = Date.now();
                        _this._disabledUntil = new Date(now + parseRetryAfterHeader(now, request.getResponseHeader('Retry-After')));
                        logger.warn("Too many requests, backing off till: " + _this._disabledUntil);
                    }
                    reject(request);
                };
                request.open('POST', _this.url);
                for (var header in _this.options.headers) {
                    if (_this.options.headers.hasOwnProperty(header)) {
                        request.setRequestHeader(header, _this.options.headers[header]);
                    }
                }
                request.send(JSON.stringify(event));
            }));
        };
        return XHRTransport;
    }(BaseTransport));



    var index = /*#__PURE__*/Object.freeze({
        BaseTransport: BaseTransport,
        FetchTransport: FetchTransport,
        XHRTransport: XHRTransport
    });

    /**
     * The Sentry Browser SDK Backend.
     * @hidden
     */
    var BrowserBackend = /** @class */ (function (_super) {
        __extends(BrowserBackend, _super);
        function BrowserBackend() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        /**
         * @inheritDoc
         */
        BrowserBackend.prototype._setupTransport = function () {
            if (!this._options.dsn) {
                // We return the noop transport here in case there is no Dsn.
                return _super.prototype._setupTransport.call(this);
            }
            var transportOptions = __assign({}, this._options.transportOptions, { dsn: this._options.dsn });
            if (this._options.transport) {
                return new this._options.transport(transportOptions);
            }
            if (supportsFetch()) {
                return new FetchTransport(transportOptions);
            }
            return new XHRTransport(transportOptions);
        };
        /**
         * @inheritDoc
         */
        BrowserBackend.prototype.eventFromException = function (exception, hint) {
            var syntheticException = (hint && hint.syntheticException) || undefined;
            var event = eventFromUnknownInput(exception, syntheticException, {
                attachStacktrace: this._options.attachStacktrace,
            });
            addExceptionMechanism(event, {
                handled: true,
                type: 'generic',
            });
            event.level = exports.Severity.Error;
            if (hint && hint.event_id) {
                event.event_id = hint.event_id;
            }
            return SyncPromise.resolve(event);
        };
        /**
         * @inheritDoc
         */
        BrowserBackend.prototype.eventFromMessage = function (message, level, hint) {
            if (level === void 0) { level = exports.Severity.Info; }
            var syntheticException = (hint && hint.syntheticException) || undefined;
            var event = eventFromString(message, syntheticException, {
                attachStacktrace: this._options.attachStacktrace,
            });
            event.level = level;
            if (hint && hint.event_id) {
                event.event_id = hint.event_id;
            }
            return SyncPromise.resolve(event);
        };
        return BrowserBackend;
    }(BaseBackend));

    var SDK_NAME = 'sentry.javascript.browser';
    var SDK_VERSION = '5.15.5';

    /**
     * The Sentry Browser SDK Client.
     *
     * @see BrowserOptions for documentation on configuration options.
     * @see SentryClient for usage documentation.
     */
    var BrowserClient = /** @class */ (function (_super) {
        __extends(BrowserClient, _super);
        /**
         * Creates a new Browser SDK instance.
         *
         * @param options Configuration options for this SDK.
         */
        function BrowserClient(options) {
            if (options === void 0) { options = {}; }
            return _super.call(this, BrowserBackend, options) || this;
        }
        /**
         * @inheritDoc
         */
        BrowserClient.prototype._prepareEvent = function (event, scope, hint) {
            event.platform = event.platform || 'javascript';
            event.sdk = __assign({}, event.sdk, { name: SDK_NAME, packages: __spread(((event.sdk && event.sdk.packages) || []), [
                    {
                        name: 'npm:@sentry/browser',
                        version: SDK_VERSION,
                    },
                ]), version: SDK_VERSION });
            return _super.prototype._prepareEvent.call(this, event, scope, hint);
        };
        /**
         * Show a report dialog to the user to send feedback to a specific event.
         *
         * @param options Set individual options for the dialog
         */
        BrowserClient.prototype.showReportDialog = function (options) {
            if (options === void 0) { options = {}; }
            // doesn't work without a document (React Native)
            var document = getGlobalObject().document;
            if (!document) {
                return;
            }
            if (!this._isEnabled()) {
                logger.error('Trying to call showReportDialog with Sentry Client is disabled');
                return;
            }
            var dsn = options.dsn || this.getDsn();
            if (!options.eventId) {
                logger.error('Missing `eventId` option in showReportDialog call');
                return;
            }
            if (!dsn) {
                logger.error('Missing `Dsn` option in showReportDialog call');
                return;
            }
            var script = document.createElement('script');
            script.async = true;
            script.src = new API(dsn).getReportDialogEndpoint(options);
            if (options.onLoad) {
                script.onload = options.onLoad;
            }
            (document.head || document.body).appendChild(script);
        };
        return BrowserClient;
    }(BaseClient));

    var ignoreOnError = 0;
    /**
     * @hidden
     */
    function shouldIgnoreOnError() {
        return ignoreOnError > 0;
    }
    /**
     * @hidden
     */
    function ignoreNextOnError() {
        // onerror should trigger before setTimeout
        ignoreOnError += 1;
        setTimeout(function () {
            ignoreOnError -= 1;
        });
    }
    /**
     * Instruments the given function and sends an event to Sentry every time the
     * function throws an exception.
     *
     * @param fn A function to wrap.
     * @returns The wrapped function.
     * @hidden
     */
    function wrap(fn, options, before) {
        if (options === void 0) { options = {}; }
        // tslint:disable-next-line:strict-type-predicates
        if (typeof fn !== 'function') {
            return fn;
        }
        try {
            // We don't wanna wrap it twice
            if (fn.__sentry__) {
                return fn;
            }
            // If this has already been wrapped in the past, return that wrapped function
            if (fn.__sentry_wrapped__) {
                return fn.__sentry_wrapped__;
            }
        }
        catch (e) {
            // Just accessing custom props in some Selenium environments
            // can cause a "Permission denied" exception (see raven-js#495).
            // Bail on wrapping and return the function as-is (defers to window.onerror).
            return fn;
        }
        var sentryWrapped = function () {
            var args = Array.prototype.slice.call(arguments);
            // tslint:disable:no-unsafe-any
            try {
                // tslint:disable-next-line:strict-type-predicates
                if (before && typeof before === 'function') {
                    before.apply(this, arguments);
                }
                var wrappedArguments = args.map(function (arg) { return wrap(arg, options); });
                if (fn.handleEvent) {
                    // Attempt to invoke user-land function
                    // NOTE: If you are a Sentry user, and you are seeing this stack frame, it
                    //       means the sentry.javascript SDK caught an error invoking your application code. This
                    //       is expected behavior and NOT indicative of a bug with sentry.javascript.
                    return fn.handleEvent.apply(this, wrappedArguments);
                }
                // Attempt to invoke user-land function
                // NOTE: If you are a Sentry user, and you are seeing this stack frame, it
                //       means the sentry.javascript SDK caught an error invoking your application code. This
                //       is expected behavior and NOT indicative of a bug with sentry.javascript.
                return fn.apply(this, wrappedArguments);
                // tslint:enable:no-unsafe-any
            }
            catch (ex) {
                ignoreNextOnError();
                withScope(function (scope) {
                    scope.addEventProcessor(function (event) {
                        var processedEvent = __assign({}, event);
                        if (options.mechanism) {
                            addExceptionTypeValue(processedEvent, undefined, undefined);
                            addExceptionMechanism(processedEvent, options.mechanism);
                        }
                        processedEvent.extra = __assign({}, processedEvent.extra, { arguments: args });
                        return processedEvent;
                    });
                    captureException(ex);
                });
                throw ex;
            }
        };
        // Accessing some objects may throw
        // ref: https://github.com/getsentry/sentry-javascript/issues/1168
        try {
            for (var property in fn) {
                if (Object.prototype.hasOwnProperty.call(fn, property)) {
                    sentryWrapped[property] = fn[property];
                }
            }
        }
        catch (_oO) { } // tslint:disable-line:no-empty
        fn.prototype = fn.prototype || {};
        sentryWrapped.prototype = fn.prototype;
        Object.defineProperty(fn, '__sentry_wrapped__', {
            enumerable: false,
            value: sentryWrapped,
        });
        // Signal that this function has been wrapped/filled already
        // for both debugging and to prevent it to being wrapped/filled twice
        Object.defineProperties(sentryWrapped, {
            __sentry__: {
                enumerable: false,
                value: true,
            },
            __sentry_original__: {
                enumerable: false,
                value: fn,
            },
        });
        // Restore original function name (not all browsers allow that)
        try {
            var descriptor = Object.getOwnPropertyDescriptor(sentryWrapped, 'name');
            if (descriptor.configurable) {
                Object.defineProperty(sentryWrapped, 'name', {
                    get: function () {
                        return fn.name;
                    },
                });
            }
        }
        catch (_oO) {
            /*no-empty*/
        }
        return sentryWrapped;
    }

    /** Global handlers */
    var GlobalHandlers = /** @class */ (function () {
        /** JSDoc */
        function GlobalHandlers(options) {
            /**
             * @inheritDoc
             */
            this.name = GlobalHandlers.id;
            /** JSDoc */
            this._onErrorHandlerInstalled = false;
            /** JSDoc */
            this._onUnhandledRejectionHandlerInstalled = false;
            this._options = __assign({ onerror: true, onunhandledrejection: true }, options);
        }
        /**
         * @inheritDoc
         */
        GlobalHandlers.prototype.setupOnce = function () {
            Error.stackTraceLimit = 50;
            if (this._options.onerror) {
                logger.log('Global Handler attached: onerror');
                this._installGlobalOnErrorHandler();
            }
            if (this._options.onunhandledrejection) {
                logger.log('Global Handler attached: onunhandledrejection');
                this._installGlobalOnUnhandledRejectionHandler();
            }
        };
        /** JSDoc */
        GlobalHandlers.prototype._installGlobalOnErrorHandler = function () {
            var _this = this;
            if (this._onErrorHandlerInstalled) {
                return;
            }
            addInstrumentationHandler({
                callback: function (data) {
                    var error = data.error;
                    var currentHub = getCurrentHub();
                    var hasIntegration = currentHub.getIntegration(GlobalHandlers);
                    var isFailedOwnDelivery = error && error.__sentry_own_request__ === true;
                    if (!hasIntegration || shouldIgnoreOnError() || isFailedOwnDelivery) {
                        return;
                    }
                    var client = currentHub.getClient();
                    var event = isPrimitive(error)
                        ? _this._eventFromIncompleteOnError(data.msg, data.url, data.line, data.column)
                        : _this._enhanceEventWithInitialFrame(eventFromUnknownInput(error, undefined, {
                            attachStacktrace: client && client.getOptions().attachStacktrace,
                            rejection: false,
                        }), data.url, data.line, data.column);
                    addExceptionMechanism(event, {
                        handled: false,
                        type: 'onerror',
                    });
                    currentHub.captureEvent(event, {
                        originalException: error,
                    });
                },
                type: 'error',
            });
            this._onErrorHandlerInstalled = true;
        };
        /** JSDoc */
        GlobalHandlers.prototype._installGlobalOnUnhandledRejectionHandler = function () {
            var _this = this;
            if (this._onUnhandledRejectionHandlerInstalled) {
                return;
            }
            addInstrumentationHandler({
                callback: function (e) {
                    var error = e;
                    // dig the object of the rejection out of known event types
                    try {
                        // PromiseRejectionEvents store the object of the rejection under 'reason'
                        // see https://developer.mozilla.org/en-US/docs/Web/API/PromiseRejectionEvent
                        if ('reason' in e) {
                            error = e.reason;
                        }
                        // something, somewhere, (likely a browser extension) effectively casts PromiseRejectionEvents
                        // to CustomEvents, moving the `promise` and `reason` attributes of the PRE into
                        // the CustomEvent's `detail` attribute, since they're not part of CustomEvent's spec
                        // see https://developer.mozilla.org/en-US/docs/Web/API/CustomEvent and
                        // https://github.com/getsentry/sentry-javascript/issues/2380
                        else if ('detail' in e && 'reason' in e.detail) {
                            error = e.detail.reason;
                        }
                    }
                    catch (_oO) {
                        // no-empty
                    }
                    var currentHub = getCurrentHub();
                    var hasIntegration = currentHub.getIntegration(GlobalHandlers);
                    var isFailedOwnDelivery = error && error.__sentry_own_request__ === true;
                    if (!hasIntegration || shouldIgnoreOnError() || isFailedOwnDelivery) {
                        return true;
                    }
                    var client = currentHub.getClient();
                    var event = isPrimitive(error)
                        ? _this._eventFromIncompleteRejection(error)
                        : eventFromUnknownInput(error, undefined, {
                            attachStacktrace: client && client.getOptions().attachStacktrace,
                            rejection: true,
                        });
                    event.level = exports.Severity.Error;
                    addExceptionMechanism(event, {
                        handled: false,
                        type: 'onunhandledrejection',
                    });
                    currentHub.captureEvent(event, {
                        originalException: error,
                    });
                    return;
                },
                type: 'unhandledrejection',
            });
            this._onUnhandledRejectionHandlerInstalled = true;
        };
        /**
         * This function creates a stack from an old, error-less onerror handler.
         */
        GlobalHandlers.prototype._eventFromIncompleteOnError = function (msg, url, line, column) {
            var ERROR_TYPES_RE = /^(?:[Uu]ncaught (?:exception: )?)?(?:((?:Eval|Internal|Range|Reference|Syntax|Type|URI|)Error): )?(.*)$/i;
            // If 'message' is ErrorEvent, get real message from inside
            var message = isErrorEvent(msg) ? msg.message : msg;
            var name;
            if (isString(message)) {
                var groups = message.match(ERROR_TYPES_RE);
                if (groups) {
                    name = groups[1];
                    message = groups[2];
                }
            }
            var event = {
                exception: {
                    values: [
                        {
                            type: name || 'Error',
                            value: message,
                        },
                    ],
                },
            };
            return this._enhanceEventWithInitialFrame(event, url, line, column);
        };
        /**
         * This function creates an Event from an TraceKitStackTrace that has part of it missing.
         */
        GlobalHandlers.prototype._eventFromIncompleteRejection = function (error) {
            return {
                exception: {
                    values: [
                        {
                            type: 'UnhandledRejection',
                            value: "Non-Error promise rejection captured with value: " + error,
                        },
                    ],
                },
            };
        };
        /** JSDoc */
        GlobalHandlers.prototype._enhanceEventWithInitialFrame = function (event, url, line, column) {
            event.exception = event.exception || {};
            event.exception.values = event.exception.values || [];
            event.exception.values[0] = event.exception.values[0] || {};
            event.exception.values[0].stacktrace = event.exception.values[0].stacktrace || {};
            event.exception.values[0].stacktrace.frames = event.exception.values[0].stacktrace.frames || [];
            var colno = isNaN(parseInt(column, 10)) ? undefined : column;
            var lineno = isNaN(parseInt(line, 10)) ? undefined : line;
            var filename = isString(url) && url.length > 0 ? url : getLocationHref();
            if (event.exception.values[0].stacktrace.frames.length === 0) {
                event.exception.values[0].stacktrace.frames.push({
                    colno: colno,
                    filename: filename,
                    function: '?',
                    in_app: true,
                    lineno: lineno,
                });
            }
            return event;
        };
        /**
         * @inheritDoc
         */
        GlobalHandlers.id = 'GlobalHandlers';
        return GlobalHandlers;
    }());

    /** Wrap timer functions and event targets to catch errors and provide better meta data */
    var TryCatch = /** @class */ (function () {
        function TryCatch() {
            /** JSDoc */
            this._ignoreOnError = 0;
            /**
             * @inheritDoc
             */
            this.name = TryCatch.id;
        }
        /** JSDoc */
        TryCatch.prototype._wrapTimeFunction = function (original) {
            return function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                var originalCallback = args[0];
                args[0] = wrap(originalCallback, {
                    mechanism: {
                        data: { function: getFunctionName(original) },
                        handled: true,
                        type: 'instrument',
                    },
                });
                return original.apply(this, args);
            };
        };
        /** JSDoc */
        TryCatch.prototype._wrapRAF = function (original) {
            return function (callback) {
                return original(wrap(callback, {
                    mechanism: {
                        data: {
                            function: 'requestAnimationFrame',
                            handler: getFunctionName(original),
                        },
                        handled: true,
                        type: 'instrument',
                    },
                }));
            };
        };
        /** JSDoc */
        TryCatch.prototype._wrapEventTarget = function (target) {
            var global = getGlobalObject();
            var proto = global[target] && global[target].prototype;
            if (!proto || !proto.hasOwnProperty || !proto.hasOwnProperty('addEventListener')) {
                return;
            }
            fill(proto, 'addEventListener', function (original) {
                return function (eventName, fn, options) {
                    try {
                        // tslint:disable-next-line:no-unbound-method strict-type-predicates
                        if (typeof fn.handleEvent === 'function') {
                            fn.handleEvent = wrap(fn.handleEvent.bind(fn), {
                                mechanism: {
                                    data: {
                                        function: 'handleEvent',
                                        handler: getFunctionName(fn),
                                        target: target,
                                    },
                                    handled: true,
                                    type: 'instrument',
                                },
                            });
                        }
                    }
                    catch (err) {
                        // can sometimes get 'Permission denied to access property "handle Event'
                    }
                    return original.call(this, eventName, wrap(fn, {
                        mechanism: {
                            data: {
                                function: 'addEventListener',
                                handler: getFunctionName(fn),
                                target: target,
                            },
                            handled: true,
                            type: 'instrument',
                        },
                    }), options);
                };
            });
            fill(proto, 'removeEventListener', function (original) {
                return function (eventName, fn, options) {
                    var callback = fn;
                    try {
                        callback = callback && (callback.__sentry_wrapped__ || callback);
                    }
                    catch (e) {
                        // ignore, accessing __sentry_wrapped__ will throw in some Selenium environments
                    }
                    return original.call(this, eventName, callback, options);
                };
            });
        };
        /** JSDoc */
        TryCatch.prototype._wrapXHR = function (originalSend) {
            return function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                var xhr = this; // tslint:disable-line:no-this-assignment
                var xmlHttpRequestProps = ['onload', 'onerror', 'onprogress', 'onreadystatechange'];
                xmlHttpRequestProps.forEach(function (prop) {
                    if (prop in xhr && typeof xhr[prop] === 'function') {
                        fill(xhr, prop, function (original) {
                            var wrapOptions = {
                                mechanism: {
                                    data: {
                                        function: prop,
                                        handler: getFunctionName(original),
                                    },
                                    handled: true,
                                    type: 'instrument',
                                },
                            };
                            // If Instrument integration has been called before TryCatch, get the name of original function
                            if (original.__sentry_original__) {
                                wrapOptions.mechanism.data.handler = getFunctionName(original.__sentry_original__);
                            }
                            // Otherwise wrap directly
                            return wrap(original, wrapOptions);
                        });
                    }
                });
                return originalSend.apply(this, args);
            };
        };
        /**
         * Wrap timer functions and event targets to catch errors
         * and provide better metadata.
         */
        TryCatch.prototype.setupOnce = function () {
            this._ignoreOnError = this._ignoreOnError;
            var global = getGlobalObject();
            fill(global, 'setTimeout', this._wrapTimeFunction.bind(this));
            fill(global, 'setInterval', this._wrapTimeFunction.bind(this));
            fill(global, 'requestAnimationFrame', this._wrapRAF.bind(this));
            if ('XMLHttpRequest' in global) {
                fill(XMLHttpRequest.prototype, 'send', this._wrapXHR.bind(this));
            }
            [
                'EventTarget',
                'Window',
                'Node',
                'ApplicationCache',
                'AudioTrackList',
                'ChannelMergerNode',
                'CryptoOperation',
                'EventSource',
                'FileReader',
                'HTMLUnknownElement',
                'IDBDatabase',
                'IDBRequest',
                'IDBTransaction',
                'KeyOperation',
                'MediaController',
                'MessagePort',
                'ModalWindow',
                'Notification',
                'SVGElementInstance',
                'Screen',
                'TextTrack',
                'TextTrackCue',
                'TextTrackList',
                'WebSocket',
                'WebSocketWorker',
                'Worker',
                'XMLHttpRequest',
                'XMLHttpRequestEventTarget',
                'XMLHttpRequestUpload',
            ].forEach(this._wrapEventTarget.bind(this));
        };
        /**
         * @inheritDoc
         */
        TryCatch.id = 'TryCatch';
        return TryCatch;
    }());

    /**
     * Default Breadcrumbs instrumentations
     * TODO: Deprecated - with v6, this will be renamed to `Instrument`
     */
    var Breadcrumbs = /** @class */ (function () {
        /**
         * @inheritDoc
         */
        function Breadcrumbs(options) {
            /**
             * @inheritDoc
             */
            this.name = Breadcrumbs.id;
            this._options = __assign({ console: true, dom: true, fetch: true, history: true, sentry: true, xhr: true }, options);
        }
        /**
         * Creates breadcrumbs from console API calls
         */
        Breadcrumbs.prototype._consoleBreadcrumb = function (handlerData) {
            var breadcrumb = {
                category: 'console',
                data: {
                    arguments: handlerData.args,
                    logger: 'console',
                },
                level: exports.Severity.fromString(handlerData.level),
                message: safeJoin(handlerData.args, ' '),
            };
            if (handlerData.level === 'assert') {
                if (handlerData.args[0] === false) {
                    breadcrumb.message = "Assertion failed: " + (safeJoin(handlerData.args.slice(1), ' ') || 'console.assert');
                    breadcrumb.data.arguments = handlerData.args.slice(1);
                }
                else {
                    // Don't capture a breadcrumb for passed assertions
                    return;
                }
            }
            getCurrentHub().addBreadcrumb(breadcrumb, {
                input: handlerData.args,
                level: handlerData.level,
            });
        };
        /**
         * Creates breadcrumbs from DOM API calls
         */
        Breadcrumbs.prototype._domBreadcrumb = function (handlerData) {
            var target;
            // Accessing event.target can throw (see getsentry/raven-js#838, #768)
            try {
                target = handlerData.event.target
                    ? htmlTreeAsString(handlerData.event.target)
                    : htmlTreeAsString(handlerData.event);
            }
            catch (e) {
                target = '<unknown>';
            }
            if (target.length === 0) {
                return;
            }
            getCurrentHub().addBreadcrumb({
                category: "ui." + handlerData.name,
                message: target,
            }, {
                event: handlerData.event,
                name: handlerData.name,
            });
        };
        /**
         * Creates breadcrumbs from XHR API calls
         */
        Breadcrumbs.prototype._xhrBreadcrumb = function (handlerData) {
            if (handlerData.endTimestamp) {
                // We only capture complete, non-sentry requests
                if (handlerData.xhr.__sentry_own_request__) {
                    return;
                }
                getCurrentHub().addBreadcrumb({
                    category: 'xhr',
                    data: handlerData.xhr.__sentry_xhr__,
                    type: 'http',
                }, {
                    xhr: handlerData.xhr,
                });
                return;
            }
            // We only capture issued sentry requests
            if (this._options.sentry && handlerData.xhr.__sentry_own_request__) {
                addSentryBreadcrumb(handlerData.args[0]);
            }
        };
        /**
         * Creates breadcrumbs from fetch API calls
         */
        Breadcrumbs.prototype._fetchBreadcrumb = function (handlerData) {
            // We only capture complete fetch requests
            if (!handlerData.endTimestamp) {
                return;
            }
            var client = getCurrentHub().getClient();
            var dsn = client && client.getDsn();
            if (this._options.sentry && dsn) {
                var filterUrl = new API(dsn).getStoreEndpoint();
                // if Sentry key appears in URL, don't capture it as a request
                // but rather as our own 'sentry' type breadcrumb
                if (filterUrl &&
                    handlerData.fetchData.url.indexOf(filterUrl) !== -1 &&
                    handlerData.fetchData.method === 'POST' &&
                    handlerData.args[1] &&
                    handlerData.args[1].body) {
                    addSentryBreadcrumb(handlerData.args[1].body);
                    return;
                }
            }
            if (handlerData.error) {
                getCurrentHub().addBreadcrumb({
                    category: 'fetch',
                    data: __assign({}, handlerData.fetchData, { status_code: handlerData.response.status }),
                    level: exports.Severity.Error,
                    type: 'http',
                }, {
                    data: handlerData.error,
                    input: handlerData.args,
                });
            }
            else {
                getCurrentHub().addBreadcrumb({
                    category: 'fetch',
                    data: __assign({}, handlerData.fetchData, { status_code: handlerData.response.status }),
                    type: 'http',
                }, {
                    input: handlerData.args,
                    response: handlerData.response,
                });
            }
        };
        /**
         * Creates breadcrumbs from history API calls
         */
        Breadcrumbs.prototype._historyBreadcrumb = function (handlerData) {
            var global = getGlobalObject();
            var from = handlerData.from;
            var to = handlerData.to;
            var parsedLoc = parseUrl(global.location.href);
            var parsedFrom = parseUrl(from);
            var parsedTo = parseUrl(to);
            // Initial pushState doesn't provide `from` information
            if (!parsedFrom.path) {
                parsedFrom = parsedLoc;
            }
            // Use only the path component of the URL if the URL matches the current
            // document (almost all the time when using pushState)
            if (parsedLoc.protocol === parsedTo.protocol && parsedLoc.host === parsedTo.host) {
                // tslint:disable-next-line:no-parameter-reassignment
                to = parsedTo.relative;
            }
            if (parsedLoc.protocol === parsedFrom.protocol && parsedLoc.host === parsedFrom.host) {
                // tslint:disable-next-line:no-parameter-reassignment
                from = parsedFrom.relative;
            }
            getCurrentHub().addBreadcrumb({
                category: 'navigation',
                data: {
                    from: from,
                    to: to,
                },
            });
        };
        /**
         * Instrument browser built-ins w/ breadcrumb capturing
         *  - Console API
         *  - DOM API (click/typing)
         *  - XMLHttpRequest API
         *  - Fetch API
         *  - History API
         */
        Breadcrumbs.prototype.setupOnce = function () {
            var _this = this;
            if (this._options.console) {
                addInstrumentationHandler({
                    callback: function () {
                        var args = [];
                        for (var _i = 0; _i < arguments.length; _i++) {
                            args[_i] = arguments[_i];
                        }
                        _this._consoleBreadcrumb.apply(_this, __spread(args));
                    },
                    type: 'console',
                });
            }
            if (this._options.dom) {
                addInstrumentationHandler({
                    callback: function () {
                        var args = [];
                        for (var _i = 0; _i < arguments.length; _i++) {
                            args[_i] = arguments[_i];
                        }
                        _this._domBreadcrumb.apply(_this, __spread(args));
                    },
                    type: 'dom',
                });
            }
            if (this._options.xhr) {
                addInstrumentationHandler({
                    callback: function () {
                        var args = [];
                        for (var _i = 0; _i < arguments.length; _i++) {
                            args[_i] = arguments[_i];
                        }
                        _this._xhrBreadcrumb.apply(_this, __spread(args));
                    },
                    type: 'xhr',
                });
            }
            if (this._options.fetch) {
                addInstrumentationHandler({
                    callback: function () {
                        var args = [];
                        for (var _i = 0; _i < arguments.length; _i++) {
                            args[_i] = arguments[_i];
                        }
                        _this._fetchBreadcrumb.apply(_this, __spread(args));
                    },
                    type: 'fetch',
                });
            }
            if (this._options.history) {
                addInstrumentationHandler({
                    callback: function () {
                        var args = [];
                        for (var _i = 0; _i < arguments.length; _i++) {
                            args[_i] = arguments[_i];
                        }
                        _this._historyBreadcrumb.apply(_this, __spread(args));
                    },
                    type: 'history',
                });
            }
        };
        /**
         * @inheritDoc
         */
        Breadcrumbs.id = 'Breadcrumbs';
        return Breadcrumbs;
    }());
    /**
     * Create a breadcrumb of `sentry` from the events themselves
     */
    function addSentryBreadcrumb(serializedData) {
        // There's always something that can go wrong with deserialization...
        try {
            var event_1 = JSON.parse(serializedData);
            getCurrentHub().addBreadcrumb({
                category: "sentry." + (event_1.type === 'transaction' ? 'transaction' : 'event'),
                event_id: event_1.event_id,
                level: event_1.level || exports.Severity.fromString('error'),
                message: getEventDescription(event_1),
            }, {
                event: event_1,
            });
        }
        catch (_oO) {
            logger.error('Error while adding sentry type breadcrumb');
        }
    }

    var DEFAULT_KEY = 'cause';
    var DEFAULT_LIMIT = 5;
    /** Adds SDK info to an event. */
    var LinkedErrors = /** @class */ (function () {
        /**
         * @inheritDoc
         */
        function LinkedErrors(options) {
            if (options === void 0) { options = {}; }
            /**
             * @inheritDoc
             */
            this.name = LinkedErrors.id;
            this._key = options.key || DEFAULT_KEY;
            this._limit = options.limit || DEFAULT_LIMIT;
        }
        /**
         * @inheritDoc
         */
        LinkedErrors.prototype.setupOnce = function () {
            addGlobalEventProcessor(function (event, hint) {
                var self = getCurrentHub().getIntegration(LinkedErrors);
                if (self) {
                    return self._handler(event, hint);
                }
                return event;
            });
        };
        /**
         * @inheritDoc
         */
        LinkedErrors.prototype._handler = function (event, hint) {
            if (!event.exception || !event.exception.values || !hint || !isInstanceOf(hint.originalException, Error)) {
                return event;
            }
            var linkedErrors = this._walkErrorTree(hint.originalException, this._key);
            event.exception.values = __spread(linkedErrors, event.exception.values);
            return event;
        };
        /**
         * @inheritDoc
         */
        LinkedErrors.prototype._walkErrorTree = function (error, key, stack) {
            if (stack === void 0) { stack = []; }
            if (!isInstanceOf(error[key], Error) || stack.length + 1 >= this._limit) {
                return stack;
            }
            var stacktrace = computeStackTrace(error[key]);
            var exception = exceptionFromStacktrace(stacktrace);
            return this._walkErrorTree(error[key], key, __spread([exception], stack));
        };
        /**
         * @inheritDoc
         */
        LinkedErrors.id = 'LinkedErrors';
        return LinkedErrors;
    }());

    var global$4 = getGlobalObject();
    /** UserAgent */
    var UserAgent = /** @class */ (function () {
        function UserAgent() {
            /**
             * @inheritDoc
             */
            this.name = UserAgent.id;
        }
        /**
         * @inheritDoc
         */
        UserAgent.prototype.setupOnce = function () {
            addGlobalEventProcessor(function (event) {
                if (getCurrentHub().getIntegration(UserAgent)) {
                    if (!global$4.navigator || !global$4.location) {
                        return event;
                    }
                    // Request Interface: https://docs.sentry.io/development/sdk-dev/event-payloads/request/
                    var request = event.request || {};
                    request.url = request.url || global$4.location.href;
                    request.headers = request.headers || {};
                    request.headers['User-Agent'] = global$4.navigator.userAgent;
                    return __assign({}, event, { request: request });
                }
                return event;
            });
        };
        /**
         * @inheritDoc
         */
        UserAgent.id = 'UserAgent';
        return UserAgent;
    }());



    var BrowserIntegrations = /*#__PURE__*/Object.freeze({
        GlobalHandlers: GlobalHandlers,
        TryCatch: TryCatch,
        Breadcrumbs: Breadcrumbs,
        LinkedErrors: LinkedErrors,
        UserAgent: UserAgent
    });

    var defaultIntegrations = [
        new InboundFilters(),
        new FunctionToString(),
        new TryCatch(),
        new Breadcrumbs(),
        new GlobalHandlers(),
        new LinkedErrors(),
        new UserAgent(),
    ];
    /**
     * The Sentry Browser SDK Client.
     *
     * To use this SDK, call the {@link init} function as early as possible when
     * loading the web page. To set context information or send manual events, use
     * the provided methods.
     *
     * @example
     *
     * ```
     *
     * import { init } from '@sentry/browser';
     *
     * init({
     *   dsn: '__DSN__',
     *   // ...
     * });
     * ```
     *
     * @example
     * ```
     *
     * import { configureScope } from '@sentry/browser';
     * configureScope((scope: Scope) => {
     *   scope.setExtra({ battery: 0.7 });
     *   scope.setTag({ user_mode: 'admin' });
     *   scope.setUser({ id: '4711' });
     * });
     * ```
     *
     * @example
     * ```
     *
     * import { addBreadcrumb } from '@sentry/browser';
     * addBreadcrumb({
     *   message: 'My Breadcrumb',
     *   // ...
     * });
     * ```
     *
     * @example
     *
     * ```
     *
     * import * as Sentry from '@sentry/browser';
     * Sentry.captureMessage('Hello, world!');
     * Sentry.captureException(new Error('Good bye'));
     * Sentry.captureEvent({
     *   message: 'Manual',
     *   stacktrace: [
     *     // ...
     *   ],
     * });
     * ```
     *
     * @see {@link BrowserOptions} for documentation on configuration options.
     */
    function init(options) {
        if (options === void 0) { options = {}; }
        if (options.defaultIntegrations === undefined) {
            options.defaultIntegrations = defaultIntegrations;
        }
        if (options.release === undefined) {
            var window_1 = getGlobalObject();
            // This supports the variable that sentry-webpack-plugin injects
            if (window_1.SENTRY_RELEASE && window_1.SENTRY_RELEASE.id) {
                options.release = window_1.SENTRY_RELEASE.id;
            }
        }
        initAndBind(BrowserClient, options);
    }
    /**
     * Present the user with a report dialog.
     *
     * @param options Everything is optional, we try to fetch all info need from the global scope.
     */
    function showReportDialog(options) {
        if (options === void 0) { options = {}; }
        if (!options.eventId) {
            options.eventId = getCurrentHub().lastEventId();
        }
        var client = getCurrentHub().getClient();
        if (client) {
            client.showReportDialog(options);
        }
    }
    /**
     * This is the getter for lastEventId.
     *
     * @returns The last event id of a captured event.
     */
    function lastEventId() {
        return getCurrentHub().lastEventId();
    }
    /**
     * This function is here to be API compatible with the loader.
     * @hidden
     */
    function forceLoad() {
        // Noop
    }
    /**
     * This function is here to be API compatible with the loader.
     * @hidden
     */
    function onLoad(callback) {
        callback();
    }
    /**
     * A promise that resolves when all current events have been sent.
     * If you provide a timeout and the queue takes longer to drain the promise returns false.
     *
     * @param timeout Maximum time in ms the client should wait.
     */
    function flush(timeout) {
        var client = getCurrentHub().getClient();
        if (client) {
            return client.flush(timeout);
        }
        return SyncPromise.reject(false);
    }
    /**
     * A promise that resolves when all current events have been sent.
     * If you provide a timeout and the queue takes longer to drain the promise returns false.
     *
     * @param timeout Maximum time in ms the client should wait.
     */
    function close(timeout) {
        var client = getCurrentHub().getClient();
        if (client) {
            return client.close(timeout);
        }
        return SyncPromise.reject(false);
    }
    /**
     * Wrap code within a try/catch block so the SDK is able to capture errors.
     *
     * @param fn A function to wrap.
     *
     * @returns The result of wrapped function call.
     */
    function wrap$1(fn) {
        return wrap(fn)(); // tslint:disable-line:no-unsafe-any
    }

    var windowIntegrations = {};
    // This block is needed to add compatibility with the integrations packages when used with a CDN
    // tslint:disable: no-unsafe-any
    var _window = getGlobalObject();
    if (_window.Sentry && _window.Sentry.Integrations) {
        windowIntegrations = _window.Sentry.Integrations;
    }
    // tslint:enable: no-unsafe-any
    var INTEGRATIONS = __assign({}, windowIntegrations, CoreIntegrations, BrowserIntegrations);

    exports.BrowserClient = BrowserClient;
    exports.Hub = Hub;
    exports.Integrations = INTEGRATIONS;
    exports.SDK_NAME = SDK_NAME;
    exports.SDK_VERSION = SDK_VERSION;
    exports.Scope = Scope;
    exports.Transports = index;
    exports.addBreadcrumb = addBreadcrumb;
    exports.addGlobalEventProcessor = addGlobalEventProcessor;
    exports.captureEvent = captureEvent;
    exports.captureException = captureException;
    exports.captureMessage = captureMessage;
    exports.close = close;
    exports.configureScope = configureScope;
    exports.defaultIntegrations = defaultIntegrations;
    exports.flush = flush;
    exports.forceLoad = forceLoad;
    exports.getCurrentHub = getCurrentHub;
    exports.getHubFromCarrier = getHubFromCarrier;
    exports.init = init;
    exports.lastEventId = lastEventId;
    exports.onLoad = onLoad;
    exports.setContext = setContext;
    exports.setExtra = setExtra;
    exports.setExtras = setExtras;
    exports.setTag = setTag;
    exports.setTags = setTags;
    exports.setUser = setUser;
    exports.showReportDialog = showReportDialog;
    exports.withScope = withScope;
    exports.wrap = wrap$1;

    return exports;

}({}));
//# sourceMappingURL=bundle.js.map
