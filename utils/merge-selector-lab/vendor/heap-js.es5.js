var __awaiter = (undefined && undefined.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator$1 = (undefined && undefined.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
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
var __read$1 = (undefined && undefined.__read) || function (o, n) {
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
};
var __spreadArray$1 = (undefined && undefined.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
var __values = (undefined && undefined.__values) || function(o) {
    var s = typeof Symbol === "function" && Symbol.iterator, m = s && o[s], i = 0;
    if (m) return m.call(o);
    if (o && typeof o.length === "number") return {
        next: function () {
            if (o && i >= o.length) o = void 0;
            return { value: o && o[i++], done: !o };
        }
    };
    throw new TypeError(s ? "Object is not iterable." : "Symbol.iterator is not defined.");
};
/**
 * Heap
 * @type {Class}
 */
var HeapAsync = /** @class */ (function () {
    /**
     * Heap instance constructor.
     * @param  {Function} compare Optional comparison function, defaults to Heap.minComparator<number>
     */
    function HeapAsync(compare) {
        if (compare === void 0) { compare = HeapAsync.minComparator; }
        var _this = this;
        this.compare = compare;
        this.heapArray = [];
        this._limit = 0;
        /**
         * Alias of add
         */
        this.offer = this.add;
        /**
         * Alias of peek
         */
        this.element = this.peek;
        /**
         * Alias of pop
         */
        this.poll = this.pop;
        /**
         * Returns the inverse to the comparison function.
         * @return {Number}
         */
        this._invertedCompare = function (a, b) {
            return _this.compare(a, b).then(function (res) { return -1 * res; });
        };
    }
    /*
              Static methods
     */
    /**
     * Gets children indices for given index.
     * @param  {Number} idx     Parent index
     * @return {Array(Number)}  Array of children indices
     */
    HeapAsync.getChildrenIndexOf = function (idx) {
        return [idx * 2 + 1, idx * 2 + 2];
    };
    /**
     * Gets parent index for given index.
     * @param  {Number} idx  Children index
     * @return {Number | undefined}      Parent index, -1 if idx is 0
     */
    HeapAsync.getParentIndexOf = function (idx) {
        if (idx <= 0) {
            return -1;
        }
        var whichChildren = idx % 2 ? 1 : 2;
        return Math.floor((idx - whichChildren) / 2);
    };
    /**
     * Gets sibling index for given index.
     * @param  {Number} idx  Children index
     * @return {Number | undefined}      Sibling index, -1 if idx is 0
     */
    HeapAsync.getSiblingIndexOf = function (idx) {
        if (idx <= 0) {
            return -1;
        }
        var whichChildren = idx % 2 ? 1 : -1;
        return idx + whichChildren;
    };
    /**
     * Min heap comparison function, default.
     * @param  {any} a     First element
     * @param  {any} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    HeapAsync.minComparator = function (a, b) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator$1(this, function (_a) {
                if (a > b) {
                    return [2 /*return*/, 1];
                }
                else if (a < b) {
                    return [2 /*return*/, -1];
                }
                else {
                    return [2 /*return*/, 0];
                }
            });
        });
    };
    /**
     * Max heap comparison function.
     * @param  {any} a     First element
     * @param  {any} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    HeapAsync.maxComparator = function (a, b) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator$1(this, function (_a) {
                if (b > a) {
                    return [2 /*return*/, 1];
                }
                else if (b < a) {
                    return [2 /*return*/, -1];
                }
                else {
                    return [2 /*return*/, 0];
                }
            });
        });
    };
    /**
     * Min number heap comparison function, default.
     * @param  {Number} a     First element
     * @param  {Number} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    HeapAsync.minComparatorNumber = function (a, b) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator$1(this, function (_a) {
                return [2 /*return*/, a - b];
            });
        });
    };
    /**
     * Max number heap comparison function.
     * @param  {Number} a     First element
     * @param  {Number} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    HeapAsync.maxComparatorNumber = function (a, b) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator$1(this, function (_a) {
                return [2 /*return*/, b - a];
            });
        });
    };
    /**
     * Default equality function.
     * @param  {any} a    First element
     * @param  {any} b    Second element
     * @return {Boolean}  True if equal, false otherwise
     */
    HeapAsync.defaultIsEqual = function (a, b) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator$1(this, function (_a) {
                return [2 /*return*/, a === b];
            });
        });
    };
    /**
     * Prints a heap.
     * @param  {HeapAsync} heap Heap to be printed
     * @returns {String}
     */
    HeapAsync.print = function (heap) {
        function deep(i) {
            var pi = HeapAsync.getParentIndexOf(i);
            return Math.floor(Math.log2(pi + 1));
        }
        function repeat(str, times) {
            var out = '';
            for (; times > 0; --times) {
                out += str;
            }
            return out;
        }
        var node = 0;
        var lines = [];
        var maxLines = deep(heap.length - 1) + 2;
        var maxLength = 0;
        while (node < heap.length) {
            var i = deep(node) + 1;
            if (node === 0) {
                i = 0;
            }
            // Text representation
            var nodeText = String(heap.get(node));
            if (nodeText.length > maxLength) {
                maxLength = nodeText.length;
            }
            // Add to line
            lines[i] = lines[i] || [];
            lines[i].push(nodeText);
            node += 1;
        }
        return lines
            .map(function (line, i) {
            var times = Math.pow(2, maxLines - i) - 1;
            return (repeat(' ', Math.floor(times / 2) * maxLength) +
                line
                    .map(function (el) {
                    // centered
                    var half = (maxLength - el.length) / 2;
                    return repeat(' ', Math.ceil(half)) + el + repeat(' ', Math.floor(half));
                })
                    .join(repeat(' ', times * maxLength)));
        })
            .join('\n');
    };
    /*
              Python style
     */
    /**
     * Converts an array into an array-heap, in place
     * @param  {Array}    arr      Array to be modified
     * @param  {Function} compare  Optional compare function
     * @return {HeapAsync}              For convenience, it returns a Heap instance
     */
    HeapAsync.heapify = function (arr, compare) {
        return __awaiter(this, void 0, void 0, function () {
            var heap;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        heap = new HeapAsync(compare);
                        heap.heapArray = arr;
                        return [4 /*yield*/, heap.init()];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, heap];
                }
            });
        });
    };
    /**
     * Extract the peek of an array-heap
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {Function} compare  Optional compare function
     * @return {any}               Returns the extracted peek
     */
    HeapAsync.heappop = function (heapArr, compare) {
        var heap = new HeapAsync(compare);
        heap.heapArray = heapArr;
        return heap.pop();
    };
    /**
     * Pushes a item into an array-heap
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {any}      item     Item to push
     * @param  {Function} compare  Optional compare function
     */
    HeapAsync.heappush = function (heapArr, item, compare) {
        return __awaiter(this, void 0, void 0, function () {
            var heap;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        heap = new HeapAsync(compare);
                        heap.heapArray = heapArr;
                        return [4 /*yield*/, heap.push(item)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    /**
     * Push followed by pop, faster
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {any}      item     Item to push
     * @param  {Function} compare  Optional compare function
     * @return {any}               Returns the extracted peek
     */
    HeapAsync.heappushpop = function (heapArr, item, compare) {
        var heap = new HeapAsync(compare);
        heap.heapArray = heapArr;
        return heap.pushpop(item);
    };
    /**
     * Replace peek with item
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {any}      item     Item as replacement
     * @param  {Function} compare  Optional compare function
     * @return {any}               Returns the extracted peek
     */
    HeapAsync.heapreplace = function (heapArr, item, compare) {
        var heap = new HeapAsync(compare);
        heap.heapArray = heapArr;
        return heap.replace(item);
    };
    /**
     * Return the `n` most valuable elements of a heap-like Array
     * @param  {Array}    heapArr  Array, should be an array-heap
     * @param  {number}   n        Max number of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    HeapAsync.heaptop = function (heapArr, n, compare) {
        if (n === void 0) { n = 1; }
        var heap = new HeapAsync(compare);
        heap.heapArray = heapArr;
        return heap.top(n);
    };
    /**
     * Return the `n` least valuable elements of a heap-like Array
     * @param  {Array}    heapArr  Array, should be an array-heap
     * @param  {number}   n        Max number of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    HeapAsync.heapbottom = function (heapArr, n, compare) {
        if (n === void 0) { n = 1; }
        var heap = new HeapAsync(compare);
        heap.heapArray = heapArr;
        return heap.bottom(n);
    };
    /**
     * Return the `n` most valuable elements of an iterable
     * @param  {number}   n        Max number of elements
     * @param  {Iterable} Iterable Iterable list of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    HeapAsync.nlargest = function (n, iterable, compare) {
        return __awaiter(this, void 0, void 0, function () {
            var heap;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        heap = new HeapAsync(compare);
                        heap.heapArray = __spreadArray$1([], __read$1(iterable), false);
                        return [4 /*yield*/, heap.init()];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, heap.top(n)];
                }
            });
        });
    };
    /**
     * Return the `n` least valuable elements of an iterable
     * @param  {number}   n        Max number of elements
     * @param  {Iterable} Iterable Iterable list of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    HeapAsync.nsmallest = function (n, iterable, compare) {
        return __awaiter(this, void 0, void 0, function () {
            var heap;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        heap = new HeapAsync(compare);
                        heap.heapArray = __spreadArray$1([], __read$1(iterable), false);
                        return [4 /*yield*/, heap.init()];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, heap.bottom(n)];
                }
            });
        });
    };
    /*
              Instance methods
     */
    /**
     * Adds an element to the heap. Aliases: `offer`.
     * Same as: push(element)
     * @param {any} element Element to be added
     * @return {Boolean} true
     */
    HeapAsync.prototype.add = function (element) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._sortNodeUp(this.heapArray.push(element) - 1)];
                    case 1:
                        _a.sent();
                        this._applyLimit();
                        return [2 /*return*/, true];
                }
            });
        });
    };
    /**
     * Adds an array of elements to the heap.
     * Similar as: push(element, element, ...).
     * @param {Array} elements Elements to be added
     * @return {Boolean} true
     */
    HeapAsync.prototype.addAll = function (elements) {
        return __awaiter(this, void 0, void 0, function () {
            var i, l;
            var _a;
            return __generator$1(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        i = this.length;
                        (_a = this.heapArray).push.apply(_a, __spreadArray$1([], __read$1(elements), false));
                        l = this.length;
                        _b.label = 1;
                    case 1:
                        if (!(i < l)) return [3 /*break*/, 4];
                        return [4 /*yield*/, this._sortNodeUp(i)];
                    case 2:
                        _b.sent();
                        _b.label = 3;
                    case 3:
                        ++i;
                        return [3 /*break*/, 1];
                    case 4:
                        this._applyLimit();
                        return [2 /*return*/, true];
                }
            });
        });
    };
    /**
     * Return the bottom (lowest value) N elements of the heap.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    HeapAsync.prototype.bottom = function () {
        return __awaiter(this, arguments, void 0, function (n) {
            if (n === void 0) { n = 1; }
            return __generator$1(this, function (_a) {
                if (this.heapArray.length === 0 || n <= 0) {
                    // Nothing to do
                    return [2 /*return*/, []];
                }
                else if (this.heapArray.length === 1) {
                    // Just the peek
                    return [2 /*return*/, [this.heapArray[0]]];
                }
                else if (n >= this.heapArray.length) {
                    // The whole heap
                    return [2 /*return*/, __spreadArray$1([], __read$1(this.heapArray), false)];
                }
                else {
                    // Some elements
                    return [2 /*return*/, this._bottomN_push(~~n)];
                }
            });
        });
    };
    /**
     * Check if the heap is sorted, useful for testing purposes.
     * @return {Undefined | Element}  Returns an element if something wrong is found, otherwise it's undefined
     */
    HeapAsync.prototype.check = function () {
        return __awaiter(this, void 0, void 0, function () {
            var j, el, children, children_1, children_1_1, ch, e_1_1;
            var e_1, _a;
            return __generator$1(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        j = 0;
                        _b.label = 1;
                    case 1:
                        if (!(j < this.heapArray.length)) return [3 /*break*/, 10];
                        el = this.heapArray[j];
                        children = this.getChildrenOf(j);
                        _b.label = 2;
                    case 2:
                        _b.trys.push([2, 7, 8, 9]);
                        children_1 = (e_1 = void 0, __values(children)), children_1_1 = children_1.next();
                        _b.label = 3;
                    case 3:
                        if (!!children_1_1.done) return [3 /*break*/, 6];
                        ch = children_1_1.value;
                        return [4 /*yield*/, this.compare(el, ch)];
                    case 4:
                        if ((_b.sent()) > 0) {
                            return [2 /*return*/, el];
                        }
                        _b.label = 5;
                    case 5:
                        children_1_1 = children_1.next();
                        return [3 /*break*/, 3];
                    case 6: return [3 /*break*/, 9];
                    case 7:
                        e_1_1 = _b.sent();
                        e_1 = { error: e_1_1 };
                        return [3 /*break*/, 9];
                    case 8:
                        try {
                            if (children_1_1 && !children_1_1.done && (_a = children_1.return)) _a.call(children_1);
                        }
                        finally { if (e_1) throw e_1.error; }
                        return [7 /*endfinally*/];
                    case 9:
                        ++j;
                        return [3 /*break*/, 1];
                    case 10: return [2 /*return*/];
                }
            });
        });
    };
    /**
     * Remove all of the elements from this heap.
     */
    HeapAsync.prototype.clear = function () {
        this.heapArray = [];
    };
    /**
     * Clone this heap
     * @return {HeapAsync}
     */
    HeapAsync.prototype.clone = function () {
        var cloned = new HeapAsync(this.comparator());
        cloned.heapArray = this.toArray();
        cloned._limit = this._limit;
        return cloned;
    };
    /**
     * Returns the comparison function.
     * @return {Function}
     */
    HeapAsync.prototype.comparator = function () {
        return this.compare;
    };
    /**
     * Returns true if this queue contains the specified element.
     * @param  {any}      o   Element to be found
     * @param  {Function} fn  Optional comparison function, receives (element, needle)
     * @return {Boolean}
     */
    HeapAsync.prototype.contains = function (o_1) {
        return __awaiter(this, arguments, void 0, function (o, fn) {
            var _a, _b, el, e_2_1;
            var e_2, _c;
            if (fn === void 0) { fn = HeapAsync.defaultIsEqual; }
            return __generator$1(this, function (_d) {
                switch (_d.label) {
                    case 0:
                        _d.trys.push([0, 5, 6, 7]);
                        _a = __values(this.heapArray), _b = _a.next();
                        _d.label = 1;
                    case 1:
                        if (!!_b.done) return [3 /*break*/, 4];
                        el = _b.value;
                        return [4 /*yield*/, fn(el, o)];
                    case 2:
                        if (_d.sent()) {
                            return [2 /*return*/, true];
                        }
                        _d.label = 3;
                    case 3:
                        _b = _a.next();
                        return [3 /*break*/, 1];
                    case 4: return [3 /*break*/, 7];
                    case 5:
                        e_2_1 = _d.sent();
                        e_2 = { error: e_2_1 };
                        return [3 /*break*/, 7];
                    case 6:
                        try {
                            if (_b && !_b.done && (_c = _a.return)) _c.call(_a);
                        }
                        finally { if (e_2) throw e_2.error; }
                        return [7 /*endfinally*/];
                    case 7: return [2 /*return*/, false];
                }
            });
        });
    };
    /**
     * Initialise a heap, sorting nodes
     * @param  {Array} array Optional initial state array
     */
    HeapAsync.prototype.init = function (array) {
        return __awaiter(this, void 0, void 0, function () {
            var i;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (array) {
                            this.heapArray = __spreadArray$1([], __read$1(array), false);
                        }
                        i = HeapAsync.getParentIndexOf(this.length - 1);
                        _a.label = 1;
                    case 1:
                        if (!(i >= 0)) return [3 /*break*/, 4];
                        return [4 /*yield*/, this._sortNodeDown(i)];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3:
                        --i;
                        return [3 /*break*/, 1];
                    case 4:
                        this._applyLimit();
                        return [2 /*return*/];
                }
            });
        });
    };
    /**
     * Test if the heap has no elements.
     * @return {Boolean} True if no elements on the heap
     */
    HeapAsync.prototype.isEmpty = function () {
        return this.length === 0;
    };
    /**
     * Get the leafs of the tree (no children nodes)
     */
    HeapAsync.prototype.leafs = function () {
        if (this.heapArray.length === 0) {
            return [];
        }
        var pi = HeapAsync.getParentIndexOf(this.heapArray.length - 1);
        return this.heapArray.slice(pi + 1);
    };
    Object.defineProperty(HeapAsync.prototype, "length", {
        /**
         * Length of the heap.
         * @return {Number}
         */
        get: function () {
            return this.heapArray.length;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(HeapAsync.prototype, "limit", {
        /**
         * Get length limit of the heap.
         * @return {Number}
         */
        get: function () {
            return this._limit;
        },
        /**
         * Set length limit of the heap.
         * @return {Number}
         */
        set: function (_l) {
            this._limit = ~~_l;
            this._applyLimit();
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Top node. Aliases: `element`.
     * Same as: `top(1)[0]`
     * @return {any} Top node
     */
    HeapAsync.prototype.peek = function () {
        return this.heapArray[0];
    };
    /**
     * Extract the top node (root). Aliases: `poll`.
     * @return {any} Extracted top node, undefined if empty
     */
    HeapAsync.prototype.pop = function () {
        return __awaiter(this, void 0, void 0, function () {
            var last;
            return __generator$1(this, function (_a) {
                last = this.heapArray.pop();
                if (this.length > 0 && last !== undefined) {
                    return [2 /*return*/, this.replace(last)];
                }
                return [2 /*return*/, last];
            });
        });
    };
    /**
     * Pushes element(s) to the heap.
     * @param  {...any} elements Elements to insert
     * @return {Boolean} True if elements are present
     */
    HeapAsync.prototype.push = function () {
        var elements = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            elements[_i] = arguments[_i];
        }
        return __awaiter(this, void 0, void 0, function () {
            return __generator$1(this, function (_a) {
                if (elements.length < 1) {
                    return [2 /*return*/, false];
                }
                else if (elements.length === 1) {
                    return [2 /*return*/, this.add(elements[0])];
                }
                else {
                    return [2 /*return*/, this.addAll(elements)];
                }
            });
        });
    };
    /**
     * Same as push & pop in sequence, but faster
     * @param  {any} element Element to insert
     * @return {any}  Extracted top node
     */
    HeapAsync.prototype.pushpop = function (element) {
        return __awaiter(this, void 0, void 0, function () {
            var _a;
            return __generator$1(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, this.compare(this.heapArray[0], element)];
                    case 1:
                        if (!((_b.sent()) < 0)) return [3 /*break*/, 3];
                        _a = __read$1([this.heapArray[0], element], 2), element = _a[0], this.heapArray[0] = _a[1];
                        return [4 /*yield*/, this._sortNodeDown(0)];
                    case 2:
                        _b.sent();
                        _b.label = 3;
                    case 3: return [2 /*return*/, element];
                }
            });
        });
    };
    /**
     * Remove an element from the heap.
     * @param  {any}   o      Element to be found
     * @param  {Function} fn  Optional function to compare
     * @return {Boolean}      True if the heap was modified
     */
    HeapAsync.prototype.remove = function (o_1) {
        return __awaiter(this, arguments, void 0, function (o, fn) {
            var queue, idx, children;
            var _this = this;
            if (fn === void 0) { fn = HeapAsync.defaultIsEqual; }
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!this.heapArray.length)
                            return [2 /*return*/, false];
                        if (!(o === undefined)) return [3 /*break*/, 2];
                        return [4 /*yield*/, this.pop()];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, true];
                    case 2:
                        queue = [0];
                        _a.label = 3;
                    case 3:
                        if (!queue.length) return [3 /*break*/, 13];
                        idx = queue.shift();
                        return [4 /*yield*/, fn(this.heapArray[idx], o)];
                    case 4:
                        if (!_a.sent()) return [3 /*break*/, 11];
                        if (!(idx === 0)) return [3 /*break*/, 6];
                        return [4 /*yield*/, this.pop()];
                    case 5:
                        _a.sent();
                        return [3 /*break*/, 10];
                    case 6:
                        if (!(idx === this.heapArray.length - 1)) return [3 /*break*/, 7];
                        this.heapArray.pop();
                        return [3 /*break*/, 10];
                    case 7:
                        this.heapArray.splice(idx, 1, this.heapArray.pop());
                        return [4 /*yield*/, this._sortNodeUp(idx)];
                    case 8:
                        _a.sent();
                        return [4 /*yield*/, this._sortNodeDown(idx)];
                    case 9:
                        _a.sent();
                        _a.label = 10;
                    case 10: return [2 /*return*/, true];
                    case 11:
                        children = HeapAsync.getChildrenIndexOf(idx).filter(function (c) { return c < _this.heapArray.length; });
                        queue.push.apply(queue, __spreadArray$1([], __read$1(children), false));
                        _a.label = 12;
                    case 12: return [3 /*break*/, 3];
                    case 13: return [2 /*return*/, false];
                }
            });
        });
    };
    /**
     * Pop the current peek value, and add the new item.
     * @param  {any} element  Element to replace peek
     * @return {any}         Old peek
     */
    HeapAsync.prototype.replace = function (element) {
        return __awaiter(this, void 0, void 0, function () {
            var peek;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        peek = this.heapArray[0];
                        this.heapArray[0] = element;
                        return [4 /*yield*/, this._sortNodeDown(0)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, peek];
                }
            });
        });
    };
    /**
     * Size of the heap
     * @return {Number}
     */
    HeapAsync.prototype.size = function () {
        return this.length;
    };
    /**
     * Return the top (highest value) N elements of the heap.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}    Array of length <= N.
     */
    HeapAsync.prototype.top = function () {
        return __awaiter(this, arguments, void 0, function (n) {
            if (n === void 0) { n = 1; }
            return __generator$1(this, function (_a) {
                if (this.heapArray.length === 0 || n <= 0) {
                    // Nothing to do
                    return [2 /*return*/, []];
                }
                else if (this.heapArray.length === 1 || n === 1) {
                    // Just the peek
                    return [2 /*return*/, [this.heapArray[0]]];
                }
                else if (n >= this.heapArray.length) {
                    // The whole peek
                    return [2 /*return*/, __spreadArray$1([], __read$1(this.heapArray), false)];
                }
                else {
                    // Some elements
                    return [2 /*return*/, this._topN_push(~~n)];
                }
            });
        });
    };
    /**
     * Clone the heap's internal array
     * @return {Array}
     */
    HeapAsync.prototype.toArray = function () {
        return __spreadArray$1([], __read$1(this.heapArray), false);
    };
    /**
     * String output, call to Array.prototype.toString()
     * @return {String}
     */
    HeapAsync.prototype.toString = function () {
        return this.heapArray.toString();
    };
    /**
     * Get the element at the given index.
     * @param  {Number} i Index to get
     * @return {any}       Element at that index
     */
    HeapAsync.prototype.get = function (i) {
        return this.heapArray[i];
    };
    /**
     * Get the elements of these node's children
     * @param  {Number} idx Node index
     * @return {Array(any)}  Children elements
     */
    HeapAsync.prototype.getChildrenOf = function (idx) {
        var _this = this;
        return HeapAsync.getChildrenIndexOf(idx)
            .map(function (i) { return _this.heapArray[i]; })
            .filter(function (e) { return e !== undefined; });
    };
    /**
     * Get the element of this node's parent
     * @param  {Number} idx Node index
     * @return {any}     Parent element
     */
    HeapAsync.prototype.getParentOf = function (idx) {
        var pi = HeapAsync.getParentIndexOf(idx);
        return this.heapArray[pi];
    };
    /**
     * Iterator interface
     */
    HeapAsync.prototype[Symbol.iterator] = function () {
        return __generator$1(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (!this.length) return [3 /*break*/, 2];
                    return [4 /*yield*/, this.pop()];
                case 1:
                    _a.sent();
                    return [3 /*break*/, 0];
                case 2: return [2 /*return*/];
            }
        });
    };
    /**
     * Returns an iterator. To comply with Java interface.
     */
    HeapAsync.prototype.iterator = function () {
        return this;
    };
    /**
     * Limit heap size if needed
     */
    HeapAsync.prototype._applyLimit = function () {
        if (this._limit && this._limit < this.heapArray.length) {
            var rm = this.heapArray.length - this._limit;
            // It's much faster than splice
            while (rm) {
                this.heapArray.pop();
                --rm;
            }
        }
    };
    /**
     * Return the bottom (lowest value) N elements of the heap, without corner cases, unsorted
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    HeapAsync.prototype._bottomN_push = function (n) {
        return __awaiter(this, void 0, void 0, function () {
            var bottomHeap, startAt, parentStartAt, indices, i, arr, i;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        bottomHeap = new HeapAsync(this.compare);
                        bottomHeap.limit = n;
                        bottomHeap.heapArray = this.heapArray.slice(-n);
                        return [4 /*yield*/, bottomHeap.init()];
                    case 1:
                        _a.sent();
                        startAt = this.heapArray.length - 1 - n;
                        parentStartAt = HeapAsync.getParentIndexOf(startAt);
                        indices = [];
                        for (i = startAt; i > parentStartAt; --i) {
                            indices.push(i);
                        }
                        arr = this.heapArray;
                        _a.label = 2;
                    case 2:
                        if (!indices.length) return [3 /*break*/, 6];
                        i = indices.shift();
                        return [4 /*yield*/, this.compare(arr[i], bottomHeap.peek())];
                    case 3:
                        if (!((_a.sent()) > 0)) return [3 /*break*/, 5];
                        return [4 /*yield*/, bottomHeap.replace(arr[i])];
                    case 4:
                        _a.sent();
                        if (i % 2) {
                            indices.push(HeapAsync.getParentIndexOf(i));
                        }
                        _a.label = 5;
                    case 5: return [3 /*break*/, 2];
                    case 6: return [2 /*return*/, bottomHeap.toArray()];
                }
            });
        });
    };
    /**
     * Move a node to a new index, switching places
     * @param  {Number} j First node index
     * @param  {Number} k Another node index
     */
    HeapAsync.prototype._moveNode = function (j, k) {
        var temp = this.heapArray[j];
        this.heapArray[j] = this.heapArray[k];
        this.heapArray[k] = temp;
    };
    /**
     * Move a node down the tree (to the leaves) to find a place where the heap is sorted.
     * @param  {Number} i Index of the node
     */
    HeapAsync.prototype._sortNodeDown = function (i) {
        return __awaiter(this, void 0, void 0, function () {
            var length, originalIndex, value, left, right, best, _a;
            return __generator$1(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        length = this.heapArray.length;
                        originalIndex = i;
                        value = this.heapArray[i];
                        left = 2 * i + 1;
                        _b.label = 1;
                    case 1:
                        if (!(left < length)) return [3 /*break*/, 5];
                        right = left + 1;
                        _a = right >= length;
                        if (_a) return [3 /*break*/, 3];
                        return [4 /*yield*/, this.compare(this.heapArray[left], this.heapArray[right])];
                    case 2:
                        _a = (_b.sent()) < 0;
                        _b.label = 3;
                    case 3:
                        best = _a ? left
                            : right;
                        return [4 /*yield*/, this.compare(this.heapArray[best], value)];
                    case 4:
                        if ((_b.sent()) < 0) {
                            this.heapArray[i] = this.heapArray[best];
                            i = best;
                            left = 2 * i + 1;
                        }
                        else
                            return [3 /*break*/, 5];
                        return [3 /*break*/, 1];
                    case 5:
                        if (i !== originalIndex) {
                            this.heapArray[i] = value;
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    /**
     * Move a node up the tree (to the root) to find a place where the heap is sorted.
     * @param  {Number} i Index of the node
     */
    HeapAsync.prototype._sortNodeUp = function (i) {
        return __awaiter(this, void 0, void 0, function () {
            var value, originalIndex, pi;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        value = this.heapArray[i];
                        originalIndex = i;
                        _a.label = 1;
                    case 1:
                        if (!(i > 0)) return [3 /*break*/, 3];
                        pi = HeapAsync.getParentIndexOf(i);
                        return [4 /*yield*/, this.compare(value, this.heapArray[pi])];
                    case 2:
                        if ((_a.sent()) < 0) {
                            this.heapArray[i] = this.heapArray[pi];
                            i = pi;
                        }
                        else
                            return [3 /*break*/, 3];
                        return [3 /*break*/, 1];
                    case 3:
                        if (i !== originalIndex) {
                            this.heapArray[i] = value;
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    /**
     * Return the top (highest value) N elements of the heap, without corner cases, unsorted
     * Implementation: push.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    HeapAsync.prototype._topN_push = function (n) {
        return __awaiter(this, void 0, void 0, function () {
            var topHeap, indices, arr, i;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        topHeap = new HeapAsync(this._invertedCompare);
                        topHeap.limit = n;
                        indices = [0];
                        arr = this.heapArray;
                        _a.label = 1;
                    case 1:
                        if (!indices.length) return [3 /*break*/, 7];
                        i = indices.shift();
                        if (!(i < arr.length)) return [3 /*break*/, 6];
                        if (!(topHeap.length < n)) return [3 /*break*/, 3];
                        return [4 /*yield*/, topHeap.push(arr[i])];
                    case 2:
                        _a.sent();
                        indices.push.apply(indices, __spreadArray$1([], __read$1(HeapAsync.getChildrenIndexOf(i)), false));
                        return [3 /*break*/, 6];
                    case 3: return [4 /*yield*/, this.compare(arr[i], topHeap.peek())];
                    case 4:
                        if (!((_a.sent()) < 0)) return [3 /*break*/, 6];
                        return [4 /*yield*/, topHeap.replace(arr[i])];
                    case 5:
                        _a.sent();
                        indices.push.apply(indices, __spreadArray$1([], __read$1(HeapAsync.getChildrenIndexOf(i)), false));
                        _a.label = 6;
                    case 6: return [3 /*break*/, 1];
                    case 7: return [2 /*return*/, topHeap.toArray()];
                }
            });
        });
    };
    /**
     * Return the top (highest value) N elements of the heap, without corner cases, unsorted
     * Implementation: init + push.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    HeapAsync.prototype._topN_fill = function (n) {
        return __awaiter(this, void 0, void 0, function () {
            var heapArray, topHeap, branch, indices, i, i;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        heapArray = this.heapArray;
                        topHeap = new HeapAsync(this._invertedCompare);
                        topHeap.limit = n;
                        topHeap.heapArray = heapArray.slice(0, n);
                        return [4 /*yield*/, topHeap.init()];
                    case 1:
                        _a.sent();
                        branch = HeapAsync.getParentIndexOf(n - 1) + 1;
                        indices = [];
                        for (i = branch; i < n; ++i) {
                            indices.push.apply(indices, __spreadArray$1([], __read$1(HeapAsync.getChildrenIndexOf(i).filter(function (l) { return l < heapArray.length; })), false));
                        }
                        if ((n - 1) % 2) {
                            indices.push(n);
                        }
                        _a.label = 2;
                    case 2:
                        if (!indices.length) return [3 /*break*/, 6];
                        i = indices.shift();
                        if (!(i < heapArray.length)) return [3 /*break*/, 5];
                        return [4 /*yield*/, this.compare(heapArray[i], topHeap.peek())];
                    case 3:
                        if (!((_a.sent()) < 0)) return [3 /*break*/, 5];
                        return [4 /*yield*/, topHeap.replace(heapArray[i])];
                    case 4:
                        _a.sent();
                        indices.push.apply(indices, __spreadArray$1([], __read$1(HeapAsync.getChildrenIndexOf(i)), false));
                        _a.label = 5;
                    case 5: return [3 /*break*/, 2];
                    case 6: return [2 /*return*/, topHeap.toArray()];
                }
            });
        });
    };
    /**
     * Return the top (highest value) N elements of the heap, without corner cases, unsorted
     * Implementation: heap.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    HeapAsync.prototype._topN_heap = function (n) {
        return __awaiter(this, void 0, void 0, function () {
            var topHeap, result, i, _a, _b;
            return __generator$1(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        topHeap = this.clone();
                        result = [];
                        i = 0;
                        _c.label = 1;
                    case 1:
                        if (!(i < n)) return [3 /*break*/, 4];
                        _b = (_a = result).push;
                        return [4 /*yield*/, topHeap.pop()];
                    case 2:
                        _b.apply(_a, [(_c.sent())]);
                        _c.label = 3;
                    case 3:
                        ++i;
                        return [3 /*break*/, 1];
                    case 4: return [2 /*return*/, result];
                }
            });
        });
    };
    /**
     * Return index of the top element
     * @param list
     */
    HeapAsync.prototype._topIdxOf = function (list) {
        return __awaiter(this, void 0, void 0, function () {
            var idx, top, i, comp;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!list.length) {
                            return [2 /*return*/, -1];
                        }
                        idx = 0;
                        top = list[idx];
                        i = 1;
                        _a.label = 1;
                    case 1:
                        if (!(i < list.length)) return [3 /*break*/, 4];
                        return [4 /*yield*/, this.compare(list[i], top)];
                    case 2:
                        comp = _a.sent();
                        if (comp < 0) {
                            idx = i;
                            top = list[i];
                        }
                        _a.label = 3;
                    case 3:
                        ++i;
                        return [3 /*break*/, 1];
                    case 4: return [2 /*return*/, idx];
                }
            });
        });
    };
    /**
     * Return the top element
     * @param list
     */
    HeapAsync.prototype._topOf = function () {
        var list = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            list[_i] = arguments[_i];
        }
        return __awaiter(this, void 0, void 0, function () {
            var heap;
            return __generator$1(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        heap = new HeapAsync(this.compare);
                        return [4 /*yield*/, heap.init(list)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, heap.peek()];
                }
            });
        });
    };
    return HeapAsync;
}());

var __generator = (undefined && undefined.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
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
var __read = (undefined && undefined.__read) || function (o, n) {
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
};
var __spreadArray = (undefined && undefined.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
var toInt = function (n) { return ~~n; };
/**
 * Heap
 * @type {Class}
 */
var Heap = /** @class */ (function () {
    /**
     * Heap instance constructor.
     * @param  {Function} compare Optional comparison function, defaults to Heap.minComparator<number>
     */
    function Heap(compare) {
        if (compare === void 0) { compare = Heap.minComparator; }
        var _this = this;
        this.compare = compare;
        this.heapArray = [];
        this._limit = 0;
        /**
         * Alias of {@link add}
         * @see add
         */
        this.offer = this.add;
        /**
         * Alias of {@link peek}
         * @see peek
         */
        this.element = this.peek;
        /**
         * Alias of {@link pop}
         * @see pop
         */
        this.poll = this.pop;
        /**
         * Alias of {@link clear}
         * @see clear
         */
        this.removeAll = this.clear;
        /**
         * Returns the inverse to the comparison function.
         * @return {Function}
         */
        this._invertedCompare = function (a, b) {
            return -1 * _this.compare(a, b);
        };
    }
    /*
              Static methods
     */
    /**
     * Gets children indices for given index.
     * @param  {Number} idx     Parent index
     * @return {Array(Number)}  Array of children indices
     */
    Heap.getChildrenIndexOf = function (idx) {
        return [idx * 2 + 1, idx * 2 + 2];
    };
    /**
     * Gets parent index for given index.
     * @param  {Number} idx  Children index
     * @return {Number | undefined}      Parent index, -1 if idx is 0
     */
    Heap.getParentIndexOf = function (idx) {
        if (idx <= 0) {
            return -1;
        }
        return (idx - 1) >> 1;
    };
    /**
     * Gets sibling index for given index.
     * @param  {Number} idx  Children index
     * @return {Number | undefined}      Sibling index, -1 if idx is 0
     */
    Heap.getSiblingIndexOf = function (idx) {
        if (idx <= 0) {
            return -1;
        }
        var whichChildren = idx % 2 ? 1 : -1;
        return idx + whichChildren;
    };
    /**
     * Min heap comparison function, default.
     * @param  {any} a     First element
     * @param  {any} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    Heap.minComparator = function (a, b) {
        if (a > b) {
            return 1;
        }
        else if (a < b) {
            return -1;
        }
        else {
            return 0;
        }
    };
    /**
     * Max heap comparison function.
     * @param  {any} a     First element
     * @param  {any} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    Heap.maxComparator = function (a, b) {
        if (b > a) {
            return 1;
        }
        else if (b < a) {
            return -1;
        }
        else {
            return 0;
        }
    };
    /**
     * Min number heap comparison function, default.
     * @param  {Number} a     First element
     * @param  {Number} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    Heap.minComparatorNumber = function (a, b) {
        return a - b;
    };
    /**
     * Max number heap comparison function.
     * @param  {Number} a     First element
     * @param  {Number} b     Second element
     * @return {Number}    0 if they're equal, positive if `a` goes up, negative if `b` goes up
     */
    Heap.maxComparatorNumber = function (a, b) {
        return b - a;
    };
    /**
     * Default equality function.
     * @param  {any} a    First element
     * @param  {any} b    Second element
     * @return {Boolean}  True if equal, false otherwise
     */
    Heap.defaultIsEqual = function (a, b) {
        return a === b;
    };
    /**
     * Prints a heap.
     * @param  {Heap} heap Heap to be printed
     * @returns {String}
     */
    Heap.print = function (heap) {
        function deep(i) {
            var pi = Heap.getParentIndexOf(i);
            return Math.floor(Math.log2(pi + 1));
        }
        function repeat(str, times) {
            var out = '';
            for (; times > 0; --times) {
                out += str;
            }
            return out;
        }
        var node = 0;
        var lines = [];
        var maxLines = deep(heap.length - 1) + 2;
        var maxLength = 0;
        while (node < heap.length) {
            var i = deep(node) + 1;
            if (node === 0) {
                i = 0;
            }
            // Text representation
            var nodeText = String(heap.get(node));
            if (nodeText.length > maxLength) {
                maxLength = nodeText.length;
            }
            // Add to line
            lines[i] = lines[i] || [];
            lines[i].push(nodeText);
            node += 1;
        }
        return lines
            .map(function (line, i) {
            var times = Math.pow(2, maxLines - i) - 1;
            return (repeat(' ', Math.floor(times / 2) * maxLength) +
                line
                    .map(function (el) {
                    // centered
                    var half = (maxLength - el.length) / 2;
                    return repeat(' ', Math.ceil(half)) + el + repeat(' ', Math.floor(half));
                })
                    .join(repeat(' ', times * maxLength)));
        })
            .join('\n');
    };
    /*
              Python style
     */
    /**
     * Converts an array into an array-heap, in place
     * @param  {Array}    arr      Array to be modified
     * @param  {Function} compare  Optional compare function
     * @return {Heap}              For convenience, it returns a Heap instance
     */
    Heap.heapify = function (arr, compare) {
        var heap = new Heap(compare);
        heap.heapArray = arr;
        heap.init();
        return heap;
    };
    /**
     * Extract the peek of an array-heap
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {Function} compare  Optional compare function
     * @return {any}               Returns the extracted peek
     */
    Heap.heappop = function (heapArr, compare) {
        var heap = new Heap(compare);
        heap.heapArray = heapArr;
        return heap.pop();
    };
    /**
     * Pushes a item into an array-heap
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {any}      item     Item to push
     * @param  {Function} compare  Optional compare function
     */
    Heap.heappush = function (heapArr, item, compare) {
        var heap = new Heap(compare);
        heap.heapArray = heapArr;
        heap.push(item);
    };
    /**
     * Push followed by pop, faster
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {any}      item     Item to push
     * @param  {Function} compare  Optional compare function
     * @return {any}               Returns the extracted peek
     */
    Heap.heappushpop = function (heapArr, item, compare) {
        var heap = new Heap(compare);
        heap.heapArray = heapArr;
        return heap.pushpop(item);
    };
    /**
     * Replace peek with item
     * @param  {Array}    heapArr  Array to be modified, should be a heap
     * @param  {any}      item     Item as replacement
     * @param  {Function} compare  Optional compare function
     * @return {any}               Returns the extracted peek
     */
    Heap.heapreplace = function (heapArr, item, compare) {
        var heap = new Heap(compare);
        heap.heapArray = heapArr;
        return heap.replace(item);
    };
    /**
     * Return the `n` most valuable elements of a heap-like Array
     * @param  {Array}    heapArr  Array, should be an array-heap
     * @param  {number}   n        Max number of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    Heap.heaptop = function (heapArr, n, compare) {
        if (n === void 0) { n = 1; }
        var heap = new Heap(compare);
        heap.heapArray = heapArr;
        return heap.top(n);
    };
    /**
     * Return the `n` least valuable elements of a heap-like Array
     * @param  {Array}    heapArr  Array, should be an array-heap
     * @param  {number}   n        Max number of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    Heap.heapbottom = function (heapArr, n, compare) {
        if (n === void 0) { n = 1; }
        var heap = new Heap(compare);
        heap.heapArray = heapArr;
        return heap.bottom(n);
    };
    /**
     * Return the `n` most valuable elements of an iterable
     * @param  {number}   n        Max number of elements
     * @param  {Iterable} Iterable Iterable list of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    Heap.nlargest = function (n, iterable, compare) {
        var heap = new Heap(compare);
        heap.heapArray = __spreadArray([], __read(iterable), false);
        heap.init();
        return heap.top(n);
    };
    /**
     * Return the `n` least valuable elements of an iterable
     * @param  {number}   n        Max number of elements
     * @param  {Iterable} Iterable Iterable list of elements
     * @param  {Function} compare  Optional compare function
     * @return {any}               Elements
     */
    Heap.nsmallest = function (n, iterable, compare) {
        var heap = new Heap(compare);
        heap.heapArray = __spreadArray([], __read(iterable), false);
        heap.init();
        return heap.bottom(n);
    };
    /*
              Instance methods
     */
    /**
     * Adds an element to the heap. Aliases: {@link offer}.
     * Same as: {@link push}(element).
     * @param {any} element Element to be added
     * @return {Boolean} true
     */
    Heap.prototype.add = function (element) {
        this._sortNodeUp(this.heapArray.push(element) - 1);
        this._applyLimit();
        return true;
    };
    /**
     * Adds an array of elements to the heap.
     * Similar as: {@link push}(element, element, ...).
     * @param {Array} elements Elements to be added
     * @return {Boolean} true
     */
    Heap.prototype.addAll = function (elements) {
        var _a;
        var i = this.length;
        (_a = this.heapArray).push.apply(_a, __spreadArray([], __read(elements), false));
        for (var l = this.length; i < l; ++i) {
            this._sortNodeUp(i);
        }
        this._applyLimit();
        return true;
    };
    /**
     * Return the bottom (lowest value) N elements of the heap.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    Heap.prototype.bottom = function (n) {
        if (n === void 0) { n = 1; }
        if (this.heapArray.length === 0 || n <= 0) {
            // Nothing to do
            return [];
        }
        else if (this.heapArray.length === 1) {
            // Just the peek
            return [this.heapArray[0]];
        }
        else if (n >= this.heapArray.length) {
            // The whole heap
            return __spreadArray([], __read(this.heapArray), false);
        }
        else {
            // Some elements
            return this._bottomN_push(~~n);
        }
    };
    /**
     * Check if the heap is sorted, useful for testing purposes.
     * @return {Undefined | Element}  Returns an element if something wrong is found, otherwise it's undefined
     */
    Heap.prototype.check = function () {
        var _this = this;
        return this.heapArray.find(function (el, j) { return !!_this.getChildrenOf(j).find(function (ch) { return _this.compare(el, ch) > 0; }); });
    };
    /**
     * Remove all of the elements from this heap.
     */
    Heap.prototype.clear = function () {
        this.heapArray = [];
    };
    /**
     * Clone this heap
     * @return {Heap}
     */
    Heap.prototype.clone = function () {
        var cloned = new Heap(this.comparator());
        cloned.heapArray = this.toArray();
        cloned._limit = this._limit;
        return cloned;
    };
    /**
     * Returns the comparison function.
     * @return {Function}
     */
    Heap.prototype.comparator = function () {
        return this.compare;
    };
    /**
     * Returns true if this queue contains the specified element.
     * @param  {any}      o   Element to be found
     * @param  {Function} callbackFn  Optional comparison function, receives (element, needle)
     * @return {Boolean}
     */
    Heap.prototype.contains = function (o, callbackFn) {
        if (callbackFn === void 0) { callbackFn = Heap.defaultIsEqual; }
        return this.indexOf(o, callbackFn) !== -1;
    };
    /**
     * Initialize a heap, sorting nodes
     * @param  {Array} array Optional initial state array
     */
    Heap.prototype.init = function (array) {
        if (array) {
            this.heapArray = __spreadArray([], __read(array), false);
        }
        for (var i = Heap.getParentIndexOf(this.length - 1); i >= 0; --i) {
            this._sortNodeDown(i);
        }
        this._applyLimit();
    };
    /**
     * Test if the heap has no elements.
     * @return {Boolean} True if no elements on the heap
     */
    Heap.prototype.isEmpty = function () {
        return this.length === 0;
    };
    /**
     * Get the index of the first occurrence of the element in the heap (using the comparator).
     * @param  {any}      element    Element to be found
     * @param  {Function} callbackFn Optional comparison function, receives (element, needle)
     * @return {Number}              Index or -1 if not found
     */
    Heap.prototype.indexOf = function (element, callbackFn) {
        if (callbackFn === void 0) { callbackFn = Heap.defaultIsEqual; }
        if (this.heapArray.length === 0) {
            return -1;
        }
        var indexes = [];
        var currentIndex = 0;
        while (currentIndex < this.heapArray.length) {
            var currentElement = this.heapArray[currentIndex];
            if (callbackFn(currentElement, element)) {
                return currentIndex;
            }
            else if (this.compare(currentElement, element) <= 0) {
                indexes.push.apply(indexes, __spreadArray([], __read(Heap.getChildrenIndexOf(currentIndex)), false));
            }
            currentIndex = indexes.shift() || this.heapArray.length;
        }
        return -1;
    };
    /**
     * Get the indexes of the every occurrence of the element in the heap (using the comparator).
     * @param  {any}      element    Element to be found
     * @param  {Function} callbackFn Optional comparison function, receives (element, needle)
     * @return {Array}               Array of indexes or empty array if not found
     */
    Heap.prototype.indexOfEvery = function (element, callbackFn) {
        if (callbackFn === void 0) { callbackFn = Heap.defaultIsEqual; }
        if (this.heapArray.length === 0) {
            return [];
        }
        var indexes = [];
        var foundIndexes = [];
        var currentIndex = 0;
        while (currentIndex < this.heapArray.length) {
            var currentElement = this.heapArray[currentIndex];
            if (callbackFn(currentElement, element)) {
                foundIndexes.push(currentIndex);
                indexes.push.apply(indexes, __spreadArray([], __read(Heap.getChildrenIndexOf(currentIndex)), false));
            }
            else if (this.compare(currentElement, element) <= 0) {
                indexes.push.apply(indexes, __spreadArray([], __read(Heap.getChildrenIndexOf(currentIndex)), false));
            }
            currentIndex = indexes.shift() || this.heapArray.length;
        }
        return foundIndexes;
    };
    /**
     * Get the leafs of the tree (no children nodes).
     * See also: {@link getChildrenOf} and {@link bottom}.
     * @return {Array}
     * @see getChildrenOf
     * @see bottom
     */
    Heap.prototype.leafs = function () {
        if (this.heapArray.length === 0) {
            return [];
        }
        var pi = Heap.getParentIndexOf(this.heapArray.length - 1);
        return this.heapArray.slice(pi + 1);
    };
    Object.defineProperty(Heap.prototype, "length", {
        /**
         * Length of the heap. Aliases: {@link size}.
         * @return {Number}
         * @see size
         */
        get: function () {
            return this.heapArray.length;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Heap.prototype, "limit", {
        /**
         * Get length limit of the heap.
         * Use {@link setLimit} or {@link limit} to set the limit.
         * @return {Number}
         * @see setLimit
         */
        get: function () {
            return this._limit;
        },
        /**
         * Set length limit of the heap. Same as using {@link setLimit}.
         * @description If the heap is longer than the limit, the needed amount of leafs are removed.
         * @param {Number} _l Limit, defaults to 0 (no limit). Negative, Infinity, or NaN values set the limit to 0.
         * @see setLimit
         */
        set: function (_l) {
            if (_l < 0 || isNaN(_l)) {
                // NaN, negative, and Infinity are treated as 0
                this._limit = 0;
            }
            else {
                // truncating a floating-point number to an integer
                this._limit = ~~_l;
            }
            this._applyLimit();
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Set length limit of the heap.
     * Same as assigning to {@link limit} but returns NaN if the value was invalid.
     * @param {Number} _l Limit. Negative, Infinity, or NaN values set the limit to 0.
     * @return {Number} The limit or NaN if the value was negative, or NaN.
     * @see limit
     */
    Heap.prototype.setLimit = function (_l) {
        this.limit = _l;
        if (_l < 0 || isNaN(_l)) {
            return NaN;
        }
        else {
            return this._limit;
        }
    };
    /**
     * Top node. Aliases: {@link element}.
     * Same as: {@link top}(1)[0].
     * @return {any} Top node
     * @see top
     */
    Heap.prototype.peek = function () {
        return this.heapArray[0];
    };
    /**
     * Extract the top node (root). Aliases: {@link poll}.
     * @return {any} Extracted top node, undefined if empty
     */
    Heap.prototype.pop = function () {
        var last = this.heapArray.pop();
        if (this.length > 0 && last !== undefined) {
            return this.replace(last);
        }
        return last;
    };
    /**
     * Pushes element(s) to the heap.
     * See also: {@link add} and {@link addAll}.
     * @param  {...any} elements Elements to insert
     * @return {Boolean} True if elements are present
     */
    Heap.prototype.push = function () {
        var elements = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            elements[_i] = arguments[_i];
        }
        if (elements.length < 1) {
            return false;
        }
        else if (elements.length === 1) {
            return this.add(elements[0]);
        }
        else {
            return this.addAll(elements);
        }
    };
    /**
     * Same as push & pop in sequence, but faster
     * @param  {any} element Element to insert
     * @return {any}  Extracted top node
     */
    Heap.prototype.pushpop = function (element) {
        var _a;
        if (this.compare(this.heapArray[0], element) < 0) {
            _a = __read([this.heapArray[0], element], 2), element = _a[0], this.heapArray[0] = _a[1];
            this._sortNodeDown(0);
        }
        return element;
    };
    /**
     * Remove the first occurrence of an element from the heap.
     * @param  {any}   o      Element to be found
     * @param  {Function} callbackFn  Optional equality function, receives (element, needle)
     * @return {Boolean}      True if the heap was modified
     */
    Heap.prototype.remove = function (o, callbackFn) {
        var _this = this;
        if (callbackFn === void 0) { callbackFn = Heap.defaultIsEqual; }
        if (!this.heapArray.length)
            return false;
        if (o === undefined) {
            this.pop();
            return true;
        }
        var queue = [0];
        while (queue.length) {
            var idx = queue.shift();
            if (callbackFn(this.heapArray[idx], o)) {
                if (idx === 0) {
                    this.pop();
                }
                else if (idx === this.heapArray.length - 1) {
                    this.heapArray.pop();
                }
                else {
                    this.heapArray.splice(idx, 1, this.heapArray.pop());
                    this._sortNodeUp(idx);
                    this._sortNodeDown(idx);
                }
                return true;
            }
            else if (this.compare(this.heapArray[idx], o) <= 0) {
                var children = Heap.getChildrenIndexOf(idx).filter(function (c) { return c < _this.heapArray.length; });
                queue.push.apply(queue, __spreadArray([], __read(children), false));
            }
        }
        return false;
    };
    /**
     * Pop the current peek value, and add the new item.
     * @param  {any} element  Element to replace peek
     * @return {any}         Old peek
     */
    Heap.prototype.replace = function (element) {
        var peek = this.heapArray[0];
        this.heapArray[0] = element;
        this._sortNodeDown(0);
        return peek;
    };
    /**
     * Size of the heap
     * @return {Number}
     */
    Heap.prototype.size = function () {
        return this.length;
    };
    /**
     * Return the top (highest value) N elements of the heap.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}    Array of length <= N.
     */
    Heap.prototype.top = function (n) {
        if (n === void 0) { n = 1; }
        if (this.heapArray.length === 0 || n <= 0) {
            // Nothing to do
            return [];
        }
        else if (this.heapArray.length === 1 || n === 1) {
            // Just the peek
            return [this.heapArray[0]];
        }
        else if (n >= this.heapArray.length) {
            // The whole peek
            return __spreadArray([], __read(this.heapArray), false);
        }
        else {
            // Some elements
            return this._topN_push(~~n);
        }
    };
    /**
     * Clone the heap's internal array
     * @return {Array}
     */
    Heap.prototype.toArray = function () {
        return __spreadArray([], __read(this.heapArray), false);
    };
    /**
     * String output, call to Array.prototype.toString()
     * @return {String}
     */
    Heap.prototype.toString = function () {
        return this.heapArray.toString();
    };
    /**
     * Get the element at the given index.
     * @param  {Number} i Index to get
     * @return {any}       Element at that index
     */
    Heap.prototype.get = function (i) {
        return this.heapArray[i];
    };
    /**
     * Get the elements of these node's children
     * @param  {Number} idx Node index
     * @return {Array(any)}  Children elements
     */
    Heap.prototype.getChildrenOf = function (idx) {
        var _this = this;
        return Heap.getChildrenIndexOf(idx)
            .map(function (i) { return _this.heapArray[i]; })
            .filter(function (e) { return e !== undefined; });
    };
    /**
     * Get the element of this node's parent
     * @param  {Number} idx Node index
     * @return {any}     Parent element
     */
    Heap.prototype.getParentOf = function (idx) {
        var pi = Heap.getParentIndexOf(idx);
        return this.heapArray[pi];
    };
    /**
     * Iterator interface
     */
    Heap.prototype[Symbol.iterator] = function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (!this.length) return [3 /*break*/, 2];
                    return [4 /*yield*/, this.pop()];
                case 1:
                    _a.sent();
                    return [3 /*break*/, 0];
                case 2: return [2 /*return*/];
            }
        });
    };
    /**
     * Returns an iterator. To comply with Java interface.
     */
    Heap.prototype.iterator = function () {
        return this.toArray();
    };
    /**
     * Limit heap size if needed
     */
    Heap.prototype._applyLimit = function () {
        if (this._limit > 0 && this._limit < this.heapArray.length) {
            var rm = this.heapArray.length - this._limit;
            // It's much faster than splice
            while (rm) {
                this.heapArray.pop();
                --rm;
            }
        }
    };
    /**
     * Return the bottom (lowest value) N elements of the heap, without corner cases, unsorted
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    Heap.prototype._bottomN_push = function (n) {
        // Use an inverted heap
        var bottomHeap = new Heap(this.compare);
        bottomHeap.limit = n;
        bottomHeap.heapArray = this.heapArray.slice(-n);
        bottomHeap.init();
        var startAt = this.heapArray.length - 1 - n;
        var parentStartAt = Heap.getParentIndexOf(startAt);
        var indices = [];
        for (var i = startAt; i > parentStartAt; --i) {
            indices.push(i);
        }
        var arr = this.heapArray;
        while (indices.length) {
            var i = indices.shift();
            if (this.compare(arr[i], bottomHeap.peek()) > 0) {
                bottomHeap.replace(arr[i]);
                if (i % 2) {
                    indices.push(Heap.getParentIndexOf(i));
                }
            }
        }
        return bottomHeap.toArray();
    };
    /**
     * Move a node to a new index, switching places
     * @param  {Number} j First node index
     * @param  {Number} k Another node index
     */
    Heap.prototype._moveNode = function (j, k) {
        var temp = this.heapArray[j];
        this.heapArray[j] = this.heapArray[k];
        this.heapArray[k] = temp;
    };
    /**
     * Move a node down the tree (to the leaves) to find a place where the heap is sorted.
     * @param  {Number} i Index of the node
     */
    Heap.prototype._sortNodeDown = function (i) {
        var length = this.heapArray.length;
        var originalIndex = i;
        var value = this.heapArray[i];
        var left = 2 * i + 1;
        while (left < length) {
            var right = left + 1;
            var best = right >= length || this.compare(this.heapArray[left], this.heapArray[right]) < 0 ? left : right;
            if (this.compare(this.heapArray[best], value) < 0) {
                this.heapArray[i] = this.heapArray[best];
                i = best;
                left = 2 * i + 1;
            }
            else
                break;
        }
        if (i !== originalIndex) {
            this.heapArray[i] = value;
        }
    };
    /**
     * Move a node up the tree (to the root) to find a place where the heap is sorted.
     * @param  {Number} i Index of the node
     */
    Heap.prototype._sortNodeUp = function (i) {
        var value = this.heapArray[i];
        var originalIndex = i;
        while (i > 0) {
            var pi = Heap.getParentIndexOf(i);
            if (this.compare(value, this.heapArray[pi]) < 0) {
                this.heapArray[i] = this.heapArray[pi];
                i = pi;
            }
            else
                break;
        }
        if (i !== originalIndex) {
            this.heapArray[i] = value;
        }
    };
    /**
     * Return the top (highest value) N elements of the heap, without corner cases, unsorted
     * Implementation: push.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    Heap.prototype._topN_push = function (n) {
        // Use an inverted heap
        var topHeap = new Heap(this._invertedCompare);
        topHeap.limit = n;
        var indices = [0];
        var arr = this.heapArray;
        while (indices.length) {
            var i = indices.shift();
            if (i < arr.length) {
                if (topHeap.length < n) {
                    topHeap.push(arr[i]);
                    indices.push.apply(indices, __spreadArray([], __read(Heap.getChildrenIndexOf(i)), false));
                }
                else if (this.compare(arr[i], topHeap.peek()) < 0) {
                    topHeap.replace(arr[i]);
                    indices.push.apply(indices, __spreadArray([], __read(Heap.getChildrenIndexOf(i)), false));
                }
            }
        }
        return topHeap.toArray();
    };
    /**
     * Return the top (highest value) N elements of the heap, without corner cases, unsorted
     * Implementation: init + push.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    Heap.prototype._topN_fill = function (n) {
        // Use an inverted heap
        var heapArray = this.heapArray;
        var topHeap = new Heap(this._invertedCompare);
        topHeap.limit = n;
        topHeap.heapArray = heapArray.slice(0, n);
        topHeap.init();
        var branch = Heap.getParentIndexOf(n - 1) + 1;
        var indices = [];
        for (var i = branch; i < n; ++i) {
            indices.push.apply(indices, __spreadArray([], __read(Heap.getChildrenIndexOf(i).filter(function (l) { return l < heapArray.length; })), false));
        }
        if ((n - 1) % 2) {
            indices.push(n);
        }
        while (indices.length) {
            var i = indices.shift();
            if (i < heapArray.length) {
                if (this.compare(heapArray[i], topHeap.peek()) < 0) {
                    topHeap.replace(heapArray[i]);
                    indices.push.apply(indices, __spreadArray([], __read(Heap.getChildrenIndexOf(i)), false));
                }
            }
        }
        return topHeap.toArray();
    };
    /**
     * Return the top (highest value) N elements of the heap, without corner cases, unsorted
     * Implementation: heap.
     *
     * @param  {Number} n  Number of elements.
     * @return {Array}     Array of length <= N.
     */
    Heap.prototype._topN_heap = function (n) {
        var topHeap = this.clone();
        var result = [];
        for (var i = 0; i < n; ++i) {
            result.push(topHeap.pop());
        }
        return result;
    };
    /**
     * Return index of the top element
     * @param list
     */
    Heap.prototype._topIdxOf = function (list) {
        if (!list.length) {
            return -1;
        }
        var idx = 0;
        var top = list[idx];
        for (var i = 1; i < list.length; ++i) {
            var comp = this.compare(list[i], top);
            if (comp < 0) {
                idx = i;
                top = list[i];
            }
        }
        return idx;
    };
    /**
     * Return the top element
     * @param list
     */
    Heap.prototype._topOf = function () {
        var list = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            list[_i] = arguments[_i];
        }
        var heap = new Heap(this.compare);
        heap.init(list);
        return heap.peek();
    };
    return Heap;
}());

export { Heap, HeapAsync, Heap as default, toInt };
