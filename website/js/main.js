/******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, { enumerable: true, get: getter });
/******/ 		}
/******/ 	};
/******/
/******/ 	// define __esModule on exports
/******/ 	__webpack_require__.r = function(exports) {
/******/ 		if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 			Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 		}
/******/ 		Object.defineProperty(exports, '__esModule', { value: true });
/******/ 	};
/******/
/******/ 	// create a fake namespace object
/******/ 	// mode & 1: value is a module id, require it
/******/ 	// mode & 2: merge all properties of value into the ns
/******/ 	// mode & 4: return value when already ns object
/******/ 	// mode & 8|1: behave like require
/******/ 	__webpack_require__.t = function(value, mode) {
/******/ 		if(mode & 1) value = __webpack_require__(value);
/******/ 		if(mode & 8) return value;
/******/ 		if((mode & 4) && typeof value === 'object' && value && value.__esModule) return value;
/******/ 		var ns = Object.create(null);
/******/ 		__webpack_require__.r(ns);
/******/ 		Object.defineProperty(ns, 'default', { enumerable: true, value: value });
/******/ 		if(mode & 2 && typeof value != 'string') for(var key in value) __webpack_require__.d(ns, key, function(key) { return value[key]; }.bind(null, key));
/******/ 		return ns;
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 0);
/******/ })
/************************************************************************/
/******/ ({

/***/ "../../website/src/js/components/case-study-card.js":
/*!**************************************************************************************!*\
  !*** /Users/cody/Sites/tech.clickhouse/website/src/js/components/case-study-card.js ***!
  \**************************************************************************************/
/*! no static exports found */
/***/ (function(module, exports) {

eval("function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError(\"Cannot call a class as a function\"); } }\n\nfunction _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if (\"value\" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }\n\nfunction _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }\n\nvar CaseStudyCard = /*#__PURE__*/function () {\n  function CaseStudyCard($el) {\n    _classCallCheck(this, CaseStudyCard);\n\n    this.onOpen = this.onOpen.bind(this);\n    this.onToggle = this.onToggle.bind(this);\n    this.$el = $el;\n    this.$el.addEventListener('click', this.onOpen);\n    this.$el.querySelector('.case-study-card-toggle').addEventListener('click', this.onToggle);\n    this.open = false;\n  }\n\n  _createClass(CaseStudyCard, [{\n    key: \"onOpen\",\n    value: function onOpen() {\n      this.open = true;\n      this.$el.classList.toggle('is-open', this.open);\n      this.$el.classList.toggle('is-closing', !this.open);\n      this.closeOthers();\n    }\n  }, {\n    key: \"onToggle\",\n    value: function onToggle(event) {\n      event.stopPropagation();\n      this.open = !this.$el.classList.contains('is-open');\n      this.$el.classList.toggle('is-open', this.open);\n      this.$el.classList.toggle('is-closing', !this.open);\n      this.closeOthers();\n    }\n  }, {\n    key: \"closeOthers\",\n    value: function closeOthers() {\n      var _this = this;\n\n      if (this.open) {\n        document.querySelectorAll('.case-study-card').forEach(function ($el) {\n          if (!$el.isSameNode(_this.$el)) {\n            $el.classList.toggle('is-closing', $el.classList.contains('is-open'));\n            $el.classList.toggle('is-open', false);\n          }\n        });\n      }\n    }\n  }]);\n\n  return CaseStudyCard;\n}();\n\ndocument.querySelectorAll('.case-study-card').forEach(function ($el) {\n  return new CaseStudyCard($el);\n});\n\n//# sourceURL=webpack:////Users/cody/Sites/tech.clickhouse/website/src/js/components/case-study-card.js?");

/***/ }),

/***/ "../../website/src/js/main.js":
/*!****************************************************************!*\
  !*** /Users/cody/Sites/tech.clickhouse/website/src/js/main.js ***!
  \****************************************************************/
/*! no exports provided */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony import */ var _components_case_study_card__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./components/case-study-card */ \"../../website/src/js/components/case-study-card.js\");\n/* harmony import */ var _components_case_study_card__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_components_case_study_card__WEBPACK_IMPORTED_MODULE_0__);\n/* harmony import */ var _utilities_equalize_heights__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./utilities/equalize-heights */ \"../../website/src/js/utilities/equalize-heights.js\");\n/* harmony import */ var _utilities_equalize_heights__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(_utilities_equalize_heights__WEBPACK_IMPORTED_MODULE_1__);\n\n\n\n//# sourceURL=webpack:////Users/cody/Sites/tech.clickhouse/website/src/js/main.js?");

/***/ }),

/***/ "../../website/src/js/utilities/equalize-heights.js":
/*!**************************************************************************************!*\
  !*** /Users/cody/Sites/tech.clickhouse/website/src/js/utilities/equalize-heights.js ***!
  \**************************************************************************************/
/*! no static exports found */
/***/ (function(module, exports) {

eval("function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }\n\nfunction _nonIterableRest() { throw new TypeError(\"Invalid attempt to destructure non-iterable instance.\\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method.\"); }\n\nfunction _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === \"string\") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === \"Object\" && o.constructor) n = o.constructor.name; if (n === \"Map\" || n === \"Set\") return Array.from(o); if (n === \"Arguments\" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }\n\nfunction _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }\n\nfunction _iterableToArrayLimit(arr, i) { var _i = arr == null ? null : typeof Symbol !== \"undefined\" && arr[Symbol.iterator] || arr[\"@@iterator\"]; if (_i == null) return; var _arr = []; var _n = true; var _d = false; var _s, _e; try { for (_i = _i.call(arr); !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i[\"return\"] != null) _i[\"return\"](); } finally { if (_d) throw _e; } } return _arr; }\n\nfunction _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }\n\nvar $allElements = document.querySelectorAll('[equalize-heights]');\nvar groupedElements = {};\n$allElements.forEach(function ($el) {\n  var group = $el.getAttribute('equalize-heights');\n  groupedElements[group] = groupedElements[group] || [];\n  groupedElements[group].push($el);\n});\n\nfunction resizeElements() {\n  Object.entries(groupedElements).forEach(function (_ref) {\n    var _ref2 = _slicedToArray(_ref, 2),\n        group = _ref2[0],\n        $elements = _ref2[1];\n\n    $elements.forEach(function ($el) {\n      var styles = window.getComputedStyle($el);\n\n      if ('none' === styles.getPropertyValue('display')) {\n        $el.style.display = 'block';\n      }\n\n      $el.style.minHeight = 'auto';\n    });\n    var minHeight = $elements.reduce(function (max, $el) {\n      if ($el.offsetHeight > max) {\n        max = $el.offsetHeight;\n      }\n\n      return max;\n    }, 0);\n    $elements.forEach(function ($el) {\n      $el.style.display = null;\n      $el.style.minHeight = \"\".concat(minHeight, \"px\");\n    });\n  });\n}\n\nwindow.addEventListener('resize', resizeElements);\nwindow.addEventListener('orientationchange', resizeElements);\nresizeElements();\n\n//# sourceURL=webpack:////Users/cody/Sites/tech.clickhouse/website/src/js/utilities/equalize-heights.js?");

/***/ }),

/***/ "../../website/src/scss/bootstrap.scss":
/*!*************************************************************************!*\
  !*** /Users/cody/Sites/tech.clickhouse/website/src/scss/bootstrap.scss ***!
  \*************************************************************************/
/*! exports provided: default */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony default export */ __webpack_exports__[\"default\"] = (__webpack_require__.p + \"./css//bootstrap.css\");\n\n//# sourceURL=webpack:////Users/cody/Sites/tech.clickhouse/website/src/scss/bootstrap.scss?");

/***/ }),

/***/ "../../website/src/scss/main.scss":
/*!********************************************************************!*\
  !*** /Users/cody/Sites/tech.clickhouse/website/src/scss/main.scss ***!
  \********************************************************************/
/*! exports provided: default */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony default export */ __webpack_exports__[\"default\"] = (__webpack_require__.p + \"./css//main.css\");\n\n//# sourceURL=webpack:////Users/cody/Sites/tech.clickhouse/website/src/scss/main.scss?");

/***/ }),

/***/ 0:
/*!*****************************************************************************************************************************************************************************************************!*\
  !*** multi /Users/cody/Sites/tech.clickhouse/website/src/scss/bootstrap.scss /Users/cody/Sites/tech.clickhouse/website/src/scss/main.scss /Users/cody/Sites/tech.clickhouse/website/src/js/main.js ***!
  \*****************************************************************************************************************************************************************************************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

eval("__webpack_require__(/*! /Users/cody/Sites/tech.clickhouse/website/src/scss/bootstrap.scss */\"../../website/src/scss/bootstrap.scss\");\n__webpack_require__(/*! /Users/cody/Sites/tech.clickhouse/website/src/scss/main.scss */\"../../website/src/scss/main.scss\");\nmodule.exports = __webpack_require__(/*! /Users/cody/Sites/tech.clickhouse/website/src/js/main.js */\"../../website/src/js/main.js\");\n\n\n//# sourceURL=webpack:///multi_/Users/cody/Sites/tech.clickhouse/website/src/scss/bootstrap.scss_/Users/cody/Sites/tech.clickhouse/website/src/scss/main.scss_/Users/cody/Sites/tech.clickhouse/website/src/js/main.js?");

/***/ })

/******/ });