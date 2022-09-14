/*
 * d3-gantt.js by @serxa
 * Based on https://github.com/ydb-platform/ydb/blob/stable-22-2/library/cpp/lwtrace/mon/static/js/d3-gantt.js
 */
 d3.gantt = function() {
    function gantt(input_data) {
        data = input_data;

        initAxis();

        // create svg element
        svg = d3.select(selector)
          .append("svg")
            .attr("class", "chart")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        ;

        // create arrowhead marker
        defs = svg.append("defs");
        defs.append("marker")
            .attr("id", "arrow")
            .attr("viewBox", "0 -5 10 10")
            .attr("refX", 5)
            .attr("refY", 0)
            .attr("markerWidth", 4)
            .attr("markerHeight", 4)
            .attr("orient", "auto")
          .append("path")
            .attr("d", "M0,-5L10,0L0,5")
            .attr("class","arrowHead")
        ;

        zoom = d3.zoom()
            .scaleExtent([0.1, 1000])
            //.translateExtent([0, 0], [1000,0])
            .on("zoom", function() {
                if (tipShown != null) {
                    tip.hide(tipShown);
                }
                var tr = d3.event.transform;
                xZoomed = tr.rescaleX(x);
                svg.select("g.x.axis").call(xAxis.scale(xZoomed));

                var dy = d3.event.sourceEvent.screenY - zoom.startScreenY;
                var newScrollTop = documentBodyScrollTop() - dy;
                window.scrollTo(documentBodyScrollLeft(), newScrollTop);
                documentBodyScrollTop(newScrollTop);
                zoom.startScreenY = d3.event.sourceEvent.screenY;

                zoomContainer1.attr("transform", "translate(" + tr.x + ",0) scale(" + tr.k + ",1)");
                zoomContainer2.attr("transform", "translate(" + tr.x + ",0) scale(" + tr.k + ",1)");

                render();
            })
            .on("start", function() {
                zoom.startScreenY = d3.event.sourceEvent.screenY;
            })
            .on("end", function() {
            })
        ;

        svgChartContainer = svg.append('g')
            .attr("transform", "translate(" + margin.left + ", " + margin.top + ")")
        ;
        svgChart = svgChartContainer.append("svg")
            .attr("top", 0)
            .attr("left", 0)
            .attr("width", width)
            .attr("height", height)
            .attr("viewBox", "0 0 " + width + " " + height)
        ;

        zoomContainer1 = svgChart.append("g");

        zoomPanel = svgChart.append("rect")
            .attr("class", "zoom-panel")
            .attr("width", width)
            .attr("height", height)
            .call(zoom)
        ;

        zoomContainer2 = svgChart.append("g");
        bandsSvg = zoomContainer2.append("g");

        // tooltips for bands
        var maxTipHeight = 130;
        const tipDirection = d => y(d.band) - maxTipHeight < documentBodyScrollTop()? 's': 'n';
        tip = d3.tip()
            .attr("class", "d3-tip")
            .offset(function(d) {
                // compute x to return tip in chart region
                var t0 = (d.t1 + d.t2) / 2;
                var t1 = Math.min(Math.max(t0, xZoomed.invert(0)), xZoomed.invert(width));
                var dir = tipDirection(d);
                return [dir === 'n'? -10 : 10, xZoomed(t1) - xZoomed(t0)];
            })
            .direction(tipDirection)
            .html(d => "<pre>" + d.text + "</pre>")
        ;

        bandsSvg.call(tip);

        render();

        // container for non-zoomable elements
        fixedContainer = svg.append("g")
            .attr("transform", "translate(" + margin.left + ", " + margin.top + ")")
        ;

        // create x axis
        fixedContainer.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0, " + (height - margin.top - margin.bottom) + ")")
          .transition()
            .call(xAxis)
        ;

        // create y axis
        fixedContainer.append("g")
            .attr("class", "y axis")
          .transition()
            .call(yAxis)
        ;

        // make y axis ticks draggable
        var ytickdrag = d3.drag()
            .on("drag", function(d) {
                var ypos = d3.event.y - margin.top;
                var index = Math.floor((ypos / y.step()));
                index = Math.min(Math.max(index, 0), this.initDomain.length - 1);
                if (index != this.curIndex) {
                    var newDomain = [];
                    for (var i = 0; i < this.initDomain.length; ++i) {
                        newDomain.push(this.initDomain[i]);
                    }
                    newDomain.splice(this.initIndex, 1);
                    newDomain.splice(index, 0, this.initDomain[this.initIndex]);

                    this.curIndex = index;
                    this.curDomain = newDomain;
                    y.domain(newDomain);

                    // rearange y scale and axis
                    svg.select("g.y.axis").transition().call(yAxis);

                    // rearange other stuff
                    render(-1, true);
                }
            })
            .on("start", function(d) {
                var ypos = d3.event.y - margin.top;
                this.initIndex = Math.floor((ypos / y.step()));
                this.initDomain = y.domain();
            })
            .on("end", function(d) {
                svg.select("g.y.axis").call(yAxis);
            })
        ;
        svg.selectAll("g.y.axis .tick")
            .call(ytickdrag)
        ;

        // right margin
        var rmargin = fixedContainer.append("g")
            .attr("id", "right-margin")
            .attr("transform", "translate(" + width + ", 0)")
        ;
        rmargin.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 1)
            .attr("height", height - margin.top - margin.bottom)
        ;

        // top margin
        var tmargin = fixedContainer.append("g")
            .attr("id", "top-margin")
            .attr("transform", "translate(0, 0)")
        ;
        tmargin.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", width)
            .attr("height", 1)
        ;

        // ruler
        ruler = fixedContainer.append("g")
            .attr("id", "ruler")
            .attr("transform", "translate(0, 0)")
        ;
        ruler.append("rect")
            .attr("id", "ruler-line")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", "1")
            .attr("height", height - margin.top - margin.bottom + 8)
        ;
        ruler.append("rect")
            .attr("id", "bgrect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 0)
            .attr("height", 0)
            .style("fill", "white")
        ;
        ruler.append("text")
            .attr("x", 0)
            .attr("y", height - margin.top - margin.bottom + 16)
            .attr("dy", "0.71em")
            .text("0")
        ;

        svg.on('mousemove', function() {
            positionRuler(d3.event.pageX);
        });

        // scroll handling
        window.onscroll = function myFunction() {
            documentBodyScrollLeft(document.body.scrollLeft);
            documentBodyScrollTop(document.body.scrollTop);
            var scroll = scrollParams();

            svgChartContainer
                .attr("transform", "translate(" + margin.left
                                         + ", " + (margin.top + scroll.y1) + ")");
            svgChart
                .attr("viewBox", "0 " + scroll.y1 + " " + width + " " + scroll.h)
                .attr("height", scroll.h);
            tmargin
                .attr("transform", "translate(0," + scroll.y1 + ")");
            fixedContainer.select(".x.axis")
                .attr("transform", "translate(0," + scroll.y2 + ")");
            rmargin.select("rect")
                .attr("y", scroll.y1)
                .attr("height", scroll.h);
            ruler.select("#ruler-line")
                .attr("y", scroll.y1)
                .attr("height", scroll.h);

            positionRuler();
        }

        // render axis
        svg.select("g.x.axis").call(xAxis);
        svg.select("g.y.axis").call(yAxis);

        // update to initiale state
        window.onscroll(0);

        return gantt;
    }

// private:

    var keyFunction = function(d) {
        return d.t1.toString() + d.t2.toString() + d.band.toString();
    }

    var bandTransform = function(d) {
        return "translate(" + x(d.t1) + "," + y(d.band) + ")";
    }

    var xPixel = function(d) {
        return xZoomed.invert(1) - xZoomed.invert(0);
    }

    var render = function(t0, smooth) {
        // Save/restore last t0 value
        if (!arguments.length || t0 == -1) {
            t0 = render.t0;
        }
        render.t0 = t0;
        smooth = smooth || false;

        // Create rectangles for bands
        bands = bandsSvg.selectAll("rect.bar")
          .data(data, keyFunction);
        bands.exit().remove();
        bands.enter().append("rect")
            .attr("class", "bar")
            .attr("vector-effect", "non-scaling-stroke")
            .style("fill", d => d.color)
            .on('click', function(d) {
                if (tipShown != d) {
                    tipShown = d;
                    tip.show(d);
                } else {
                    tipShown = null;
                    tip.hide(d);
                }
            })
          .merge(bands)
          .transition().duration(smooth? 250: 0)
            .attr("y", 0)
            .attr("transform", bandTransform)
            .attr("height", y.bandwidth())
            .attr("width", d => Math.max(1*xPixel(), x(d.t2) - x(d.t1)))
        ;

        var emptyMarker = bandsSvg.selectAll("text")
          .data(data.length == 0? ["no data to show"]: []);
        emptyMarker.exit().remove();
        emptyMarker.enter().append("text")
            .text(d => d)
        ;
    }

    function initAxis() {
        x = d3.scaleLinear()
            .domain([timeDomainStart, timeDomainEnd])
            .range([0, width])
            //.clamp(true); // dosn't work with zoom/pan
        xZoomed = x;
        y = d3.scaleBand()
            .domain(Object.values(data).map(d => d.band).sort())
            .rangeRound([0, height - margin.top - margin.bottom])
            .padding(0.5);
        xAxis = d3.axisBottom()
            .scale(x)
            //.tickSubdivide(true)
            .tickSize(8)
            .tickPadding(8);
        yAxis = d3.axisLeft()
            .scale(y)
            .tickSize(0);
    }

    // slow function wrapper
    var documentBodyScrollLeft = function(value) {
        if (!arguments.length) {
            if (documentBodyScrollLeft.value === undefined) {
                documentBodyScrollLeft.value = document.body.scrollLeft;
            }
            return documentBodyScrollLeft.value;
        } else {
            documentBodyScrollLeft.value = value;
        }
    }

    // slow function wrapper
    var documentBodyScrollTop = function(value) {
        if (!arguments.length) {
            if (!documentBodyScrollTop.value === undefined) {
                documentBodyScrollTop.value = document.body.scrollTop;
            }
            return documentBodyScrollTop.value;
        } else {
            documentBodyScrollTop.value = value;
        }
    }

    var scrollParams = function() {
        var y1 = documentBodyScrollTop();
        var y2 = y1 + window.innerHeight - margin.footer;
        y2 = Math.min(y2, height - margin.top - margin.bottom);
        var h = y2 - y1;
        return {
            y1: y1,
            y2: y2,
            h: h
        };
    }

    var posTextFormat = d3.format(".1f");

    var positionRuler = function(pageX) {
        if (!arguments.length) {
            pageX = positionRuler.pageX || 0;
        } else {
            positionRuler.pageX = pageX;
        }

        // x-coordinate
        if (!positionRuler.svgLeft) {
            positionRuler.svgLeft = svg.node().getBoundingClientRect().x;
        }

        var xpos = pageX - margin.left + 1 - positionRuler.svgLeft;
        var tpos = xZoomed.invert(xpos);
        tpos = Math.min(Math.max(tpos, xZoomed.invert(0)), xZoomed.invert(width));
        ruler.attr("transform", "translate(" + xZoomed(tpos) + ", 0)");
        var posText = posTextFormat(tpos);

        // scroll-related
        var scroll = scrollParams();

        var text = ruler.select("text")
            .attr("y", scroll.y2 + 16)
        ;

        // getBBox() is very slow, so compute symbol width once
        var xpadding = 5;
        var ypadding = 5;
        if (!positionRuler.bbox) {
            positionRuler.bbox = text.node().getBBox();
        }

        text.text(posText);
        var textWidth = 10 * posText.length;
        ruler.select("#bgrect")
            .attr("x", -textWidth/2 - xpadding)
            .attr("y", positionRuler.bbox.y - ypadding)
            .attr("width", textWidth + (xpadding*2))
            .attr("height", positionRuler.bbox.height + (ypadding*2))
        ;

        render(tpos);
    }

// public:

    gantt.width = function(value) {
        if (!arguments.length)
            return width;
        width = +value;
        return gantt;
    }

    gantt.height = function(value) {
        if (!arguments.length)
            return height;
        height = +value;
        return gantt;
    }

    gantt.selector = function(value) {
        if (!arguments.length)
            return selector;
        selector = value;
        return gantt;
    }

    gantt.timeDomain = function(value) {
        if (!arguments.length)
            return [timeDomainStart, timeDomainEnd];
        timeDomainStart = value[0];
        timeDomainEnd = value[1];
        return gantt;
    }

    gantt.data = function() {
        return data;
    }

    // constructor

    // Config
    var margin = { top: 20, right: 40, bottom: 20, left: 200, footer: 100 },
        height = document.body.clientHeight - margin.top - margin.bottom - 5,
        width = document.body.clientWidth - margin.right - margin.left - 5,
        selector = 'body',
        timeDomainStart = 0,
        timeDomainEnd = 1000,
        scales = {};
    ;

    // View
    var x = null,
        xZoomed = null,
        y = null,
        xAxis = null,
        yAxis = null,
        svg = null,
        defs = null,
        svgChartContainer = null,
        svgChart = null,
        zoomPanel = null,
        zoomContainer1 = null,
        zoomContainer2 = null,
        fixedContainer = null,
        zoom = null,
        bandsSvg = null,
        bands = null,
        tip = null,
        tipShown = null,
        ruler = null
    ;

    // Model
    var data = null;

    return gantt;
}
