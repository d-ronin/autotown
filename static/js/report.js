var boardColors = d3.scale.category20();
var procColors = d3.scale.category20();
var countryColors = d3.scale.category20();
var countries = {};

function drawBoardGraph(data) {
    nv.addGraph(function() {
        var chart = nv.models.discreteBarChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .staggerLabels(true)
            .tooltips(false)
            .valueFormat(d3.format(',f'))
            .color(function(d) { return boardColors(d.key);})
            .showValues(true);

        chart.yAxis.tickFormat(d3.format(',d'));

        var values = d3.entries(data['board']);
        values.sort(function (a, b) {return d3.descending(a.value, b.value); });

        d3.select('#boards svg')
            .datum([{'key': 'Boards', values: values}])
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}

function drawCountryGraph(data) {
    nv.addGraph(function() {
        var chart = nv.models.discreteBarChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .staggerLabels(true)
            .tooltips(false)
            .valueFormat(d3.format(',f'))
            .color(function(d) { return boardColors(d.key);})
            .showValues(true);

        chart.yAxis.tickFormat(d3.format(',d'));

        var values = d3.entries(data['country_board']).map(function(e) {
            return {key: e.key, value: d3.sum(d3.values(e.value))};
        });
        values.sort(function (a, b) {return d3.descending(a.value, b.value); });
        values.splice(20);

        d3.select('#countries svg')
            .datum([{'key': 'Countries', values: values}])
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}

function drawCountryGraphNormalized(data) {
    nv.addGraph(function() {
        var chart = nv.models.discreteBarChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .staggerLabels(true)
            .tooltips(false)
            .valueFormat(d3.format(',.1f'))
            .color(function(d) { return boardColors(d.key);})
            .showValues(true);

        chart.yAxis.tickFormat(d3.format(',d'));

        var values = d3.entries(data['country_board']).map(function(e) {
            return {key: e.key,
                    pop: d3.sum(d3.values(e.value)),
                    value: (countries[e.key] || 0) * d3.sum(d3.values(e.value))};
        }).filter(function(d) { return d.value >= 1; });
        values.sort(function (a, b) {return d3.descending(a.value, b.value); });
        values.splice(20);

        d3.select('#countries-pop svg')
            .datum([{'key': 'Countries', values: values}])
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}

function drawCountryMap(world, boards) {
    boards = boards.filter(function(d) { return d.lon != 0 && d.lat != 0; });

    var width = 800,
        height = 450,
        centered;

    var projection = d3.geo.mercator()
        .scale((width + 1) / 2 / Math.PI)
        .translate([width / 2, height / 2])
        .precision(.1);

    var path = d3.geo.path()
        .projection(projection);

    var graticule = d3.geo.graticule();

    var svg = d3.select("#countrymap").append("svg")
        .attr("width", width)
        .attr("height", height);

    var g = svg.append("g");

    var clickedMap = function(d) {
        var x, y, k;
        if (d && d.lat) {
            var p = projection([d.lon, d.lat]);
            x = p[0];
            y = p[1];
            k = 6;
        } else if (d) {
            var centroid = path.centroid(d);
            x = centroid[0];
            y = centroid[1];
            k = 4;
        } else {
            x = width / 2;
            y = height / 2;
            k = 1;
            centered = null;
        }

        g.selectAll("path")
            .classed("active", centered && function(d) { return d === centered; });

        g.transition()
            .duration(750)
            .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")scale(" + k + ")translate(" + -x + "," + -y + ")")
            .style("stroke-width", 1.5 / k + "px");

        g.selectAll("#countrymap svg .cluster").transition().duration(1200)
            .attr("r", 2/k);
    }

    g.append("rect")
        .attr("class", "background")
        .attr("width", width)
        .attr("height", height)
        .on("click", clickedMap);

    g.append("g")
        .attr("id", "countries")
        .selectAll("path")
        .data(topojson.feature(world, world.objects.countries).features)
        .enter().append("path")
        .attr("d", path)
        .attr("class", "land")
        .on("click", clickedMap);

    g.append("path")
        .datum(topojson.mesh(world, world.objects.countries, function(a, b) { return a !== b; }))
        .attr("class", "boundary")
        .attr("d", path);


    var tooltip = d3.select("#countrymap")
        .append("div")
        .attr("class", "nvtooltip xy-tooltip nv-pointer-events-none")
        .style("position", "absolute")
        .style("z-index", "10")
        .style("visibility", "hidden")
        .text("");

    g.selectAll("#countrymap svg .cluster")
        .data(boards)
        .enter().append("svg:circle")
        .attr("class", function(d) {
            return "cluster board-" + d.name;
        })
        .attr("cx", function(d) {  return projection([d.lon, d.lat])[0]; })
        .attr("cy", function(d) { return projection([d.lon, d.lat])[1]; })
        .attr("fill", function(d) { return boardColors(d.name); })
        .attr("r", 0)
        .on("mouseover", function(d){
            var ref = (d.ref || d.git_hash);
            tooltip.text("A " + d.name + " on " + ref + " in " + d.city + ", " + d.region + ", " + d.country);
            return tooltip.style("visibility", "visible");})
        .on("mousemove", function() {
            return tooltip.style("top", (d3.event.pageY-10)+"px")
                .style("left",(d3.event.pageX+10)+"px");})
        .on("mouseout", function()
            {return tooltip.style("visibility", "hidden");})
        .on("click", clickedMap);


    g.selectAll("#countrymap svg .cluster")
        .data(boards)
        .exit().remove();

    g.selectAll("#countrymap svg .cluster")
        .data(boards)
        .transition()
        .duration(3000)
        .ease('bounce')
        .attr("r", 2);


    d3.select(self.frameElement).style("height", height + "px");
}

var processorTypes = {
    "AQ32":'F4',
    "Brain":'F4',
    "BrainRE1":'F4',
    "CC3D":'F1',
    "DTFc": 'F3',
    "Lux":'F3',
    "Naze":'F1',
    "Naze32Pro":'F3',
    "Revo": 'F4',
    "RevoMini":'F4',
    "Revolution":'F4',
    "Sparky":'F3',
    "Sparky2":'F4',
    "colibri": 'F4',
    "flyingf3":'F3',
    "quanton":'F4',
};

function drawProcessorGraph(data) {
    nv.addGraph(function() {
        var chart = nv.models.discreteBarChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .staggerLabels(true)
            .color(function(d) { return procColors(d.key); })
            .tooltips(false)
            .valueFormat(d3.format(',f'))
            .showValues(true);

        chart.yAxis.tickFormat(d3.format(',d'));

        var values = {'F1': 0, 'F3': 0, 'F4': 0};
        d3.map(data['board']).forEach(function(k, v) {
            if (processorTypes[k]) {
                values[processorTypes[k]] += v;
            }
        });

        d3.select('#procs svg')
            .datum([{'key': 'Procs', values: d3.entries(values)}])
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}

function drawOSGraph(data) {
    nv.addGraph(function() {
        var chart = nv.models.discreteBarChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .staggerLabels(true)
            .tooltips(false)
            .valueFormat(d3.format(',f'))
            .showValues(true);

        chart.yAxis.tickFormat(d3.format(',d'));

        var entries = d3.entries(data['os_board']).map(function(e) {
            return {key: e.key, value: d3.sum(d3.values(e.value))};
        });

        d3.select('#oses svg')
            .datum([{key: 'OSes', values: entries}])
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}

var interestingRegex = /^(preview-|Release-)/;

function drawVersionGraph(data) {
    nv.addGraph(function() {
        var chart = nv.models.pieChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .donut(true)
            .valueFormat(d3.format(',f'))
            .showLabels(true);

        var entries = d3.entries(data['version_board']).map(function(e) {
            return {key: e.key,
                    value: d3.sum(d3.values(e.value)),
                    disabled: !interestingRegex.test(e.key),
                   };
        });

        d3.select("#versions svg")
            .datum(entries)
            .transition().duration(1200)
            .call(chart);

        return chart;
    });
}

function showRecent(data) {
    data.reverse();
    var ul = d3.select("#recent").append("ul");

    ul.selectAll("ul").data(data)
        .enter().append("li")
        .html(function(d) {
            console.log(d);
            var ts = moment(d.timestamp).fromNow();
            var t = "<span title=\"" + d.timestamp + "\">" + ts + "</span> - " + d.os  + " from " + [d.city, d.region, d.country].join(", ");
            if (d.boards) {
                t += " with <tt>" + d.boards.join(", ") + "</tt>";
            }
            return t;
        });
}

function grokCountries(data) {
    var pops = {};
    var min = 100000000000, max = 0;
    for (var i = 0; i < data.length; i++) {
        var c = data[i];
        var pop = +c.Population;
        if (pop == 0) {
            continue;
        }
        min = Math.min(min, pop);
        max = Math.max(max, pop);
        pops[c['Country code']] = pop;
    }

    d3.map(pops).forEach(function(k, v) {
        countries[k] = 1000000/v;
    });
}

function drawWeeklyAdditions(data, dest, colors, options, accessor) {
    var timeline = {};
    options.forEach(function(i) { timeline[i] = {}; });
    data.forEach(function(v) {
        var t = accessor(v);
        if (!timeline[t]) { t = 'Other'; }

        var d = new Date(v.oldest.substr(0, 10));
        d.setHours(0);
        d.setMinutes(0);
        d.setSeconds(0);

        // Ensure we've got a value for every proc type.  There was one week
        // with zero F3s, and that breaks the charting stuff. :(
        options.forEach(function(proc) { timeline[proc][+d] |= 0; });

        timeline[t][+d]++;
    });
    var tldata = [];
    options.forEach(function(proc) {
        var values = [];
        d3.map(timeline[proc]).forEach(function(k, v) {
            values.push({x: +k, y: v});
        });
        values.sort(function (a, b) {return d3.ascending(a.x, b.x); });
        // Reduce value set to sum of seven day windows.
        var v7 = [];
        for (var i = 7; i<values.length; i++) {
            v7.push({x: values[i].x, y: d3.sum(values.slice(i-7, i), function(d) { return d.y; })});
        }
        tldata.push({key: proc, values: v7});
    });

    nv.addGraph(function() {
        var chart = nv.models.stackedAreaChart()
            .useInteractiveGuideline(true)
            .showControls(true)
            .clipEdge(true)
            .color(function(d) { return colors(d.key);})
            .interpolate("basis");

        chart.xAxis.tickFormat(function(d) {
            return d3.time.format('%x')(new Date(d));
        });

        chart.yAxis.tickFormat(d3.format(',f'));

        d3.select(dest)
            .datum(tldata)
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}

function drawPlots() {
    // Charts drawn with basic data.
    d3_queue.queue()
        .defer(d3_request.requestTsv, "/static/countries.tsv")
        .defer(d3_request.requestJson, "//dronin-autotown.appspot.com/api/usageStats")
        .awaitAll(function(error, results) {
            if (error) {
                console.log(error);
                return;
            }

            grokCountries(results[0]);
            var data = results[1];

            var total = 0;
            d3.map(data['board']).forEach(function(k, v) { total += v; });
            d3.select("#totalboards").text(d3.format(",d")(total));
            drawBoardGraph(data);
            drawOSGraph(data);
            drawCountryGraph(data);
            drawCountryGraphNormalized(data);
            drawVersionGraph(data);
            drawProcessorGraph(data);
        });

    // Charts that need a bit more data.
    d3_queue.queue()
        .defer(d3_request.requestJson, "/static/lib/world-110m.json")
        .defer(d3_request.requestCsv, "//dronin-autotown.appspot.com/api/usageDetails")
        .awaitAll(function(error, results) {
            if (error) {
                console.log(error);
                return;
            }
            drawCountryMap(results[0], results[1]);
            /*
            drawWeeklyAdditions(results[1], '#weekly svg', boardColors,
                                ["AQ32", "Brain", "BrainRE1", "CC3D", "Naze",
                                 "Revo", "Sparky", "Sparky2", "Other"],
                                function(d) { return d.name; });
            */
            drawWeeklyAdditions(results[1], '#weekly-proc svg', procColors,
                                ['F1', 'F3', 'F4', 'Other'],
                                function(d) { return processorTypes[d.name]; });
        });
}
