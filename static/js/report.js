function drawBoardGraph(data) {
    nv.addGraph(function() {
        var chart = nv.models.discreteBarChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .staggerLabels(true)
            .tooltips(false)
            .valueFormat(d3.format(',f'))
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
            .showValues(true);

        chart.yAxis.tickFormat(d3.format(',d'));

        var values = d3.entries(data['country_board']).map(function(e) {
            return {key: e.key, value: d3.sum(d3.values(e.value))};
        });
        values.sort(function (a, b) {return d3.descending(a.value, b.value); });

        d3.select('#countries svg')
            .datum([{'key': 'Countries', values: values}])
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}

var processorTypes = {
    "AQ32":'F4',
    "Brain":'F4',
    "BrainRE1":'F4',
    "CC3D":'F1',
    "Lux":'F3',
    "Naze":'F1',
    "Naze32Pro":'F3',
    "RevoMini":'F4',
    "Sparky":'F3',
    "Sparky2":'F4',
    "flyingf3":'F3',
    "quanton":'F4',
};

function drawProcessorGraph(data) {
    nv.addGraph(function() {
        var chart = nv.models.discreteBarChart()
            .x(function(d) { return d.key })
            .y(function(d) { return d.value })
            .staggerLabels(true)
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
