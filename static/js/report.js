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

        d3.select('#chart svg')
            .datum([{'key': 'Stuff',
                     values: d3.entries(data["board"])}])
            .transition().duration(500)
            .call(chart)
        ;

        nv.utils.windowResize(chart.update);

        return chart;
    });
}
