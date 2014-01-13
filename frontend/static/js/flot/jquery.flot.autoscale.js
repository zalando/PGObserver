function flotSmartRescale(data, from, to) {

    // Trim the dataseries, but add a small extra to min and max to avoid clipping.
    var rescaledSeries = [];

    // Iterate data. Add an extra minute to the timespan to avoid clipping.
    $.each(data, function(e, obj){
        var newData = [];
        from -= 60000;
        to += 60000;

        $.each(obj.data, function(e, val){
            if ((val[0] >= from) && (val[0] <= to)) {
                newData.push(val);
            }
        });

        var newSerie = {label: obj.label, color: obj.color, data: newData};
        if (obj.yaxis) newSerie.yaxis = obj.yaxis;

        rescaledSeries.push(newSerie);
    });

    return rescaledSeries;
}