function flotSmartRescale(data, from, to) {

    // Trim the dataseries, but add a small extra to min and max to avoid clipping.
    var rescaledSeries = [];
    from -= 1;
    to += 1;

    // Iterate data.
    $.each(data, function(e, obj){
        var newData = [];

        $.each(obj["data"], function(e, val){
            if ((val[0] >= from) && (val[0] <= to)) {
                newData.push(val);
            }
        });

        rescaledSeries.push({label: obj.label, color: obj.color, data: newData});
    });

    return rescaledSeries;
}