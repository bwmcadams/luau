/*
 * Mongo code to test map/reduce Trips data.
 */

m = function() {
    var x = this.direction_id;
    var v = {};
    if (x == 0) { 
       v = {'num_outbound': 1, 'num_inbound': 0};
    } else {
       v = {'num_outbound': 0, 'num_inbound': 1};
    }

    emit(this.route_id, v);
}

r = function(k, vals) {
    var total = {'num_outbound': 0, 'num_inbound': 0};
    for (var x in vals) {
        total['num_outbound'] += vals[x]['num_outbound'];
        total['num_inbound'] += vals[x]['num_inbound'];
    }
    return total;
}


db.trips.mapReduce(m, r)
