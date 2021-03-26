var data_sizes =
    [
        {id: "10000000", name: "10 mln."},
        {id: "100000000", name: "100 mln."},
        {id: "1000000000", name: "1 bn."}
    ];


var systems = [];
var systems_full = [];
var system_kinds = [];
var systems_uniq = {};
for (r in results) {
    if (systems_uniq[results[r].system])
        continue;
    systems_uniq[results[r].system] = 1;
    systems.push(results[r].system);
    systems_full.push(results[r].system_full);
    system_kinds.push(results[r].kind);
}

var runs = ["first (cold cache)", "second", "third"];
var current_runs = ['1', '2'];

try {
    var state = JSON.parse(decodeURIComponent(window.location.hash.substring(1)));
    current_data_size = state[0];
    current_systems = state[1];
    current_runs = state[2];
} catch (e) {
}

if (!current_systems.length) {
    current_systems = systems;
}

function update_hash() {
    window.location.hash = JSON.stringify([current_data_size, current_systems, current_runs]);
}


function generate_selectors(elem) {
    var html = '<div id="systems_selector"><h3>Compare</h3>';

    var available_results = results;

    if (current_data_size) {
        available_results = results.filter(function (run) {
            return run.data_size == current_data_size;
        });
    }
    var available_systems_for_current_data_size = available_results.map(function (run) {
        return run.system;
    });

    for (var i = 0; i < systems.length; i++) {
        var selected = current_systems.indexOf(systems[i]) != -1;
        var available = available_systems_for_current_data_size.indexOf(systems[i]) != -1;
        var button_class = 'btn-outline-dark';
        if (system_kinds[i] == 'cloud' || system_kinds[i] == 'vps') {
            button_class = 'btn-outline-primary';
        } else if (system_kinds[i] == 'desktop' || system_kinds[i] == 'laptop') {
            button_class = 'btn-outline-secondary';
        };

        html += '<button type="button" class="btn btn-sm mr-2 mb-2 ' + button_class +
            (selected && available ? ' active' : '') +
            (available ? '' : ' disabled') + '"';
        if (systems_full[i]) {
            html += ' data-toggle="tooltip" data-placement="bottom" title="' + systems_full[i] + '"';
        }
        html += '>' + systems[i] + '</button>';
    }

    if (current_data_size) {
        html += '</div><div id="data_size_selector"><h3>Dataset&nbsp;size</h3>';

        for (var i = 0; i < data_sizes.length; i++) {
            html += '<button type="button" class="btn btn-sm btn-outline-dark mr-2 mb-2' + (data_sizes[i].id == current_data_size ? ' active' : '') + '" data-size-id="' + data_sizes[i].id + '">' + data_sizes[i].name + '</button>';
        }
    }

    html += '</div><div id="runs_selector"><h3>Run</h3>';

    for (var i = 0; i < runs.length; i++) {
        html += '<button type="button" class="btn btn-sm btn-outline-dark mr-2 mb-2' + (current_runs.indexOf(String(i)) != -1 ? ' active' : '') + '" data-run-id="' + i + '">' + runs[i] + '</button>';
    }

    html += '</div>';

    elem.html(html);

    $('button').focus(function() {
       $('[data-toggle="tooltip"]').tooltip('hide');
    });

    $('#systems_selector button:not(.disabled)').click(function (event) {
        event.preventDefault();
        var target = $(event.target || event.srcElement);

        if (target.hasClass("active") && current_systems.length == 1) {
            return;
        }

        target.toggleClass("active");

        current_systems = $.map($('#systems_selector button'), function (elem) {
            return $(elem).hasClass("active") ? $(elem).html() : null
        }).filter(function (x) {
            return x;
        });

        update_hash();
        generate_selectors(elem);
        generate_comparison_table();
        generate_diagram();
    });

    if (current_data_size) {
        $('#data_size_selector button').click(function (event) {
            event.preventDefault();
            var target = $(event.target || event.srcElement);

            current_data_size = target.attr("data-size-id");

            update_hash();
            generate_selectors(elem);
            generate_comparison_table();
            generate_diagram();
        });
    }

    $('#runs_selector button').click(function (event) {
        event.preventDefault();
        var target = $(event.target || event.srcElement);

        if (target.hasClass("active") && current_runs.length == 1) {
            return;
        }

        target.toggleClass("active");

        current_runs = $.map($('#runs_selector button'), function (elem) {
            return $(elem).hasClass("active") ? $(elem).attr("data-run-id") : null
        }).filter(function (x) {
            return x;
        });

        update_hash();
        generate_selectors(elem);
        generate_comparison_table();
        generate_diagram();
    });
}

function format_number_cell(value, ratio) {
    var html = "";

    var redness = (ratio - 1) / ratio;
    var blackness = ratio < 10 ? 0 : ((ratio - 10) / ratio / 2);

    var color = !value ? "#FFF" :
        ratio == 1 ?
            ("rgba(0, 255, 0, 1)") :
            ("rgba(" + ~~(255 * (1 - blackness)) + ", 0, 0, " + redness + ")");

    html += "<td style='background-color: " + color + "'>";
    html += value ?
        (ratio == 1 ? "" : ("×" + ratio.toFixed(2))) + "&nbsp;<span style='color: #888;'>(" + value.toFixed(3) + "&nbsp;s.)</span>" :
        "—";
    html += "</td>";

    return html;
}

/* Ratio of execution time to best execution time:
 * system index -> run index -> query index -> ratio.
 */
var ratios = [];


function generate_comparison_table() {
    ratios = [];

    var filtered_results = results;
    if (current_data_size) {
        filtered_results = filtered_results.filter(function (x) {
            return x.data_size == current_data_size;
        });
    }
    filtered_results = filtered_results.filter(function (x) {
        return current_systems.indexOf(x.system) != -1;
    });

    var html = "";

    html += "<table class='table table-bordered table-sm'>";
    html += "<tr>";
    html += "<th><input id='query_checkbox_toggler' type='checkbox' checked /></th>";
    html += "<th>Query</th>";
    for (var j = 0; j < filtered_results.length; j++) {
        html += "<th colspan='" + current_runs.length + "'";
        if (filtered_results[j].system_full) {
            html += ' data-toggle="tooltip" title="' + filtered_results[j].system_full + '"';
        }
        html += " class='text-center'>" + filtered_results[j].system.replace(/ /g, '&nbsp;') +
            (filtered_results[j].version ? " (" + filtered_results[j].version + ")" : "") + "</th>";
    }
    html += "</tr>";

    for (var i = 0; i < queries.length; i++) {
        html += "<tr>";
        html += "<td><input id='query_checkbox" + i + "' type='checkbox' " +
            ($('#query_checkbox' + i).length == 0 || $('#query_checkbox' + i).is(':checked') ? "checked" : "") + " /></td>";

            html += "<td class='benchmark-query-cell-wrapper'><div class='benchmark-query-cell'>" + queries[i].query + "</div></td>";

        // Max and min execution time per system, for each of three runs
        var minimums = [0, 0, 0], maximums = [0, 0, 0];

        for (var j = 0; j < filtered_results.length; j++) {
            for (var current_run_idx = 0; current_run_idx < current_runs.length; current_run_idx++) {
                var k = current_runs[current_run_idx];
                var value = filtered_results[j].result[i][k];

                if (value && (!minimums[k] || value < minimums[k])) {
                    minimums[k] = value;

                    // Ignore below 10ms
                    if (minimums[k] < 0.01) {
                        minimums[k] = 0.01;
                    }
                }

                if (value > maximums[k]) {
                    maximums[k] = value;
                }
            }
        }

        for (var j = 0; j < filtered_results.length; j++) {
            if (!ratios[j]) {
                ratios[j] = [];
            }

            for (var current_run_idx = 0; current_run_idx < current_runs.length; current_run_idx++) {
                var k = current_runs[current_run_idx];
                var value = filtered_results[j].result[i][k];

                var ratio = value / minimums[k];

                ratios[j][k] = ratios[j][k] || [];

                if (ratio && ratio <= 1) {
                    ratio = 1;
                }

                ratios[j][k].push(ratio);

                html += format_number_cell(value, ratio);
            }
        }
        html += "</tr>";
    }

    if (current_systems.length) {
        html += "<tr>";
        html += "<td rowspan='2'></td>";
        html += "<td rowspan='2'>Geometric mean of ratios</td>";

        for (var j = 0; j < filtered_results.length; j++) {
            for (var k = 0; k < current_runs.length; k++) {
                html += "<th id='totals" + j + "_" + current_runs[k] + "' class='number_cell text-center'></th>";
            }
        }

        html += "</tr>";
        html += "<tr>";

        for (var j = 0; j < filtered_results.length; j++) {
            html += "<th id='absolute_totals" + j + "' colspan='" + current_runs.length + "' class='number_cell text-center'></th>";
        }

        html += "</tr>";
    }

    html += "</table>";

    $('#comparison_table').html(html);

    for (var i = 0; i < queries.length; i++) {
        $('#query_checkbox' + i).click(function () {
            calculate_totals();
            generate_diagram();
        });
    }
    $('#query_checkbox_toggler').click(function () {
        for (var i = 0; i < queries.length; i++) {
            var item = $('#query_checkbox' + i);
            item.prop("checked", !item.prop("checked"));
        }
    });

    calculate_totals();
}


function calculate_totals() {
    if (!current_systems.length) return;
    var filtered_results = results;
    if (current_data_size) {
        filtered_results = filtered_results.filter(function (x) {
            return x.data_size == current_data_size;
        });
    }

    filtered_results = filtered_results.filter(function (x) {
        return current_systems.indexOf(x.system) != -1;
    });

    var total_ratios = [];

    for (var j = 0; j < filtered_results.length; j++) {
        for (var current_run_idx = 0; current_run_idx < current_runs.length; current_run_idx++) {
            var k = current_runs[current_run_idx];

            var current_ratios = ratios[j][k].filter(
                function (x, i) {
                    return x && $("#query_checkbox" + i).is(':checked');
                }
            );

            var ratio = Math.pow(
                current_ratios.reduce(
                    function (acc, cur) {
                        return acc * cur;
                    },
                    1),
                1 / current_ratios.length);

            total_ratios[j] = total_ratios[j] || 1;
            total_ratios[j] *= ratio;

            $("#totals" + j + "_" + k).attr("data-ratio", ratio).html("x" + ratio.toFixed(2));
        }
    }

    for (var j = 0; j < filtered_results.length; j++) {
        var total_ratio = Math.pow(total_ratios[j], 1 / current_runs.length);
        $("#absolute_totals" + j).attr("data-ratio", total_ratio).html("x" + total_ratio.toFixed(2));
    }
}


function generate_diagram() {
    var html = "";
    var filtered_results = results;
    if (current_data_size) {
        filtered_results = filtered_results.filter(function (x) {
            return x.data_size == current_data_size && current_systems.indexOf(x.system) != -1;
        });
    }
    filtered_results = filtered_results.filter(function (x) {
        return current_systems.indexOf(x.system) != -1;
    });

    var max_ratio = 1;
    var min_ratio = 0;

    var max_total_ratio = 1;
    var min_total_ratio = 0;

    for (var j = 0; j < filtered_results.length; j++) {
        for (var current_run_idx = 0; current_run_idx < current_runs.length; current_run_idx++) {
            var k = current_runs[current_run_idx];
            var ratio = +$("#totals" + j + "_" + k).attr("data-ratio");

            if (ratio > max_ratio) {
                max_ratio = ratio;
            }

            if (!min_ratio || ratio < min_ratio) {
                min_ratio = ratio;
            }
        }

        var total_ratio = +$("#absolute_totals" + j).attr("data-ratio");

        if (total_ratio > max_total_ratio) {
            max_total_ratio = total_ratio;
        }

        if (!min_total_ratio || total_ratio < min_total_ratio) {
            min_total_ratio = total_ratio;
        }
    }

    html += "<table class='table table-borderless table-sm'>";
    var table_rows = [];

    for (var j = 0; j < filtered_results.length; j++) {
        var total_ratio = +$("#absolute_totals" + j).attr("data-ratio");
        var table_row = "";

        table_row += "<tr>";
        table_row += "<td class='text-right w-15 align-middle'";
        if (filtered_results[j].system_full) {
            table_row += ' data-toggle="tooltip" data-placement="right" title="' + filtered_results[j].system_full + '"';
        }
        table_row += ">" + filtered_results[j].system + "" +
            (filtered_results[j].version ? "<br /><span class='text-muted'(" + filtered_results[j].version.replace(/ /g, '&nbsp;') + ")</span>" : "") + "</td>";

        table_row += "<td class='w-75'>";

        for (var current_run_idx = 0; current_run_idx < current_runs.length; current_run_idx++) {
            var k = current_runs[current_run_idx];

            var ratio = +$("#totals" + j + "_" + k).attr("data-ratio");
            var percents = (ratio * 100 / max_ratio).toFixed(2);

            if (!ratio) {
                ratio = +$("#absolute_totals" + j).attr("data-ratio");
                percents = (ratio * 100 / max_total_ratio).toFixed(2);
            }


            table_row += '<div class="progress ml-2 my-2" style="height:1rem;"><div class="progress-bar ';

            var bg = 'bg-dark';
            if (filtered_results[j].kind == 'cloud' || filtered_results[j].kind == 'vps') {
                bg = 'bg-primary';
            } else if (filtered_results[j].kind == 'desktop' || filtered_results[j].kind == 'laptop' || filtered_results[j].kind == 'phone') {
                bg = 'bg-secondary';
            }

            table_row += bg + '" style="width: ' + percents + '%;">&nbsp;</div></div>';

        }


        table_row += "</td>";

        table_row += "<td class='align-middle'><strong>" + (total_ratio / min_total_ratio).toFixed(2) + "</strong></td>";
        table_row += "</tr>";
        table_rows.push({ data: table_row, value: total_ratio / min_total_ratio });
    }
    table_rows.sort(function(a, b) { return a.value - b.value; });

    for (var j = 0; j < table_rows.length; j++) {
        html += table_rows[j].data;
    }

    html += "</table>";

    $('#diagram').html(html);
    $('[data-toggle="tooltip"]').tooltip({
        trigger: 'hover',
        delay: { "show": 300, "hide": 100 }
    });
}

generate_selectors($('#selectors'));
generate_comparison_table();
generate_diagram();
