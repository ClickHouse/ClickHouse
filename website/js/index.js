$(document).ready(function () {
    $.get('https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/README.md', function(e) {
        var skip = true;
        var lines = e.split('\n');
        var result = [];
        $.each(lines, function(idx) {
            var line = lines[idx];
            if (skip) {
                if (line.includes('Upcoming Events')) {
                    skip = false;
                }
            } else {
                if (!line) { return; };
                line = line.split('](');
                var tail = line[1].split(') ');
                result.push(
                    '<a class="announcement-link" rel="external nofollow" target="_blank" href="' +
                    tail[0] + '">' + line[0].replace('* [', '').replace('ClickHouse Meetup in ', '') +
                    '</a> ' + tail[1].slice(0, -1)
                );
            }
        });
        if (result.length) {
            if (result.length == 1) {
                result = 'Upcoming Meetup: ' + result[0];
            } else {
                result = 'Upcoming Meetups: ' + result.join(', ');
                var offset = result.lastIndexOf(', ');
                result = result.slice(0, offset) + result.slice(offset).replace(', ', ' and ');
            }
            $('#announcement>.page').html(result);
        }
    });
    var name = $('#logo-text').attr('alt').trim().toLowerCase();
    var feedback_address = name + '-feedback' + '@yandex-team.com';
    var feedback_email = $('#feedback_email');
    feedback_email.attr('href', 'mailto:' + feedback_address);
    feedback_email.html(feedback_address);

    $("a[href^='#']").on('click', function (e) {
        e.preventDefault();
        var selector = $(e.target).attr('href');
        var offset = 0;

        if (selector) {
            offset = $(selector).offset().top - $('#logo').height() * 1.5;
        }
        $('html, body').animate({
            scrollTop: offset
        }, 500);
        window.history.replaceState('', document.title, window.location.href.replace(location.hash, '') + this.hash);
    });

    var hostParts = window.location.host.split('.');
    if (hostParts.length > 2 && hostParts[0] != 'test' && hostParts[1] != 'github') {
        window.location.host = hostParts[0] + '.' + hostParts[1];
    }

    var available_distributives = ['deb', 'rpm', 'tgz'];
    var selected_distributive = 'deb';

    function refresh_distributives() {
        available_distributives.forEach(function (name) {
            if (name == selected_distributive) {
                $('#repo_' + name).attr("class", "distributive_selected");
                $('#instruction_' + name).show();
            } else {
                $('#repo_' + name).attr("class", "distributive_not_selected");
                $('#instruction_' + name).hide();
            }
        });
    };

    refresh_distributives();

    available_distributives.forEach(function (name) {
        $('#repo_' + name).on('click', function () {
            selected_distributive = name;
            refresh_distributives();
        });
    });
});
