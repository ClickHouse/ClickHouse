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
                var event_date = tail[1].slice(0, -1).replace('on ', '');
                result.push(
                    '<a class="stealth-link" rel="external nofollow" target="_blank" href="' +
                    tail[0] + '"><span class="text-orange">'+ event_date + '</span>&nbsp;' +
                    line[0].replace('* [', '') + '</a> '
                );
            }
        });
        if (result.length) {
            if (result.length == 1) {
                result = '<h2>Upcoming Event</h2><p class="lead">' + result[0] + '</p>';
            } else {
                result = '<h2>Upcoming Events</h2><ul class="lead list-unstyled"><li>' + result.join('</li><li>') + '</li></ul>';
            }
            $('#events>.container').html(result);
        }
    });
    var name = $('#logo-text').attr('alt').trim().toLowerCase();
    var feedback_address = name + '-feedback' + '@yandex-team.com';
    var feedback_email = $('#feedback_email');
    feedback_email.attr('href', 'mailto:' + feedback_address);
    feedback_email.html(feedback_address);

    $(document).click(function (event) {
        var target = $(event.target);
        var target_id = target.attr('id');
        var selector = target.attr('href');
        var is_tab = target.attr('role') === 'tab';

        $('#navbar-toggle').collapse('hide');

        if (target_id && target_id.startsWith('logo-')) {
            selector = '#';
        }

        if (selector && selector.startsWith('#') && !is_tab) {
            event.preventDefault();
            var dst = window.location.href.replace(window.location.hash, '');
            var offset = 0;

            if (selector !== '#') {
                offset = $(selector).offset().top - $('#navbar-toggle').height() * 1.5;
                dst += selector;
            }
            $('html, body').animate({
                scrollTop: offset
            }, 500);
            window.history.replaceState('', document.title, dst);
        }
    });
});
