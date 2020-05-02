(function () {
    var logo_text = $('#logo-text');
    if (logo_text.length) {
        var name = logo_text.attr('alt').trim().toLowerCase();
        var feedback_address = name + '-feedback' + '@yandex-team.com';
        var feedback_email = $('#feedback_email');
        feedback_email.attr('href', 'mailto:' + feedback_address);
        feedback_email.html(feedback_address);
    }

    $(document).click(function (event) {
        var target = $(event.target);
        var target_id = target.attr('id');
        var selector = target.attr('href');
        var is_tab = target.attr('role') === 'tab';
        var is_collapse = target.attr('data-toggle') === 'collapse';
        var navbar_toggle = $('#navbar-toggle');

        navbar_toggle.collapse('hide');
        $('.algolia-autocomplete .ds-dropdown-menu').hide();

        if (target_id && target_id.startsWith('logo-')) {
            selector = '#';
        }

        if (selector && selector.startsWith('#') && !is_tab && !is_collapse) {
            event.preventDefault();
            var dst = window.location.href.replace(window.location.hash, '');
            var offset = 0;

            if (selector !== '#') {
                offset = $(selector).offset().top - $('#top-nav').height() * 1.5;
                dst += selector;
            }
            $('html, body').animate({
                scrollTop: offset
            }, 500);
            window.history.replaceState('', document.title, dst);
        }
    });

    var top_nav = $('#top-nav.sticky-top');
    if (window.location.hash.length > 1 && top_nav.length) {
        var offset = $(window.location.hash).offset().top - top_nav.height() * 1.5;
        $('html, body').animate({
            scrollTop: offset
        }, 70);
    }

    $('img').each(function() {
        var src = $(this).attr('data-src');
        if (src) {
            $(this).attr('src', src);
        }
    });

    (function (d, w, c) {
        (w[c] = w[c] || []).push(function() {
            var is_single_page = $('html').attr('data-single-page') === 'true';
            try {
                w.yaCounter18343495 = new Ya.Metrika2({
                    id: 18343495,
                    clickmap: !is_single_page,
                    trackLinks: !is_single_page,
                    accurateTrackBounce: !is_single_page,
                    webvisor: !is_single_page
                });
            } catch(e) { }
        });

        var n = d.getElementsByTagName("script")[0],
            s = d.createElement("script"),
            f = function () { n.parentNode.insertBefore(s, n); };
        s.type = "text/javascript";
        s.async = true;
        s.src = "/js/metrika.js";
        if (window.location.hostname.endsWith('clickhouse.tech')) {
            if (w.opera == "[object Opera]") {
                d.addEventListener("DOMContentLoaded", f, false);
            } else {
                f();
            }
        }
    })(document, window, "yandex_metrika_callbacks2");

    var beforePrint = function() {
        var details = document.getElementsByTagName("details");
        for (var i = 0; i < details.length; ++i) {
            details[i].open = 1;
        }
    };

    if (window.matchMedia) {
        window.matchMedia('print').addListener(function(q) {
            if (q.matches) {
                beforePrint();
            }
        });
        if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
            $.fx.off = true;
        }
    }
    window.onbeforeprint = beforePrint;
})();
