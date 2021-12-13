(function () {
    Sentry.init({
        dsn: 'https://2b95b52c943f4ad99baccab7a9048e4d@o388870.ingest.sentry.io/5246103',
        environment: window.location.hostname === 'clickhouse.tech' ? 'prod' : 'test'
    });
    $(document).click(function (event) {
        var target = $(event.target);
        var target_id = target.attr('id');
        var selector = target.attr('href');
        var is_tab = target.attr('role') === 'tab';
        var is_collapse = target.attr('data-toggle') === 'collapse';
        var is_rating = target.attr('role') === 'rating';
        var navbar_toggle = $('#navbar-toggle');
        navbar_toggle.collapse('hide');
        $('.algolia-autocomplete .ds-dropdown-menu').hide();
        if (target_id && target_id.startsWith('logo-')) {
            selector = '#';
        }
    });

    var top_nav = $('#top-nav.sticky-top');
    if (window.location.hash.length > 1 && top_nav.length) {
        var hash_destination = $(window.location.hash);
        if (hash_destination.length) {
            var offset = hash_destination.offset().top - top_nav.height() * 1.5;
            $('html, body').animate({
                scrollTop: offset
            }, 70);
        }
    }

    $('img').each(function() {
        var src = $(this).attr('data-src');
        if (src) {
            $(this).attr('src', src);
        }
    });

    if (window.location.hostname.endsWith('clickhouse.tech')) {
        $('a.favicon').each(function () {
            $(this).css({
                background: 'url(/favicon/' + this.hostname + ') left center no-repeat',
                'padding-left': '20px'
            });
        });

        function copy_to_clipboard(element) {
            var temp = $('<textarea></textarea>');
            $('body').append(temp);
            temp.val($(element).text());
            temp.select();
            document.execCommand('copy');
            temp.remove();
        }

        $('pre').each(function(_, element) {
           $(element).prepend(
               '<img src="/images/mkdocs/copy.svg" alt="Copy" title="Copy" class="code-copy btn float-right m-0 p-0" />'
           );
        });

        $('.code-copy').each(function(_, element) {
           element = $(element);
           element.click(function() {
               copy_to_clipboard(element.parent());
           })
        });
    }

    $('#feedback_email, .feedback-email').each(function() {
        var name = window.location.host.substring(0, 10)
        var feedback_address = name + '-feedback' + '@yandex-team.com';
        $(this).attr('href', 'mailto:' + feedback_address);
        $(this).html(feedback_address);
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
