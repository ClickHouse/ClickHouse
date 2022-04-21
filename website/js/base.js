(function () {
    Sentry.init({
        dsn: 'https://2b95b52c943f4ad99baccab7a9048e4d@o388870.ingest.sentry.io/5246103',
        environment: window.location.hostname === 'clickhouse.com' ? 'prod' : 'test'
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

    if (window.location.hostname.endsWith('clickhouse.com')) {
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
               '<img src="/docs/images/mkdocs/copy.svg" alt="Copy" title="Copy" class="code-copy btn float-right m-0 p-0" />'
           );
        });

        $('.code-copy').each(function(_, element) {
           element = $(element);
           element.click(function() {
               copy_to_clipboard(element.parent());
           })
        });
    }

    var is_single_page = $('html').attr('data-single-page') === 'true';
    if (!is_single_page) {
        $('head').each(function (_, element) {
            $(element).append(
                '<script async src="https://www.googletagmanager.com/gtag/js?id=G-KF1LLRTQ5Q"></script><script>window.dataLayer = window.dataLayer || [];function gtag(){dataLayer.push(arguments);}gtag(\'js\', new Date());gtag(\'config\', \'G-KF1LLRTQ5Q\');</script>'
            );
            $(element).append(
                '<script>!function(){var analytics=window.analytics=window.analytics||[];if(!analytics.initialize)if(analytics.invoked)window.console&&console.error&&console.error("Segment snippet included twice.");else{analytics.invoked=!0;analytics.methods=["trackSubmit","trackClick","trackLink","trackForm","pageview","identify","reset","group","track","ready","alias","debug","page","once","off","on","addSourceMiddleware","addIntegrationMiddleware","setAnonymousId","addDestinationMiddleware"];analytics.factory=function(e){return function(){var t=Array.prototype.slice.call(arguments);t.unshift(e);analytics.push(t);return analytics}};for(var e=0;e<analytics.methods.length;e++){var key=analytics.methods[e];analytics[key]=analytics.factory(key)}analytics.load=function(key,e){var t=document.createElement("script");t.type="text/javascript";t.async=!0;t.src="https://cdn.segment.com/analytics.js/v1/" + key + "/analytics.min.js";var n=document.getElementsByTagName("script")[0];n.parentNode.insertBefore(t,n);analytics._loadOptions=e};analytics._writeKey="dZuEnmCPmWqDuSEzCvLUSBBRt8Xrh2el";;analytics.SNIPPET_VERSION="4.15.3";analytics.load("dZuEnmCPmWqDuSEzCvLUSBBRt8Xrh2el");analytics.page();}}();</script>'
            );
        });
    }

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
