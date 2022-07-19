function onResize() {
    var window_height = $(window).height();
    var window_width = $(window).width();
    var is_wide = window_width >= 768;
    var docs_top_nav = $('#top-nav.bg-dark-alt');

    $('body').attr('data-offset', window_height.toString());
    var sidebar = $('#sidebar');
    var languages = $('#languages-dropdown')
    var edit = $('#edit-link');
    var single_page_switch = $('#single-page-switch');
    if ((sidebar.width() - single_page_switch.width() - sidebar.find('.dropdown-toggle').width()) >= 36) {
        single_page_switch.addClass('float-right');
    } else {
        single_page_switch.removeClass('float-right');
    }
    if (is_wide) {
        sidebar.removeClass('collapse');
        edit.detach().appendTo($('#edit-wrapper'));
        languages.detach().appendTo($('#languages-wrapper'));
    } else {
        sidebar.addClass('collapse');
        edit.detach().insertBefore(single_page_switch);
        edit.addClass('float-right');
        languages.detach().insertBefore(edit);
        languages.addClass('float-right');
        single_page_switch.removeClass('float-right');
    }
    if (window_height < 800 && is_wide) {
        docs_top_nav.removeClass('fixed-top');
        $('#sidebar, #toc.toc-right').css({
            'height': window_height,
            'position': 'sticky',
            'top': 0
        });
    } else {
        var top_nav_height = docs_top_nav.height();
        docs_top_nav.addClass('fixed-top');
        $('#sidebar, #toc.toc-right').css({
            'height': (window_height - top_nav_height) + 'px',
            'position': 'fixed',
            'top': top_nav_height + 16
        });
    }
}

$(document).ready(function () {
    onResize();
    $('#sidebar .nav-link.active').parents('.collapse').each(function() {
        var current = $(this);
        if (current.attr('id') !== 'sidebar') {
            current.collapse('show');
        }
    });
    $(window).resize(onResize);
    $(window).on('activate.bs.scrollspy', function () {
        var maxActiveOffset = 0;
        $('#toc .nav-link.active').each(function() {
            if (maxActiveOffset < $(this).offset().top) {
                maxActiveOffset = $(this).offset().top;
            }
        });
        $('#toc .nav-link').each(function() {
            if (maxActiveOffset >= $(this).offset().top) {
                $(this).addClass('toc-muted');
            } else {
                $(this).removeClass('toc-muted');
            }
        });
    });
    $('#sidebar').on('shown.bs.collapse', function () {
        onResize();
        $('body').on('touchmove', function (e) {
            e.preventDefault();
        });
    });
    $('#sidebar').on('hidden.bs.collapse', function () {
        $('body').on('touchmove', function () {});
    });

    var headers = $('#content h1, #content h2, #content h3, #content h4, #content h5, #content h6');
    headers.mouseenter(function() {
        $(this).find('.headerlink').html('¶');
    });
    headers.mouseleave(function() {
        $(this).find('.headerlink').html('&nbsp;');
    });

    if ($('#docsearch-input').length) {
        docsearch({
            apiKey: 'e239649803024433599de47a53b2d416',
            indexName: 'yandex_clickhouse',
            inputSelector: '#docsearch-input',
            algoliaOptions: {
                advancedSyntax: true,
                clickAnalytics: true,
                hitsPerPage: 25,
                'facetFilters': [
                    'lang:' + $('html').attr('lang'),
                    'version:' + $('html').attr('data-version')
                ]
            },
            debug: false
        });
    }

    var rating_stars = $('#rating-stars');
    if (rating_stars.length) {
        var key = '';
        var stars_text = rating_stars.text().replace(/^\s+|\s+$/g, '');
        var url_match = window.location.pathname.match(/^\/docs(?:\/v[0-9]+\.[0-9]+)?(?:\/[a-z]{2})(.*)/);
        if (url_match && url_match.length > 1) {
            key = url_match[1];
            if (key.length > 2) {
                key = key.replace(/^\/|\/$/g, '');
            }
            key = 'rating_' + key;
            var local_stars = localStorage.getItem(key);
            if (local_stars) {
                stars_text = local_stars;
                rating_stars.addClass('text-orange');
            }
        }
        var titles = JSON.parse(rating_stars.attr('data-titles').replace(/'/g, '"'));
        var documentation = rating_stars.attr('data-documentation');
        var replacement = '';
        for (var i = 0; i < 5; i++) {
            var star = stars_text.charAt(i);
            replacement += '<a href="#" title="' + titles[i] + ' ' + documentation +
              '" data-toggle="tooltip" data-placement="bottom" role="rating" ' +
              'class="rating-star text-reset text-decoration-none">' + star + '</a>';
        }
        rating_stars.html(replacement);
        $('[data-toggle="tooltip"]').tooltip({
            trigger: 'hover'
        });
        var rating_star_item = $('.rating-star');
        rating_star_item.hover(function() {
            var current = $(this);
            current.text('★');
            current.prevAll().text('★');
            current.nextAll().text('☆');
        });
        rating_stars.mouseleave(function() {
            for (var i = 0; i < 5; i++) {
                rating_stars.children().eq(i).text(stars_text[i]);
            }
        });
        rating_star_item.click(function(e) {
            e.preventDefault();
            rating_stars.addClass('text-orange');
            stars_text = rating_stars.text();
            localStorage.setItem(key, stars_text);
            $.ajax({
                url: window.location.pathname + 'rate/',
                type: 'POST',
                dataType: 'json',
                data: JSON.stringify({rating: $(this).prevAll().length + 1}),
                success: function () {
                    try {
                        window.yaCounter18343495.reachGoal('docs_feedback');
                    } catch (e) {
                        Sentry.captureException(e);
                    }
                },
                error: function () {
                    rating_stars.removeClass('text-orange');
                    localStorage.removeItem(key);
                }
            });
        });
    }
});
