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
        docs_top_nav.removeClass('sticky-top');
        $('#sidebar, #toc.toc-right').css({
            'height': window_height,
            'position': 'sticky',
            'top': 0
        });
    } else {
        var top_nav_height = docs_top_nav.height();
        docs_top_nav.addClass('sticky-top');
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
            current.css('transition-duration', '0s');
            current.collapse('show');
            current.css('transition-duration', '0.4s');
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
});
