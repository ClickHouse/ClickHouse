function onResize() {
    $('#sidebar, #toc').css({
        'height': ($(window).height() - $('#top-nav').height()) + 'px'
    });
    $('body').attr('data-offset', $(window).height().toString());
    var single_page_switch = $('#single-page-switch');
    console.log($('#sidebar').width());
    if ($('#sidebar').width() >= 248) {
        single_page_switch.addClass('float-right');
    } else {
        single_page_switch.removeClass('float-right');
    }
}
$(document).ready(function () {
    var tmp = $.fx.off;
    $.fx.off = true;
    $('#sidebar .nav-link.active').parents('.collapse').collapse('show');
    $.fx.off = tmp;
    onResize();
    $(window).resize(onResize);
    // $(window).scroll(function () {
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

    var headers = $('#content h1, #content h2, #content h3, #content h4, #content h5, #content h6');
    headers.mouseenter(function() {
        $(this).find('.headerlink').show();
    });
    headers.mouseleave(function() {
        $(this).find('.headerlink').hide();
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
                'facetFilters': ["lang:" + $('html').attr('lang')]
            },
            debug: true
        });
    }
});
