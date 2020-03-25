function resizeSidebars() {
    $('#sidebar, #toc').css({
        'height': ($(window).height() - $('#top-nav').height()) + 'px'});
    $('body').attr('data-offset', $(window).height().toString());
}
$(document).ready(function () {
    $('#sidebar .nav-link.active').parents().collapse('show');
    resizeSidebars();
    $(window).resize(resizeSidebars);
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
                $(this).addClass('text-muted');
            } else {
                $(this).removeClass('text-muted');
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
