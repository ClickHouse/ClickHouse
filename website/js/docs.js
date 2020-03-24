function resizeSidebars() {
    $('#sidebar, #toc').css({
        'height': ($(window).height() - $('#top-nav').height()) + 'px'});
    $('body').attr('data-offset', $(window).height().toString());

}
$(document).ready(function () {
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
                $(this).addClass('active');
            } else {
                $(this).removeClass('active');
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
});
