$(document).ready(function () {
    $('#content h1, #content h2, #content h3, #content h4, #content h5, #content h6').mouseenter(function() {
        $(this).find('.headerlink').show();
    }).mouseleave(function() {
        $(this).find('.headerlink').hide();
    });
});
