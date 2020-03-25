$(document).ready(function () {
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

        if (target_id === 'dark-mode-switch') {
            var body = $('body');
            if(body.hasClass('dark-mode')) {
                body.removeClass('dark-mode');
                target.attr('src', '/images/mkdocs/moon.svg');
                localStorage.removeItem('dark-mode');
            } else {
                body.addClass('dark-mode');
                target.attr('src', '/images/mkdocs/sun.svg');
                localStorage.setItem('dark-mode', '1');
            }
            return;
        }

        if (target_id && target_id.startsWith('logo-')) {
            selector = '#';
        }

        if (selector && selector.startsWith('#') && !is_tab && !is_collapse) {
            event.preventDefault();
            var dst = window.location.href.replace(window.location.hash, '');
            var offset = 0;

            if (selector !== '#') {
                offset = $(selector).offset().top - navbar_toggle.height() * 1.5;
                dst += selector;
            }
            $('html, body').animate({
                scrollTop: offset
            }, 500);
            window.history.replaceState('', document.title, dst);
        }
    });
    (function (d, w, c) {
        (w[c] = w[c] || []).push(function() {
            try {
                w.yaCounter18343495 = new Ya.Metrika2({
                    id:18343495,
                    clickmap:true,
                    trackLinks:true,
                    accurateTrackBounce:true,
                    webvisor:true
                });
            } catch(e) { }
        });

        var n = d.getElementsByTagName("script")[0],
            s = d.createElement("script"),
            f = function () { n.parentNode.insertBefore(s, n); };
        s.type = "text/javascript";
        s.async = true;
        s.src = "https://mc.yandex.ru/metrika/tag.js";

        if (w.opera == "[object Opera]") {
            d.addEventListener("DOMContentLoaded", f, false);
        } else { f(); }
    })(document, window, "yandex_metrika_callbacks2");
    var nua = navigator.userAgent;
    var isAndroid = (nua.indexOf('Mozilla/5.0') > -1 && nua.indexOf('Android ') > -1 && nua.indexOf('AppleWebKit') > -1 && nua.indexOf('Chrome') === -1);
    if (isAndroid) {
        $('select.form-control').removeClass('form-control').css('width', '100%');
    }
    if (window.matchMedia('(prefers-reduced-motion: reduce)')) {
        $.fx.off = true;
    }
});

{% include "js/index.js" %}
{% include "js/docs.js" %}
