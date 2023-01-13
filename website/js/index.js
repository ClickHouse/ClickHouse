function serializeForm(form){
    var result = {};
    $.map(form.serializeArray(), function(n, i){
        result[n['name']] = n['value'];
    });
    return result;
};

$(document).ready(function () {
    var meetup_form = $('#meetup-form');
    $("#meetup-form-send").on('click', function(e) {
        e.preventDefault();
        var valid = true;
        var required_fields = $('#meetup-form input[required="true"]');
        $.each(required_fields, function(idx) {
            var input = $(required_fields[idx]);
            if (!input.val()) {
                valid = false;
                input.addClass('border-danger');
            } else {
                input.removeClass('border-danger');
            }
        });
        if (valid) {
            var data = JSON.stringify(serializeForm(meetup_form));
            $.ajax({
                url: '/meet-form/',
                type: 'POST',
                dataType: 'json',
                data: data,
                success: function () {
                    meetup_form.html('<div class="alert alert-success mt-3"><h2>Thanks!</h2><p class="lead">We\'ll be in touch soon.</p></div>');
                    $('#meetup-form-error').html('');
                },
                error: function () {
                    $('#meetup-form-error').html('<div class="alert alert-danger mt-3"><strong>Error!</strong> Unfortunately it didn\'t work for some reason, please try again or use the email address below.</div>');
                }
            });
        }
        return false;
    });
});
