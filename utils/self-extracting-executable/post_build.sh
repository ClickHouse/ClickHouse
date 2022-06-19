padding="               "
sz="$(stat -c %s 'decompressor')"
printf "%s%s" "${padding:${#sz}}" $sz
