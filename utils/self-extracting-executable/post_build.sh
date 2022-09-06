padding="               "
if [[ $OSTYPE == 'darwin'* ]]; then
    if which gstat; then
        sz="$(gstat -c %s 'decompressor')"
    else
        sz="$(stat -f %z 'decompressor')"
    fi
else
    sz="$(stat -c %s 'decompressor')"
fi
printf "%s%s" "${padding:${#sz}}" $sz
