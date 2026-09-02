padding="               "
if [[ $OSTYPE == 'darwin'* ]]; then
    sz="$(stat -f %z 'decompressor')"
else
    sz="$(stat -c %s 'decompressor')"
fi
printf "%s%s" "${padding:${#sz}}" $sz
