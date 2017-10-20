ClickHouse website quickstart:

On Linux, do the following:
```
sudo apt-get install nodejs
sudo ln -s /usr/bin/nodejs /usr/bin/node
sudo npm install gulp-cli -g
sudo npm install gulp -D
```

1. Make sure you have `npm`, `docker` and `python` installed and available in your `$PATH`.
2. Run `setup\_gulp.sh` once to install build prerequisites via npm.
3. Use `gulp build` to minify website to "public" subfolder or just `gulp` to run local webserver with livereload serving it (note: livereload browser extension is required to make it actually reload pages on edits automatically).
4. There's Dockerfile that can be used to build and run ClickHouse website inside docker.
5. Deployment to https://clickhouse.yandex/ is managed by `release.sh`, but it is only usable from inside Yandex private network.
