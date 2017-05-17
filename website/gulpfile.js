var gulp = require('gulp');
var concat = require('gulp-concat');
var uglify = require('gulp-uglify');
var cleanCss = require('gulp-clean-css');
var imagemin = require('gulp-imagemin');
var sourcemaps = require('gulp-sourcemaps');
var htmlmin = require('gulp-htmlmin');
var minifyInline = require('gulp-minify-inline');
var del = require('del');
var connect = require('gulp-connect');
var run = require('gulp-run');

var outputDir = 'public';
var docsDir = '../docs/build/docs';

var paths = {
    htmls: ['*.html', '!reference_ru.html', '!reference_en.html'],
    reference: ['reference_ru.html', 'reference_en.html'],
    docs: [docsDir + '/**'],
    scripts: ['*.js', '!gulpfile.js'],
    styles: ['*.css'],
    images: ['*.png', '*.ico'],
    robotstxt: ['robots.txt'],
    presentations: ['../doc/presentations/**']
};

gulp.task('clean', function () {
    return del([outputDir + '/**']);
});

gulp.task('reference', [], function () {
    return gulp.src(paths.reference)
        .pipe(minifyInline())
        .pipe(gulp.dest(outputDir))
        .pipe(connect.reload())
});

gulp.task('docs', [], function () {
    run('cd ' + docsDir + '; make');
    return gulp.src(paths.docs)
        .pipe(gulp.dest(outputDir + '/docs'))
        .pipe(connect.reload())
});

gulp.task('presentations', [], function () {
    return gulp.src(paths.presentations)
        .pipe(gulp.dest(outputDir + '/presentations'))
        .pipe(connect.reload())
});

gulp.task('robotstxt', [], function () {
    return gulp.src(paths.robotstxt)
        .pipe(gulp.dest(outputDir))
        .pipe(connect.reload())
});

gulp.task('htmls', ['reference', 'docs', 'robotstxt', 'presentations'], function () {
    return gulp.src(paths.htmls)
        .pipe(htmlmin({collapseWhitespace: true}))
        .pipe(minifyInline())
        .pipe(gulp.dest(outputDir))
        .pipe(connect.reload())
});

gulp.task('scripts', [], function () {
    return gulp.src(paths.scripts)
        .pipe(sourcemaps.init())
        .pipe(uglify())
        .pipe(sourcemaps.write())
        .pipe(gulp.dest(outputDir))
        .pipe(connect.reload())
});

gulp.task('styles', [], function () {
    return gulp.src(paths.styles)
        .pipe(cleanCss())
        .pipe(gulp.dest(outputDir))
        .pipe(connect.reload())
});

gulp.task('images', [], function () {
    return gulp.src(paths.images)
        .pipe(imagemin({optimizationLevel: 9}))
        .pipe(gulp.dest(outputDir))
        .pipe(connect.reload())
});

gulp.task('watch', function () {
    gulp.watch(paths.htmls, ['htmls']);
    gulp.watch(paths.scripts, ['scripts']);
    gulp.watch(paths.images, ['images']);
});

gulp.task('connect', function() {
    connect.server({
        root: outputDir,
        port: 8080,
        keepalive: true,
        livereload: true
    })
});

gulp.task('build', ['htmls', 'scripts', 'styles', 'images']);

gulp.task('default', ['build', 'watch', 'connect']);
