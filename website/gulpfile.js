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
var docsDir = '../doc/reference';

var paths = {
    htmls: [
        '**/*.html',
        '!reference_ru.html',
        '!reference_en.html',
        '!node_modules/**/*.html',
        '!public/**/*.html'],
    reference: ['reference_ru.html', 'reference_en.html'],
    docs: ['../doc/reference/build/docs/**/*'],
    scripts: [
        '**/*.js',
        '!gulpfile.js',
        '!node_modules/**/*.js',
        '!public/**/*.js'],
    styles: [
        '**/*.css',
        '!node_modules/**/*.css',
        '!public/**/*.css'],
    images: [
        '**/*.{jpg,jpeg,png,svg,ico}',
        '!node_modules/**/*.{jpg,jpeg,png,svg,ico}',
        '!public/**/*.{jpg,jpeg,png,svg,ico}'],
    robotstxt: ['robots.txt'],
    presentations: ['../doc/presentations/**/*']
};

gulp.task('clean', function () {
    return del([outputDir + '/**']);
});

gulp.task('reference', [], function () {
    return gulp.src(paths.reference)
        .pipe(minifyInline())
        .pipe(gulp.dest(outputDir))
});

gulp.task('docstxt', [], function () {
    run('cd ' + docsDir + '; make');
    return gulp.src(paths.docs)
        .pipe(gulp.dest(outputDir + '/../docs'))
});

gulp.task('docs', ['docstxt'], function () {
    run('cd ' + docsDir + '; make');
    return gulp.src(paths.docs)
        .pipe(gulp.dest(outputDir + '/../docs'))
});

gulp.task('presentations', [], function () {
    return gulp.src(paths.presentations)
        .pipe(gulp.dest(outputDir + '/presentations'))
});

gulp.task('robotstxt', [], function () {
    return gulp.src(paths.robotstxt)
        .pipe(gulp.dest(outputDir))
});

gulp.task('htmls', ['docs'], function () {
    return gulp.src(paths.htmls)
        .pipe(htmlmin({collapseWhitespace: true}))
        .pipe(minifyInline())
        .pipe(gulp.dest(outputDir))
});

gulp.task('scripts', ['docs'], function () {
    return gulp.src(paths.scripts)
        .pipe(sourcemaps.init())
        .pipe(uglify())
        .pipe(sourcemaps.write())
        .pipe(gulp.dest(outputDir))
});

gulp.task('styles', ['docs'], function () {
    return gulp.src(paths.styles)
        .pipe(cleanCss({inline: ['none']}))
        .pipe(gulp.dest(outputDir))
});

gulp.task('images', ['docs'], function () {
    return gulp.src(paths.images)
        .pipe(imagemin({optimizationLevel: 9}))
        .pipe(gulp.dest(outputDir))
});

gulp.task('watch', function () {
    gulp.watch(paths.htmls, ['htmls']);
    gulp.watch(paths.docs, ['docs']);
    gulp.watch(paths.reference, ['reference']);
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

gulp.task('build', ['htmls', 'robotstxt', 'reference', 'presentations', 'scripts', 'styles', 'images']);

gulp.task('default', ['build', 'connect']);
