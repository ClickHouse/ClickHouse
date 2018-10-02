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
var docsDir = '../docs';

var paths = {
    htmls: [
        '**/*.html',
        '!deprecated/reference_ru.html',
        '!deprecated/reference_en.html',
        '!node_modules/**/*.html',
        '!presentations/**/*.html',
        '!public/**/*.html'],
    reference: ['deprecated/reference_ru.html', 'deprecated/reference_en.html'],
    docs: [docsDir + '/build/**/*'],
    docstxt: ['docs/**/*.txt', 'docs/redirects.conf'],
    docsjson: ['docs/**/*.json'],
    docsxml: ['docs/**/*.xml'],
    docssitemap: ['sitemap.xml', 'sitemap_static.xml'],
    scripts: [
        '**/*.js',
        '!gulpfile.js',
        '!node_modules/**/*.js',
        '!presentations/**/*.js',
        '!public/**/*.js'],
    styles: [
        '**/*.css',
        '!node_modules/**/*.css',
        '!presentations/**/*.css',
        '!public/**/*.css'],
    images: [
        '**/*.{jpg,jpeg,png,gif,svg,ico}',
        '!node_modules/**/*.{jpg,jpeg,png,gif,svg,ico}',
        '!presentations/**/*.{jpg,jpeg,png,gif,svg,ico}',
        '!public/**/*.{jpg,jpeg,png,gif,svg,ico}'],
    robotstxt: ['robots.txt'],
    presentations: ['presentations/**/*']
};

gulp.task('clean', function () {
    return del([outputDir + '/**']);
});

gulp.task('reference', [], function () {
    return gulp.src(paths.reference)
        .pipe(minifyInline())
        .pipe(gulp.dest(outputDir + '/deprecated'))
});

gulp.task('docs', [], function () {
    run('cd ' + docsDir + '/tools; ./build.py');
    return gulp.src(paths.docs)
        .pipe(gulp.dest(outputDir + '/../docs'))
});

gulp.task('docstxt', ['docs'], function () {
    return gulp.src(paths.docstxt)
        .pipe(gulp.dest(outputDir + '/docs'))
});

gulp.task('docsjson', ['docs'], function () {
    return gulp.src(paths.docsjson)
        .pipe(gulp.dest(outputDir + '/docs'))
});

gulp.task('docsxml', ['docs'], function () {
    return gulp.src(paths.docsxml)
        .pipe(gulp.dest(outputDir + '/docs'))
});

gulp.task('docssitemap', [], function () {
    return gulp.src(paths.docssitemap)
        .pipe(gulp.dest(outputDir + '/docs'))
});

gulp.task('presentations', [], function () {
    return gulp.src(paths.presentations)
        .pipe(gulp.dest(outputDir + '/presentations'))
});

gulp.task('robotstxt', [], function () {
    return gulp.src(paths.robotstxt)
        .pipe(gulp.dest(outputDir))
});

gulp.task('htmls', ['docs', 'docstxt', 'docsjson', 'docsxml', 'docssitemap'], function () {
    return gulp.src(paths.htmls)
        .pipe(htmlmin({collapseWhitespace: true}))
        .pipe(minifyInline())
        .pipe(gulp.dest(outputDir))
});

gulp.task('sourcemaps', ['docs'], function () {
    return gulp.src(paths.scripts)
        .pipe(sourcemaps.init())
        .pipe(uglify())
        .pipe(sourcemaps.write())
        .pipe(gulp.dest(outputDir))
});

gulp.task('scripts', ['docs'], function () {
    return gulp.src(paths.scripts)
        .pipe(uglify())
        .pipe(gulp.dest(outputDir))
});

gulp.task('styles', ['docs'], function () {
    return gulp.src(paths.styles)
        .pipe(cleanCss())
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

gulp.task('build', ['htmls', 'robotstxt', 'reference', 'scripts', 'styles', 'images', 'presentations']);

gulp.task('default', ['build', 'connect']);
