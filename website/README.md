ClickHouse website is built alongside it's documentation via [docs/tools](https://github.com/ClickHouse/ClickHouse/tree/master/docs/tools), see [README.md there](https://github.com/ClickHouse/ClickHouse/tree/master/docs/tools/README.md).

# How to quickly test the main page of the website

```
cd ../docs/tools
sudo apt install python-3 pip
pip3 install -r requirements.txt

# This is needed only when documentation is included
sudo npm install -g purify-css amphtml-validator
sudo apt install wkhtmltopdf
virtualenv build

./build.py --skip-multi-page --skip-single-page --skip-amp --skip-pdf --skip-blog --skip-git-log --skip-docs --livereload 8080

# Open the web browser and go to http://localhost:8080/
```

# How to quickly test the blog

```
./build.py --skip-multi-page --skip-single-page --skip-amp --skip-pdf --skip-git-log --skip-docs --livereload 8080
```

# How to quickly test the broken links in docs

```
./build.py --skip-multi-page --skip-amp --skip-pdf --skip-blog --skip-git-log --lang en --livereload 8080
```
