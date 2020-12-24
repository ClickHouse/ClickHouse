ClickHouse website is built alongside it's documentation via [docs/tools](https://github.com/ClickHouse/ClickHouse/tree/master/docs/tools), see [README.md there](https://github.com/ClickHouse/ClickHouse/tree/master/docs/tools/README.md).

# How to quickly test the main page of the website

```
cd ../docs/tools
sudo apt install python-3 pip
pip3 install -r requirements.txt
./build.py --skip-multi-page --skip-single-page --skip-amp --skip-pdf --skip-blog --skip-git-log --skip-docs --skip-test-templates --livereload 8080

# Open the web browser and go to http://localhost:8080/
```
