ClickHouse website is built alongside it's documentation via [docs/tools](https://github.com/ClickHouse/ClickHouse/tree/master/docs/tools), see [README.md there](https://github.com/ClickHouse/ClickHouse/tree/master/docs/tools/README.md).

# How to quickly test the main page of the website

```
# If you have old OS distribution,
# Run this from repository root:

docker run -it --rm --network host --volume $(pwd):/workspace ubuntu:20.04 /bin/bash
cd workspace/docs/tools
apt update
apt install sudo python pip git
pip3 install -r requirements.txt
git config --global --add safe.directory /workspace
./build.py --skip-multi-page --skip-blog --skip-docs --livereload 8080
```

```
cd ../docs/tools
sudo apt install python-3 pip
pip3 install -r requirements.txt

virtualenv build

./build.py --skip-multi-page --skip-blog --skip-docs --livereload 8080

# Open the web browser and go to http://localhost:8080/
```

# How to quickly test the blog

```
./build.py --skip-multi-page --skip-docs --livereload 8080
```

# How to quickly test the broken links in docs

```
./build.py --skip-multi-page --skip-blog --lang en --livereload 8080
```
