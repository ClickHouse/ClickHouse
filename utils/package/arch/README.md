### Build Arch Linux package

From binary directory:

```
make
cd utils/package/arch
makepkg
```

### Install and start ClickHouse server

```
pacman -U clickhouse-*.pkg.tar.xz
systemctl enable clickhouse-server
systemctl start clickhouse-server
```
