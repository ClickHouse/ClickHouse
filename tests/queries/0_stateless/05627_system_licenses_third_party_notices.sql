-- The notices of components bundled into a library (here `rapidhash` in `zxc`) are listed next to its main license.
SELECT library_name, license_type, license_path, position(license_text, 'rapidhash') > 0 FROM system.licenses WHERE library_name = 'zxc' ORDER BY license_path;
