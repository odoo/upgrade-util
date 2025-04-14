# 🧰 Upgrade Utils

This repository contains helper functions[^1] to facilitate the writing of upgrade scripts.

The functions in this repo are meant to work (sometimes just not fail) from Odoo 7.0 up to latest version.
Thus the only supported version of this repo is `master` head.

## Installation

### Recommended

Once you have cloned this repository locally, start `odoo` with the `src` directory prepended to the `--upgrade-path` option.
```shell-session
$ ./odoo-bin --upgrade-path=/path/to/upgrade-util/src,/path/to/other/upgrade/script/directory [...]
```

### Alternative

On platforms where you don't manage Odoo yourself, you can install this package via pip:
```shell-session
$ python3 -m pip install git+https://github.com/odoo/upgrade-util@master
```

You can freeze the hash version when installing in this fashion. Just replace `master` by the hash of the commit you want to target.

On [Odoo.sh](https://www.odoo.sh/) it is recommended to add it to the `requirements.txt` of your repository:
```
odoo_upgrade @ git+https://github.com/odoo/upgrade-util@master
```

## How to use the helper functions?

Once installed, the following packages are available
 - `odoo.upgrade.util`: the helper functions.
 - `odoo.upgrade.testing`: base `TestCase` classes

## Documentation

- [Basic guide on how to write upgrade scripts](https://www.odoo.com/documentation/master/developer/reference/upgrades/upgrade_scripts.html)
- [The reference documentation](https://www.odoo.com/documentation/master/developer/reference/upgrades/upgrade_utils.html)

[^1]: We call them "utils".
