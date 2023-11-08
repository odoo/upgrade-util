# 🧰 Upgrade Utils

This repository contains helper functions to facilitate the writing of upgrade scripts.

## Installation

### The usual one
Once you have clone this repository locally, just start `odoo` with the `src` directory prepended to the `--upgrade-path` option.
```shell-session
$ ./odoo-bin --upgrade-path=/path/to/upgrade-util/src,/path/to/other/upgrade/script/directory [...]
```

### The alternative one
On platforms where you dont manage odoo yourself, i.e. [Odoo.sh](https://www.odoo.sh/), you can install this package via pip:
```shell-session
$ python3 -m pip install git+https://github.com/odoo/upgrade-util@master
```

## How to use them?
Once installed, the following packages are available
 - `odoo.upgrade.util`: the helper themself. See the wiki for info.
 - `odoo.upgrade.testing`: base TestCase classes
