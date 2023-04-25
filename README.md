# mbpy_plugin_powerschool
Synchronize PowerSchool with ManageBac.

## Requirements:

- mbpy
- PowerSchool Plugin

## Installation

Clone this repo, and install it into mbpy's virtual environment. 

1. `cd /path/to/mbpy/src/plugins`
2. `git clone …`
3. `pip install --editable mbpy_plugin_powerschool`

If it has installed correctly, doing this:

```
mbpy plugins --help
```

will list this plugin.

## Configure and execute

To see all available options:

```
mbpy plugins mbpy_plugin_powerschool --help
```

Finally when configured:

```
mbpy plugins mbpy_plugin_powerschool …
```


## Mock mode

Either:

```
mbpy --mock plugins mbpy_plugin_powerschool …
```

or, set `mock=True` in your configuration subdomain file (inside `/path/to/mbpy/conf`).
