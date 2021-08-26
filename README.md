# tap-dixa

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls data from the [Dixa API](https://support.dixa.help/en/categories/65-dixa-api-s)
- Extracts from the following sources to produce [streams](tap_dixa/streams.py). Below is a list of all the streams available.
  - [Conversations](https://support.dixa.help/en/articles/174-export-conversations-via-api)
  - [Messages](https://support.dixa.help/en/articles/219-export-messages-via-api)
  - [Activity Log](https://integrations.dixa.io/docs#/ActivityLog)

- Includes a schema for each resource. See the [schemas](tap_dixa/schemas) folder for details.

## Authentication

API token can be retrieved by logging into your Dixa account then going to Settings > Integrations > Configure API Tokens > Add API Token.

# Config

The tap accepts the following config items:

| field                  | type   | required | description                                                                                                                                                                                                |
|------------------------|--------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| api_token          | string | yes      | [How to generate an API token](https://docs.dixa.io/docs/tutorial-create-an-api-token)                         |
| start_date             | string | yes      | ISO-8601  Example: "2021-08-03" or "2021-08-10T21:24:59.036000+00:00"                                                                                                                                             |
| interval            | string | no       | One of the following: "HOUR", "DAY", "WEEK", "MONTH". Default is "MONTH". Interval is used for determing the time interval for the `created_after` and `created_before` query string parameters for the conversations and messages streams.        |

## Quick Start

1. Install

Clone this repository, and then install using setup.py. We recommend using a virtualenv:

```bash
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip install -e .
```

2. Create your tap's config.json file (refer to [example config file](config.json.example)).

Run the Tap in Discovery Mode This creates a catalog.json for selecting objects/fields to sync:

```bash
tap-dixa --config config.json --discover > catalog.json
```

See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

**NOTE**: By default, discovery mode does not "select" any streams or fields to be synced. In order to be synced, each stream and field must contain a `selected: true` key-value. This is tedious to do by hand but there is [this](https://github.com/chrisgoddard/singer-discover) useful tool that provides a CLI GUI to select streams and fields.

3. Run the Tap in Sync Mode (with catalog) and write out to state file

For Sync mode:

```bash
$ tap-dixa --config config.json --catalog catalog.json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To load to json files to verify outputs:

```bash
$ tap-dixa --config config.json --catalog catalog.json | target-json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To pseudo-load to Stitch Import API with dry run:

```bash
$ tap-dixa --config config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

**NOTE**: Running sync mode with a `state.json` file to resume running from a prior state:

```bash
$ tap-dixa --state state.json --config config.json --catalog catalog.json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

---

Copyright &copy; 2018 Stitch
