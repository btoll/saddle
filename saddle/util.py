import functools
import json
import os
import subprocess
import yaml


def create_abs_path_filename(filename):
    return "/".join((os.getcwd(), filename))


def get_json(o):
    return json.dumps(o, sort_keys=True, indent=4)


def get_mule_config(filename):
    with open(filename, "r") as fp:
        mule = load_yaml(fp)
        # This is kind of a cheat, but if we tack on the filename we won't
        # need to pass it along in as part of a compose pipeline (or curry
        # another function).
        mule["filename"] = filename
        return mule


def get_yaml(o):
    return yaml.dump(o)


def load_yaml(filename):
    return yaml.safe_load(filename.read())


def write_file(state, filename, fn):
    with open(filename, "w") as fp:
        fp.write(fn(state))
    print(f"File written to {create_abs_path_filename(filename)}")


def write_recipe(mule_state):
    filename = input(f"Name of outfile: ")
    [basename, ext] = os.path.splitext(filename)
    if filename.endswith(".*"):
        for (ext, fn) in extensions.items():
            write_file(mule_state, "".join((basename, ext)), fn)
    else:
        # Default to yaml if there is an unrecognized file extension
        # or if there is no file extension.
        write_file(mule_state, filename, extensions.get(ext, get_yaml))


extensions = {
    ".json": get_json,
    ".yaml": get_yaml
}
