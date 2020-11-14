import datetime
import functools
import glob
import json
import os
import subprocess
import sys
import yaml


# Note!!!
# We're not importing `mulecli` because it's not yet ready to work as a library.
# Instead, we're having to call the `mule` binary and do some futzing with the
# results in order to work with them.
# This is an area that will change.

def partial(fn, *args):
    def wrap(*inner):
        return fn(*args, *inner)
    return wrap


def compose(f, g):
    def wrap(*args):
        return f(g(*args))
    return wrap


def _charset(charset):
    def decoder(f):
        @functools.wraps(f)
        def wrap(*args):
            out = f(*args)
            return out.decode(charset)
        return wrap
    return decoder


def _get_list_item(f):
    @functools.wraps(f)
    def wrap(*args):
        items = args[1]
        choice = f(*args)
        return choice, items[choice]
    return wrap


def _validate(f):
    @functools.wraps(f)
    def wrap(*args):
        items = args[1]
        choice = f(*args)
        if -1 < choice < len(items):
            return choice
        else:
            raise ValueError("Out of range")
    return wrap


@_get_list_item
@_validate
def choose_input(name, items):
    for item in list_items(items):
        print(item)
    return int(input(f"\nChoose {name}: ")) - 1


def create_outfile(state, filename, fn):
    out = fn(state)
    write_to_file(out, filename)
    print(f"\nFile written to {'/'.join((os.getcwd(), filename))}\n")
    print(out)


def get_agents(mule_yaml):
    agents = get_cmd_results(["mule", "-f", mule_yaml, "--list-agents"]).replace("\n", " ")
    if agents:
        return agents.split(" ")


@_charset("utf8")
def get_cmd_results(cmd):
    return subprocess.check_output(cmd).strip()


def get_files():
    yamls = list(filter(magic_number, glob.glob("*.yaml")))
    if not len(yamls):
        raise Exception(f"No `mule` files found in {os.getcwd()}>")
    return yamls


def get_job_env(mule_yaml, mule_job):
    res = get_cmd_results(["mule", "-f", mule_yaml, "--list-env", mule_job, "--verbose"])
    return yaml.safe_load(res)


def get_jobs(mule_yaml):
    jobs = get_cmd_results(["mule", "-f", mule_yaml, "--list-jobs"])
    return list(filter(warnings, jobs.split("\n")))


def get_json(o):
    return json.dumps(o, sort_keys=True, indent=4)


def get_mule_state(mule_yaml, mule_job):
    mule_agents = get_agents(mule_yaml)
    mule_state = []
    if mule_agents:
        mule_job_env = get_job_env(mule_yaml, mule_job)
        mule_state = get_state(mule_job_env)
    return mule_state


def get_state(mule_job_env):
    print(f"Environment variables for job:\n")
    # To avoid duplicate env vars, we first collect the agents' envs into
    # a set in case there is more than one.
    items = { item for agent in mule_job_env for item in mule_job_env[agent] }
    env_state = []
    for item in items:
        [key, value] = item.split("=")
        parsed = os.path.expandvars(value)
        # If `parsed` starts with "$", then we know that it is an undefined env var.
        # In this case, we don't want to display anything as a default value.
        if parsed.startswith("$"):
            parsed = ""
        v = input(f"{key} [{parsed}]: ")
        # TODO: This is just disgusting :)
        if (
            parsed and not v or
            parsed and v or
            not parsed and v
            ):
            env_state.append("=".join((key, v or parsed)))
    return env_state


def get_yaml(o):
    return yaml.dump(o)


def list_items(items):
    return ["{}) {}".format(idx + 1, item) for idx, item in enumerate(items)]


def magic_number(filename):
    with open(filename, "r") as fp:
        return fp.read(6) == "#!mule"


def warnings(item):
    # Ignore blank lines and `mule` warnings from undefined environment variables.
    return not (not len(item) or item.find("Could not") > -1)


def write_recipe(mule_yaml, mule_job, mule_state):
    filename = input(f"\nName of outfile: ")
    [basename, ext] = os.path.splitext(filename)
    state = {
        "created": str(datetime.datetime.now()),
        "version": get_cmd_results(["mule", "-v"]),
        "file": "/".join((os.getcwd(), mule_yaml)),
        "job": mule_job,
        "env": mule_state
    }
    if filename.endswith(".*"):
        for (ext, fn) in extensions.items():
            create_outfile(state, "".join((basename, ext)), fn)
    else:
        # Default to yaml if there is an unrecognized file extension
        # or if there is no file extension.
        create_outfile(state, filename, extensions.get(ext, get_yaml))


def write_to_file(out, filename):
    with open(filename, "w") as fp:
        fp.write(out)


extensions = {
    ".json": get_json,
    ".yaml": get_yaml
}


choose_file = partial(choose_input, "file")
choose_job = partial(choose_input, "job")
choose_operation = partial(choose_input, "operation")


get_mule_yaml = compose(choose_file, get_files)
get_mule_job = compose(choose_job, get_jobs)


def main():
    _, mule_yaml = get_mule_yaml()
    _, mule_job = get_mule_job(mule_yaml)
    mule_state = get_mule_state(mule_yaml, mule_job)
    idx, _ = choose_operation(["Run", "Save as recipe"])
    if idx == 0:
        print(" ".join(mule_state + [f"mule -f {mule_yaml} {mule_job}"]))
    else:
        write_recipe(mule_yaml, mule_job, mule_state)


if __name__ == "__main__":
    sys.exit(main())


