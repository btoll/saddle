import functools
import json
import os
import subprocess
import yaml


def _charset(charset):
    def decoder(f):
        @functools.wraps(f)
        def wrap(*args):
            out = f(*args)
            return out.decode(charset)
        return wrap
    return decoder


@_charset("utf8")
def cmd_results(cmd):
    return subprocess.check_output(cmd).strip()


def create_abs_path_filename(filename):
    return "/".join((os.getcwd(), filename))


def get_all_agent_configs(mule_agents, jobs):
    if len(mule_agents):
        return {agent.get("name"): agent for agent in mule_agents}
    else:
        return {}


def get_env_union(agents):
    # This is used twice:
    #   1. To avoid duplicate env vars, we collect the agents' envs
    #      into a set in case the agents share env vars.
    #   2. For individual agent to convert a list to a dict
    #      (`env` blocks can be defined as either lists or dicts).
    return {env.split("=")[0]: env.split("=")[1] for agent in agents for env in agent["env"]}


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


def get_task(mule_config, job_task):
    for task in mule_config["tasks"]:
        if "name" in task:
            name = ".".join((task["task"], task["name"]))
        else:
            name = task["task"]
        if name == job_task:
            return task


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
