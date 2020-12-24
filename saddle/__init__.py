import argparse
import copy
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


parser = argparse.ArgumentParser()
parser.add_argument("-r", "--recipe", help="Compile from recipe", type=argparse.FileType("r"))
parser.add_argument("-s", "--stdout", help="Write to stdout", action="store_true")
args = parser.parse_args()


def compose(*fs):
    return functools.reduce(compose2, fs)


def compose2(f, g):
    return lambda *a, **kw: f(g(*a, **kw))


def _charset(charset):
    def decoder(f):
        @functools.wraps(f)
        def wrap(*args):
            out = f(*args)
            return out.decode(charset)
        return wrap
    return decoder


def _get_list_item(items, n):
    if 0 < n <= len(items):
        return items[n - 1]
    else:
        raise ValueError(f"Selection `{n}` out of range")


def _validate(f):
    @functools.wraps(f)
    def wrap(*args):
        items = args[1]
        choice = f(*args)
        if len(choice) == 1 and choice[0] == len(items) + 1:
            # All jobs were selected so return the entire list.
            return items
        else:
            return list(map(functools.partial(_get_list_item, items), choice))
    return wrap


@_validate
def choose_input(name, items):
    for item in list_items(items):
        print(item)
    print(f"\n( To select all {name}, enter in 1 more than the highest-numbered item. )")
    choice = input(f"\nChoose {name}: ")
    # TODO: Probably need to trim as well.
    return list(map(int, choice.split(",")))


def choose_job_env(mule_job_env):
    print(f"Environment variables for job:\n")
    env_state = {}
    for key, value in mule_job_env.items():
        evald = os.path.expandvars(value)
        # If `parsed` starts with "$", then we know that it is an undefined env var
        # and could not be parsed.
        # In this case, we don't want to display anything as a default value.
        if evald.startswith("$"):
            evald = ""
        v = input(f"{key} [{evald}]: ")
        env_state[key] = v or evald
    return env_state


def compile(stream, to_yaml=True):
    recipe = load_yaml(stream)
    state = get_mule_state(
        get_jobs_state(
            get_mule_config(recipe["filename"]),
            recipe["jobs"]
        ), recipe)
    return get_yaml(state) if to_yaml else state


def create_abs_path_filename(filename):
    return "/".join((os.getcwd(), filename))


def create_outfile(state, filename, fn):
    out = fn(state)
    write_to_file(out, filename)
    print(f"File written to {create_abs_path_filename(filename)}")


def first(l):
    return l[0]


@_charset("utf8")
def get_cmd_results(cmd):
    return subprocess.check_output(cmd).strip()


def get_env_union(agents):
    # This is used twice:
    #   1. To avoid duplicate env vars, we collect the agents' envs
    #      into a set in case the agents share env vars.
    #   2. For individual agent to convert a list to a dict
    #      (`env` blocks can be defined as either lists or dicts).
    return {env.split("=")[0]: env.split("=")[1] for agent in agents for env in agent["env"]}


def get_files():
    yamls = list(filter(magic_number, glob.glob("*.yaml")))
    if not len(yamls):
        raise Exception(f"No `mule` files found in {os.getcwd()}>")
    return yamls


def get_jobs_state(mule_config, jobs):
    agents = []
    job_state = []
    mule_agents = mule_config.get("agents", [])
    task_configs = []
    for job in jobs:
        job_def = mule_config["jobs"].get(job)
        job_configs = job_def.get("configs", {})
        tasks = job_def.get("tasks", [])
        for job_task in tasks:
            task = get_task(mule_config, job_task)
            task_configs.append(task)
            for dependency in task.get("dependencies", []):
                task = get_task(mule_config, dependency)
                task_configs.append(task)
        if len(mule_agents):
            all_agents = {}
            for agent in mule_agents:
                all_agents[agent.get("name")] = agent
            agents_names = list({task.get("agent") for task in task_configs if task.get("agent")})
            agents = [all_agents[name] for name in agents_names]
        job_state.append({
            "filename": mule_config["filename"],
            "name": job,
            "configs": copy.deepcopy(job_configs),
            "agents": copy.deepcopy(agents),
            "tasks": tasks,
            "task_configs": copy.deepcopy(task_configs)
        })
    return {
        "agents": agents,
        "job_state": job_state,
        "task_configs": task_configs,
    }


def get_jobs(mule_yaml):
    jobs = get_cmd_results(["mule", "-f", first(mule_yaml), "--list-jobs"])
    return list(filter(warnings, jobs.split("\n")))


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


def get_mule_state(job_config, recipe):
    state = {
        "created": datetime.datetime.now(),
        "mule_version": get_cmd_results(["mule", "-v"]),
        # TODO: Figure out a better way to do this!
        "filename": "/".join((os.getcwd(), job_config.get("job_state")[0].get("filename"))),
        "items": []
    }
    agents = []
    for j_c in job_config.get("job_state"):
        if len(j_c["agents"]):
            agents += j_c["agents"]
        state["items"].append(j_c)
    env = recipe["env"]
    for agent in agents:
        if agent.get("env"):
            # Agent env blocks could be either lists or dicts. We only want
            # to work with the latter.
            agent_env = agent if type(agent["env"]) is dict else get_env_union([agent])
            # We only want truthy values (no empty strings). These will then
            # be looked up from our unique (non-duplicates) `env` dict.
            agent["env"] = {key: env[key] for key in agent_env.keys() if key in env and env[key]}
    return state


def get_recipe_env(fields):
    if len(fields["agents"]):
        mule_job_env = get_env_union(fields["agents"])
        fields["env"] = choose_job_env(mule_job_env)
    else:
        fields["env"] = []
    return fields


def get_recipe_fields(mule_config, jobs):
    job_state = get_jobs_state(mule_config, jobs)
    return {
        "filename": create_abs_path_filename(mule_config["filename"]),
        "jobs": jobs,
        "tasks": job_state.get("task_configs"),
        "agents": job_state.get("agents")
    }


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


def list_items(items):
    return ["{}) {}".format(idx + 1, item) for idx, item in enumerate(items)]


def load_yaml(filename):
    return yaml.safe_load(filename.read())


def magic_number(filename):
    with open(filename, "r") as fp:
        return fp.read(6) == "#!mule"


def make_recipe(fields):
    return {
        "created": datetime.datetime.now(),
        "mule_version": get_cmd_results(["mule", "-v"]),
        "filename": fields["filename"],
        "jobs": fields["jobs"],
        "env": fields["env"]
     }


def warnings(item):
    # Ignore blank lines and `mule` warnings from undefined environment variables.
    return not (not len(item) or item.find("Could not") > -1)


def write_recipe(mule_state):
    filename = input(f"Name of outfile: ")
    [basename, ext] = os.path.splitext(filename)
    if filename.endswith(".*"):
        for (ext, fn) in extensions.items():
            create_outfile(mule_state, "".join((basename, ext)), fn)
    else:
        # Default to yaml if there is an unrecognized file extension
        # or if there is no file extension.
        create_outfile(mule_state, filename, extensions.get(ext, get_yaml))


def write_to_file(out, filename):
    with open(filename, "w") as fp:
        fp.write(out)


extensions = {
    ".json": get_json,
    ".yaml": get_yaml
}


choose_file = functools.partial(choose_input, "file")
choose_job = functools.partial(choose_input, "job")
get_mule_yaml = compose(choose_file, get_files)


def init():
    mule_yaml = get_mule_yaml()
    return lambda: mule_yaml


def main():
    if args.recipe:
        if args.stdout:
            print(compile(args.recipe))
        else:
            write_recipe(compile(args.recipe, to_yaml=False))
    else:
        get_mule_filename = init()
        get_config = compose(
                get_mule_config,
                first,
                get_mule_filename)

        get_mule_jobs = compose(
                choose_job,
                get_jobs,
                get_mule_filename)

        get_env = compose(
                get_recipe_env,
                functools.partial(get_recipe_fields, get_config()),
                get_mule_jobs)

        write_job = compose(
                write_recipe,
                make_recipe,
                get_env)

        write_job()


if __name__ == "__main__":
    sys.exit(main())
