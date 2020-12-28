import datetime
import functools
import glob
import os


import saddle.func
import saddle.util


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
def _choose_input(name, items):
    for item in _list_items(items):
        print(item)
    print(f"\n( To select all {name}, enter in 1 more than the highest-numbered item. )")
    choice = input(f"\nChoose {name}: ")
    # TODO: Probably need to trim as well.
    return list(map(int, choice.split(",")))


def _choose_job_env(mule_job_env):
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


def _get_agent_configs(mule_config, jobs):
    agent_configs = {agent.get("name"): agent for agent in mule_config.get("agents", [])}
    if agent_configs:
        agents_names = list({task.get("agent") for task in _get_task_configs(mule_config, jobs) if task.get("agent")})
        return [agent_configs.get(name) for name in agents_names]
    else:
        return []


def _get_env_union(agents):
    # This is used twice:
    #   1. To avoid duplicate env vars, we collect the agents' envs
    #      into a set in case the agents share env vars.
    #   2. For individual agent to convert a list to a dict
    #      (`env` blocks can be defined as either lists or dicts).
    return {env.split("=")[0]: env.split("=")[1] for agent in agents for env in agent["env"]}


def _get_jobs(mule_yaml):
    jobs = saddle.util.cmd_results(["mule", "-f", saddle.func.first(mule_yaml), "--list-jobs"])
    return list(filter(_warnings, jobs.split("\n")))


def _get_env(fields):
    if len(fields["agents"]):
        mule_job_env = _get_env_union(fields.get("agents"))
        fields["env"] = _choose_job_env(mule_job_env)
    else:
        fields["env"] = []
    return fields


def _get_fields(mule_config, jobs):
    return {
        "filename": saddle.util.create_abs_path_filename(mule_config.get("filename")),
        "jobs": jobs,
        "agents": _get_agent_configs(mule_config, jobs)
    }


def _get_list_item(items, n):
    if 0 < n <= len(items):
        return items[n - 1]
    else:
        raise ValueError(f"Selection `{n}` out of range")


def _get_mule_files():
    yamls = list(filter(_magic_number, glob.glob("*.yaml")))
    if not len(yamls):
        raise Exception(f"No `mule` files found in {os.getcwd()}>")
    return yamls


def _get_task(mule_config, job_task):
    for task in mule_config["tasks"]:
        if "name" in task:
            name = ".".join((task["task"], task["name"]))
        else:
            name = task["task"]
        if name == job_task:
            return task


def _get_task_configs(mule_config, jobs):
    # Should we be checking for jobs at this point or assuming we're good?
    task_configs = []
    for job in jobs:
        tasks = mule_config.get("jobs").get(job).get("tasks", [])
        for job_task in tasks:
            task = _get_task(mule_config, job_task)
            task_configs.append(task)
            for dependency in task.get("dependencies", []):
                task = _get_task(mule_config, dependency)
                task_configs.append(task)
    return task_configs


def _init():
    mule_yaml = _get_mule_yaml()
    return lambda: mule_yaml


def _list_items(items):
    return ["{}) {}".format(idx + 1, item) for idx, item in enumerate(items)]


def _magic_number(filename):
    with open(filename, "r") as fp:
        return fp.read(6) == "#!mule"


def _make_recipe(fields):
    return {
        "created": datetime.datetime.now(),
        "mule_version": saddle.util.cmd_results(["mule", "-v"]),
        "filename": fields.get("filename"),
        "jobs": fields.get("jobs"),
        "env": fields.get("env")
     }


def _warnings(item):
    # Ignore blank lines and `mule` warnings from undefined environment variables.
    return not (not len(item) or item.find("Could not") > -1)


_choose_file = functools.partial(_choose_input, "file")
_choose_job = functools.partial(_choose_input, "job")
_get_mule_yaml = saddle.func.compose(_choose_file, _get_mule_files)


def write():
    get_mule_filename = _init()
    get_config = saddle.func.compose(
            saddle.util.get_mule_config,
            saddle.func.first,
            get_mule_filename)

    get_mule_jobs = saddle.func.compose(
            _choose_job,
            _get_jobs,
            get_mule_filename)

    get_recipe_env = saddle.func.compose(
            _get_env,
            functools.partial(_get_fields, get_config()),
            get_mule_jobs)

    _write = saddle.func.compose(
            saddle.util.write_recipe,
            _make_recipe,
            get_recipe_env)

    _write()