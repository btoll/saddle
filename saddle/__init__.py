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


def _get_list_item(f):
    @functools.wraps(f)
    def wrap(*args):
        items = args[1]
        choice = f(*args)
        return items[choice]
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


def choose_job_env(mule_job_env):
    print(f"Environment variables for job:\n")
    env_state = []
    for item in mule_job_env:
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


def create_outfile(state, filename, fn):
    out = fn(state)
    write_to_file(out, filename)
    print(f"\nFile written to {'/'.join((os.getcwd(), filename))}\n")
    print(out)


@_charset("utf8")
def get_cmd_results(cmd):
    return subprocess.check_output(cmd).strip()


def get_files():
    yamls = list(filter(magic_number, glob.glob("*.yaml")))
    if not len(yamls):
        raise Exception(f"No `mule` files found in {os.getcwd()}>")
    return yamls


def get_job_config(mule_config, job):
    job_def = mule_config["jobs"].get(job)
    job_configs = job_def.get("configs", {})
    tasks = job_def.get("tasks", [])
    task_configs = []
    agents = []
    for job_task in tasks:
        name, task = get_task(mule_config, job_task)
        task_configs.append(task)
        if "dependencies" in task:
            for dependency in task["dependencies"]:
                _, task = get_task(mule_config, dependency)
                task_configs.append(task)
        # Recall not all tasks have an agent!
        if "agent" in task and job_task == name:
            agents = [item for item in mule_config["agents"] if task["agent"] == item["name"]]
    return {
        "filename": mule_config["filename"],
        "name": job,
        "configs": job_configs,
        "agents": agents,
        "tasks": tasks,
        "task_configs": task_configs,
    }


def get_job_env(agents):
    # To avoid duplicate env vars, we collect the agents' envs
    # into a set in case the agents share env vars.
    return {env for agent in agents for env in agent["env"]}


def get_jobs(mule_yaml):
    jobs = get_cmd_results(["mule", "-f", mule_yaml, "--list-jobs"])
    return list(filter(warnings, jobs.split("\n")))


def get_json(o):
    return json.dumps(o, sort_keys=True, indent=4)


def get_mule_config(mule_yaml):
    with open(mule_yaml, "r") as fp:
        mule = yaml.safe_load(fp.read())
        # This is kind of a cheat, but if we tack on the filename we won't
        # need to pass it along in as part of a compose pipeline (or curry
        # another function).
        mule["filename"] = mule_yaml
        return mule


def get_mule_state(job_config):
    agents = job_config["agents"]
    state = {
        "created": str(datetime.datetime.now()),
        "mule_version": get_cmd_results(["mule", "-v"]),
        "file": "/".join((os.getcwd(), job_config["filename"])),
        "job": job_config["name"],
        "configs": job_config["configs"],
        "tasks": job_config["tasks"],
        "task_configs": job_config["task_configs"],
        "agents": agents
    }
    if len(agents):
        mule_job_env = get_job_env(agents)
        for agent in agents:
            agent["env"] = choose_job_env(mule_job_env)
    return state


def get_task(mule_config, job_task):
    for task in mule_config["tasks"]:
        if "name" in task:
            name = ".".join((task["task"], task["name"]))
        else:
            name = task["task"]
        if name == job_task:
            return name, task


def get_yaml(o):
    return yaml.dump(o)


def list_items(items):
    return ["{}) {}".format(idx + 1, item) for idx, item in enumerate(items)]


def magic_number(filename):
    with open(filename, "r") as fp:
        return fp.read(6) == "#!mule"


def run_selected_job(mule_state):
    print(f"mule -f {os.path.basename(mule_state['file'])} {mule_state['job']}")
    return get_cmd_results(["mule", "-f", os.path.basename(mule_state['file']), mule_state["job"]])


def warnings(item):
    # Ignore blank lines and `mule` warnings from undefined environment variables.
    return not (not len(item) or item.find("Could not") > -1)


def write_recipe(mule_state):
    filename = input(f"\nName of outfile: ")
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
choose_operation = functools.partial(choose_input, "operation")
get_mule_yaml = compose(choose_file, get_files)


def init():
    mule_yaml = get_mule_yaml()
    return lambda: mule_yaml


def main():
    get_mule_filename = init()
    get_config = compose(get_mule_config, get_mule_filename)

    get_mule_job = compose(
            choose_job,
            get_jobs,
            get_mule_filename)

    job_config = compose(
            functools.partial(get_job_config, get_config()),
            get_mule_job)

    get_state = compose(
            get_mule_state,
            job_config)

    run_job = compose(
            run_selected_job,
            get_state)

    write_job = compose(
            write_recipe,
            get_state)

    if choose_operation(["Run", "Save as recipe"]) == "Run":
        print(run_job())
    else:
        write_job()


if __name__ == "__main__":
    sys.exit(main())
