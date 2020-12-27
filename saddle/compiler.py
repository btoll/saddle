import copy
import functools
import datetime
import os


import saddle.util


def compile(stream, to_yaml=True):
    recipe = saddle.util.load_yaml(stream)
    state = get_compiled_state(
        get_jobs_state(
            saddle.util.get_mule_config(recipe.get("filename")),
            recipe.get("jobs")
        ), recipe)
    return saddle.util.get_yaml(state) if to_yaml else state


def get_compiled_state(jobs_state, recipe):
    state = {
        "created": datetime.datetime.now(),
        "mule_version": saddle.util.cmd_results(["mule", "-v"]),
        # TODO: Figure out a better way to do this!
        "filename": "/".join((os.getcwd(), jobs_state[0].get("filename"))),
        "items": []
    }
    agents = []
    for j_c in jobs_state:
        if len(j_c["agents"]):
            agents += j_c["agents"]
        state["items"].append(j_c)
    env = recipe["env"]
    for agent in agents:
        if agent.get("env"):
            # Agent env blocks could be either lists or dicts. We only want
            # to work with the latter.
            agent_env = agent if type(agent["env"]) is dict else saddle.util.get_env_union([agent])
            # We only want truthy values (no empty strings). These will then
            # be looked up from our unique (non-duplicates) `env` dict.
            agent["env"] = {key: env.get(key) for key in agent_env.keys() if key in env and env.get(key)}
    return state


def get_jobs_state(mule_config, jobs):
    agents = []
    job_state = []
    mule_agents = mule_config.get("agents", [])
    all_agent_configs = saddle.util.get_all_agent_configs(mule_agents, jobs)
    for job in jobs:
        task_configs = []
        job_def = mule_config.get("jobs").get(job)
        job_configs = job_def.get("configs", {})
        tasks = job_def.get("tasks", [])
        for job_task in tasks:
            task = saddle.util.get_task(mule_config, job_task)
            task_configs.append(task)
            for dependency in task.get("dependencies", []):
                task = saddle.util.get_task(mule_config, dependency)
                task_configs.append(task)
        # TODO: Check for agents?
        agents_names = list({task.get("agent") for task in task_configs if task.get("agent")})
        agents = [all_agent_configs.get(name) for name in agents_names]
        job_state.append({
            "filename": mule_config.get("filename"),
            "name": job,
            "configs": copy.deepcopy(job_configs),
            "agents": copy.deepcopy(agents),
            "tasks": tasks,
            "task_configs": copy.deepcopy(task_configs)
        })
    return job_state
