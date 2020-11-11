import datetime
import glob
import json
import os
import re
import subprocess
import sys
import yaml


#def validate_input(f):
#    def wrap():
#        pass
#    pass


def charset(charset):
    def decoder(f):
        def wrap(*args):
            out = f(*args)
            return out.decode(charset)
        return wrap
    return decoder


def choose_input(items, name):
    print()
    for item in list_items(items):
        print(item)
    return int(input(f"\nChoose {name}: ")) - 1


def create_outfile(state, filename, fn):
    out = fn(state)
    write_to_file(out, filename)
    print(f"\nFile written to {'/'.join((os.getcwd(), filename))}\n")
    print(out)


def filter_magic_number(filename):
    with open(filename, "r") as fp:
        return fp.read(6) == "#!mule"


def filter_warnings(item):
    # Ignore blank lines and `mule` warnings from undefined environment variables.
    return not (not len(item) or item.find("Could not") > -1)


@charset("utf8")
def get_cmd_results(cmd):
    return run_cmd(cmd)


def get_json(o):
    return json.dumps(o, sort_keys=True, indent=4)


def get_mule_yamls():
    return list(filter(filter_magic_number, glob.glob("*.yaml")))


def get_yaml(o):
    return yaml.dump(o)


def list_items(items):
    return ["{}) {}".format(index + 1, item) for index, item in enumerate(items)]


def run_cmd(cmd):
    return subprocess.check_output(cmd)


def write_to_file(out, filename):
    with open(filename, "w") as fp:
        fp.write(out)


extensions = {
    ".json": get_json,
    ".yaml": get_yaml
}


def main():
    yamls = get_mule_yamls()
    if len(yamls):
        try:
            my_file = choose_input(yamls, "file")
            if -1 < my_file < len(yamls):
                mule_yaml = yamls[my_file]
                decoded = get_cmd_results(["mule", "-f", mule_yaml, "--list-jobs"])
                jobs = list(filter(filter_warnings, decoded.split("\n")))
                my_job = choose_input(jobs, "job")
                if -1 < my_job < len(jobs):
                    mule_job = jobs[my_job]
                    try:
                        decoded = get_cmd_results(["mule", "-f", mule_yaml, "--list-env", mule_job, "--verbose"])
                        if decoded.find("None") > -1:
                            print(f"\nmule -f {mule_yaml} {mule_job}")
                        else:
                            print(f"\nEnv found in {mule_job}:\n")
                            # Strip off the first line to just get the key=value pairs.
                            items = re.sub("agent.*\n", "", decoded)
                            env_state = []
                            # This is wonky, but we need to trim the newlines from either side of the string before we then
                            # split by the internal newlines :)
                            for item in items.strip("\n").split("\n"):
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
                            my_operation = choose_input(["Run", "Save as recipe"], "operation")
                            if my_operation == 0:
                                env_state.append(f"mule -f {mule_yaml} {mule_job}")
                                print()
                                print(" ".join(env_state))
                            else:
                                try:
                                    filename = input(f"\nName of outfile: ")
                                    [basename, ext] = os.path.splitext(filename)
                                    state = {
                                        "created": str(datetime.datetime.now()),
                                        "version": get_cmd_results(["mule", "-v"]).strip(),
                                        "file": mule_yaml,
                                        "job": mule_job,
                                        "env": env_state
                                    }
                                    if filename.endswith(".*"):
                                        for (ext, fn) in extensions.items():
                                            create_outfile(state, "".join((basename, ext)), fn)
                                    else:
                                        # Default to yaml if there is an unrecognized file extension
                                        # or if there is no file extension.
                                        create_outfile(state, filename, extensions.get(ext, get_yaml))
                                except Exception as err:
                                    print(err)
                    except Exception as err:
                        print(err)
                        print(f"\nmule -f {mule_yaml} {mule_job}")
                else:
                    raise ValueError("Out of range")
            else:
                raise ValueError("Out of range")
        except Exception as err:
            print(err)
    else:
        print(f"No files found in {os.getcwd()} with the `.yaml` extension.")


if __name__ == "__main__":
    sys.exit(main())


