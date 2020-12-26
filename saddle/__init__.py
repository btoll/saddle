import argparse
import sys


import saddle.compiler
import saddle.recipe
import saddle.util


# Note!!!
# We're not importing `mulecli` because it's not yet ready to work as a library.
# Instead, we're having to call the `mule` binary and do some futzing with the
# results in order to work with them.


parser = argparse.ArgumentParser()
parser.add_argument("-r", "--recipe", help="Compile from recipe", type=argparse.FileType("r"))
parser.add_argument("-s", "--stdout", help="Write to stdout", action="store_true")
args = parser.parse_args()


def main():
    if args.recipe:
        if args.stdout:
            print(saddle.compiler.compile(args.recipe))
        else:
            saddle.util.write_recipe(compile(args.recipe, to_yaml=False))
    else:
        saddle.recipe.main()


if __name__ == "__main__":
    sys.exit(main())
