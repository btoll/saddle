import argparse
import sys


import saddle.compiler
import saddle.recipe
import saddle.util


parser = argparse.ArgumentParser()
parser.add_argument("-r", "--recipe", help="Compile from recipe", type=argparse.FileType("r"))
parser.add_argument("-s", "--stdout", help="Write to stdout", action="store_true")
args = parser.parse_args()


def main():
    if args.recipe:
        if args.stdout:
            print(saddle.compiler.compile(args.recipe))
        else:
            saddle.util.write_recipe(saddle.compiler.compile(args.recipe, to_yaml=False))
    else:
        saddle.recipe.write()


if __name__ == "__main__":
    sys.exit(main())
