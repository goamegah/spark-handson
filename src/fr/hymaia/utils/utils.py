import os
import shutil

def move_output(src, dest, ext='.csv'):
    for f in os.listdir(src):
        if f.lower().endswith(ext):
            src_path = os.path.join(src, f)
            dest_path = os.path.join(dest, "aggregate.csv")
            shutil.copy(src_path, dest_path)


def remove_dirs(path):
    shutil.rmtree(path)