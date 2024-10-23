import os
import shutil

def move_csv_files(src, dst):
    for file in os.listdir(src):
        if file.endswith(".csv"):
            shutil.move(os.path.join(src, file), dst)

def remove_files(path):
    for file in os.listdir(path):
        os.remove(os.path.join(path, file))
    os.rmdir(path)

def rename_file(src, dst):
    os.rename(src, dst)


def make_dirs(path):
    os.makedirs(path, exist_ok=True)