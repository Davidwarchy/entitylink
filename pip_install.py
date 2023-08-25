import os
import sys
import subprocess
import ctypes
import sysconfig
import os
import urllib.request
import subprocess
import sys

import os
import sys

def is_pip_installed():
    python_path = sys.executable
    # scripts_path = os.path.join(os.path.dirname(python_path), 'Scripts')
    pip_exe_path = os.path.join(sys.prefix, 'Scripts', 'pip.exe')
    # pip_exe_path = os.path.join(scripts_path, 'pip.exe')
    
    return os.path.exists(pip_exe_path)

def pip_install():
    # Set the version of pip you want to install
    pip_version = '21.1.3'

    # Set the download URL for the get-pip.py script
    get_pip_url = f'https://bootstrap.pypa.io/get-pip.py'

    # Set the path where pip will be installed
    install_path = os.path.join(sys.prefix, 'Scripts', 'pip.exe')

    # Download the get-pip.py script
    print('Downloading get-pip.py...')
    urllib.request.urlretrieve(get_pip_url, 'get-pip.py')

    # Install pip
    print('Installing pip...')
    subprocess.check_call([sys.executable, 'get-pip.py', f'pip=={pip_version}'])

    # Clean up the get-pip.py script
    print('Cleaning up...')
    os.remove('get-pip.py')

    print('pip installation completed.')

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


def add_pip_to_path(python_path):
    scripts_path = os.path.join(python_path, "Scripts")
    if not os.path.exists(scripts_path):
        print(f"Scripts path '{scripts_path}' does not exist.")
        return

    pip_path = os.path.join(scripts_path, "pip.exe")
    if not os.path.exists(pip_path):
        print(f"pip is not installed in the specified path.")
        return

    if "Scripts" not in sysconfig.get_path("scripts"):
        sys.path.append(scripts_path)
        os.environ["PATH"] = scripts_path + os.pathsep + os.environ.get("PATH", "")

    print("pip is installed and added to the system path.")

def main():
    python_path = os.path.expanduser(r"~\AppData\Local\Programs\Python\Python311")
    pip_path = pip_exe_path = os.path.join(sys.prefix, "Scripts")
    # Example usage:
    pip_installed = is_pip_installed()
    if not pip_installed:
        pip_install()
        add_pip_to_path(python_path)
    else:
        print(f'pip is already installed.')
        return

    add_pip_to_path(python_path)

if __name__ == "__main__":
    main()